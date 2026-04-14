#include <Arduino.h>
#include <WiFi.h>
#include <esp_wifi.h>
#include <esp_now.h>
#include <ModbusMaster.h>
#include <math.h>

// =========================
// ESPNOW CONFIG
// =========================
static const uint8_t ESPNOW_FIXED_CHANNEL = 6;   // samakan dengan channel ESP32A
uint8_t ESP32A_MAC[6] = {0xA8, 0x42, 0xE3, 0x4C, 0x78, 0xC8}; // ganti sesuai MAC ESP32A

// =========================
// UART CONFIG
// =========================
static const int PZEM_RX = 25;   // ESP32 RX2 <- TX PZEM
static const int PZEM_TX = 26;   // ESP32 TX2 -> RX PZEM
static const uint32_t PZEM_BAUD = 9600;

static const int VIB_RX = 16;    // ESP32 RX1 <- TX sensor vibrasi
static const int VIB_TX = 17;    // ESP32 TX1 -> RX sensor vibrasi
static const uint32_t VIB_BAUD = 4800;

// =========================
// ADDRESS CANDIDATES
// =========================
static const uint8_t PZEM_ADDR_CANDIDATES[] = {0xF8, 1, 2, 3, 4, 5};
static const uint8_t NUM_PZEM_ADDR = sizeof(PZEM_ADDR_CANDIDATES) / sizeof(PZEM_ADDR_CANDIDATES[0]);

static const uint8_t VIB_ADDR_CANDIDATES[] = {1, 2, 3, 4};
static const uint8_t NUM_VIB_ADDR = sizeof(VIB_ADDR_CANDIDATES) / sizeof(VIB_ADDR_CANDIDATES[0]);

static const uint16_t VIB_REG_CANDIDATES[] = {0x0001, 0x0000};
static const uint8_t NUM_VIB_REG = sizeof(VIB_REG_CANDIDATES) / sizeof(VIB_REG_CANDIDATES[0]);

// =========================
// TIMING
// =========================
static const uint32_t SAMPLE_INTERVAL_MS      = 1000;
static const uint32_t BOOT_WARMUP_MS          = 3000;
static const uint32_t REINIT_GAP_MS           = 5000;
static const uint32_t ESPNOW_CB_WAIT_MS       = 300;

static const uint32_t PZEM_HOLD_LAST_MS       = 5000;
static const uint32_t PZEM_SOFT_RECOVER_GAP   = 800;

static const uint32_t VIB_HOLD_LAST_MS        = 5000;
static const uint32_t VIB_SOFT_RECOVER_GAP    = 800;

// =========================
// FILTER / LIMIT
// =========================
static const float PHASE_OFF_VOLT_THRESHOLD = 20.0f;
static const float CURRENT_TINY_THRESHOLD   = 0.02f;
static const float POWER_TINY_THRESHOLD_W   = 2.0f;
static const float APPARENT_POWER_MIN_VA    = 5.0f;
static const float POWER_OVERSHOOT_RATIO    = 1.10f;

static const float VIB_MAX_REASONABLE_MM_S  = 100.0f;

// =========================
// FAIL LEVEL
// =========================
static const uint8_t PZEM_SOFT_FAIL_LEVEL   = 2;
static const uint8_t PZEM_HARD_FAIL_LEVEL   = 6;
static const uint8_t PZEM_CLEAR_FAIL_LEVEL  = 12;

static const uint8_t VIB_SOFT_FAIL_LEVEL    = 2;
static const uint8_t VIB_HARD_FAIL_LEVEL    = 6;
static const uint8_t VIB_CLEAR_FAIL_LEVEL   = 12;

// =========================
// STRUCTS (SAMAKAN DENGAN ESP32A)
// =========================
struct __attribute__((packed)) PhaseMetrics {
  float v, i, p, q, s, f, pf;
  bool ok;
};

struct __attribute__((packed)) BNodePayload {
  uint32_t seq;
  uint32_t ms;
  PhaseMetrics pzemT;
  float vib_speed_mm_s;
  bool vib_ok;
  uint16_t crc;
};

// =========================
// CRC
// =========================
static uint16_t simpleCRC16(const uint8_t* data, size_t len) {
  uint16_t crc = 0xFFFF;
  for (size_t i = 0; i < len; i++) {
    crc ^= data[i];
    for (int b = 0; b < 8; b++) {
      if (crc & 1) crc = (crc >> 1) ^ 0xA001;
      else crc >>= 1;
    }
  }
  return crc;
}

// =========================
// OBJECTS
// =========================
HardwareSerial PzemSerial(2);
HardwareSerial VibSerial(1);

ModbusMaster nodePzem;
ModbusMaster nodeVib;

// =========================
// GLOBAL
// =========================
BNodePayload payload{};
uint32_t seq = 0;
uint32_t lastSampleMs = 0;

uint32_t lastPzemReinitMs = 0;
uint32_t lastPzemSoftRecoverMs = 0;

uint32_t lastVibReinitMs = 0;
uint32_t lastVibSoftRecoverMs = 0;

uint8_t pzemFailCount = 0;
uint8_t vibFailCount  = 0;

uint8_t activePzemAddr = 0xF8;
bool pzemDetected = false;

uint8_t activeVibAddr = 1;
uint16_t activeVibReg = 0x0001;
bool activeUseHolding = true;
bool vibDetected = false;

// cache PZEM
PhaseMetrics lastGoodPzem{};
bool hasLastGoodPzem = false;
bool pzemHoldingLastData = false;
uint32_t lastGoodPzemMs = 0;

// cache VIB
float lastGoodVibSpeed = 0.0f;
bool hasLastGoodVib = false;
bool vibHoldingLastData = false;
uint32_t lastGoodVibMs = 0;

volatile bool lastSendDone = false;
volatile esp_now_send_status_t lastSendStatus = ESP_NOW_SEND_FAIL;

// =========================
// HELPER
// =========================
void printMacArray(const uint8_t mac[6]) {
  Serial.printf("%02X:%02X:%02X:%02X:%02X:%02X",
                mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
}

const char* modbusResultText(uint8_t code) {
  switch (code) {
    case ModbusMaster::ku8MBSuccess: return "OK";
    case ModbusMaster::ku8MBIllegalFunction: return "IllegalFunction";
    case ModbusMaster::ku8MBIllegalDataAddress: return "IllegalDataAddress";
    case ModbusMaster::ku8MBIllegalDataValue: return "IllegalDataValue";
    case ModbusMaster::ku8MBSlaveDeviceFailure: return "SlaveDeviceFailure";
    case ModbusMaster::ku8MBInvalidSlaveID: return "InvalidSlaveID";
    case ModbusMaster::ku8MBInvalidFunction: return "InvalidFunction";
    case ModbusMaster::ku8MBResponseTimedOut: return "ResponseTimedOut";
    case ModbusMaster::ku8MBInvalidCRC: return "InvalidCRC";
    default: return "Unknown";
  }
}

void flushSerialRx(HardwareSerial &ser) {
  while (ser.available()) ser.read();
}

void onDataSent(const wifi_tx_info_t *tx_info, esp_now_send_status_t status) {
  lastSendDone = true;
  lastSendStatus = status;
}

bool waitEspNowCallback(uint32_t timeoutMs) {
  uint32_t t0 = millis();
  while (!lastSendDone && (millis() - t0 < timeoutMs)) {
    delay(1);
  }
  return lastSendDone;
}

// =========================
// SANITIZE PZEM
// =========================
void sanitizePhaseMetrics(PhaseMetrics &m) {
  if (!m.ok) {
    m = {};
    return;
  }

  if (!isfinite(m.v) || !isfinite(m.i) || !isfinite(m.p) || !isfinite(m.f)) {
    m = {};
    return;
  }

  if (m.v < PHASE_OFF_VOLT_THRESHOLD) {
    m = {};
    return;
  }

  if (m.i < 0.0f) m.i = fabsf(m.i);
  if (m.p < 0.0f) m.p = fabsf(m.p);

  m.s = m.v * m.i;

  if (m.s < APPARENT_POWER_MIN_VA || m.i < CURRENT_TINY_THRESHOLD) {
    m.i = 0.0f;
    m.p = 0.0f;
    m.s = 0.0f;
    m.q = 0.0f;
    m.pf = 0.0f;
    m.f = (m.f >= 45.0f && m.f <= 65.0f) ? m.f : 0.0f;
    m.ok = true;
    return;
  }

  if (m.p > (m.s * POWER_OVERSHOOT_RATIO)) m.p = m.s;
  else if (m.p > m.s) m.p = m.s;

  m.pf = (m.s > 0.0f) ? (m.p / m.s) : 0.0f;
  if (!isfinite(m.pf)) m.pf = 0.0f;
  if (m.pf < 0.0f) m.pf = 0.0f;
  if (m.pf > 1.0f) m.pf = 1.0f;

  if (fabsf(m.p) < POWER_TINY_THRESHOLD_W) {
    m.p = 0.0f;
    m.s = 0.0f;
    m.q = 0.0f;
    m.pf = 0.0f;
    return;
  }

  if (m.f < 45.0f || m.f > 65.0f) m.f = 0.0f;

  float q2 = (m.s * m.s) - (m.p * m.p);
  if (q2 < 0.0f) q2 = 0.0f;
  m.q = sqrtf(q2);
}

// =========================
// ESPNOW
// =========================
void setupEspNowRadio() {
  WiFi.mode(WIFI_STA);
  WiFi.disconnect(true, true);
  WiFi.setSleep(false);
  delay(200);

  esp_wifi_set_promiscuous(true);
  esp_wifi_set_channel(ESPNOW_FIXED_CHANNEL, WIFI_SECOND_CHAN_NONE);
  esp_wifi_set_promiscuous(false);

  Serial.print("ESP32B MAC       : ");
  Serial.println(WiFi.macAddress());
  Serial.print("TARGET ESP32A MAC: ");
  printMacArray(ESP32A_MAC);
  Serial.println();
  Serial.printf("ESP-NOW channel  : %u\n", ESPNOW_FIXED_CHANNEL);
}

bool initEspNowPeer() {
  if (esp_now_init() != ESP_OK) {
    Serial.println("ESP-NOW init FAILED");
    return false;
  }

  esp_now_register_send_cb(onDataSent);

  esp_now_peer_info_t peerInfo{};
  memcpy(peerInfo.peer_addr, ESP32A_MAC, 6);
  peerInfo.channel = ESPNOW_FIXED_CHANNEL;
  peerInfo.encrypt = false;

  if (esp_now_is_peer_exist(ESP32A_MAC)) {
    esp_now_del_peer(ESP32A_MAC);
    delay(50);
  }

  if (esp_now_add_peer(&peerInfo) != ESP_OK) {
    Serial.println("Add peer FAILED");
    return false;
  }

  Serial.println("ESP-NOW READY");
  return true;
}

// =========================
// SERIAL INIT
// =========================
void initPzemSerial() {
  PzemSerial.end();
  delay(100);
  PzemSerial.begin(PZEM_BAUD, SERIAL_8N1, PZEM_RX, PZEM_TX);
  PzemSerial.setTimeout(200);
  delay(500);
  flushSerialRx(PzemSerial);

  Serial.printf("[INIT] PZEM | RX=%d TX=%d baud=%lu\n",
                PZEM_RX, PZEM_TX, (unsigned long)PZEM_BAUD);
}

void initVibration() {
  VibSerial.end();
  delay(100);
  VibSerial.begin(VIB_BAUD, SERIAL_8N1, VIB_RX, VIB_TX);
  VibSerial.setTimeout(200);
  delay(500);
  flushSerialRx(VibSerial);

  Serial.printf("[INIT] VIB  | RX=%d TX=%d baud=%lu\n",
                VIB_RX, VIB_TX, (unsigned long)VIB_BAUD);
}

// =========================
// PZEM READ
// =========================
bool readPZEM_Generic(ModbusMaster &node, HardwareSerial &ser, PhaseMetrics &m, uint8_t &mbCode) {
  m = {};
  m.ok = false;

  flushSerialRx(ser);
  mbCode = node.readInputRegisters(0x0000, 10);
  if (mbCode != node.ku8MBSuccess) return false;

  uint16_t r0 = node.getResponseBuffer(0);
  uint16_t r1 = node.getResponseBuffer(1);
  uint16_t r2 = node.getResponseBuffer(2);
  uint16_t r3 = node.getResponseBuffer(3);
  uint16_t r4 = node.getResponseBuffer(4);
  uint16_t r7 = node.getResponseBuffer(7);
  uint16_t r8 = node.getResponseBuffer(8);

  m.v = r0 / 10.0f;

  uint32_t i_raw = ((uint32_t)r2 << 16) | r1;
  m.i = i_raw / 1000.0f;

  uint32_t p_raw = ((uint32_t)r4 << 16) | r3;
  m.p = p_raw / 10.0f;

  m.f  = r7 / 10.0f;
  m.pf = r8 / 100.0f;

  m.s = m.v * m.i;
  float s2 = m.s * m.s;
  float p2 = m.p * m.p;
  float q2 = s2 - p2;
  if (q2 < 0.0f) q2 = 0.0f;
  m.q = sqrtf(q2);

  m.ok = true;
  return true;
}

bool readPZEM_WithRetry(ModbusMaster &node, HardwareSerial &ser, PhaseMetrics &out, uint8_t &lastErr) {
  for (int k = 0; k < 3; k++) {
    if (readPZEM_Generic(node, ser, out, lastErr)) return true;
    delay(80);
    flushSerialRx(ser);
  }
  return false;
}

bool detectPZEMAddress(ModbusMaster &node, HardwareSerial &ser, uint8_t &foundAddr) {
  PhaseMetrics tmp;
  uint8_t err = 0;

  Serial.println("=== SCAN PZEM ADDRESS ===");

  for (uint8_t i = 0; i < NUM_PZEM_ADDR; i++) {
    uint8_t cand = PZEM_ADDR_CANDIDATES[i];
    node.begin(cand, ser);
    delay(50);

    if (readPZEM_WithRetry(node, ser, tmp, err)) {
      Serial.printf("[PZEM] RESPOND | addr=0x%02X | V=%.1f | I=%.3f | P=%.1f\n",
                    cand, tmp.v, tmp.i, tmp.p);

      if (tmp.v > PHASE_OFF_VOLT_THRESHOLD) {
        foundAddr = cand;
        return true;
      }
    } else {
      Serial.printf("[PZEM] FAIL    | addr=0x%02X | code=%u (%s)\n",
                    cand, err, modbusResultText(err));
    }
  }
  return false;
}

bool acceptGoodPzemRead(PhaseMetrics &m, uint32_t now) {
  sanitizePhaseMetrics(m);

  if (!m.ok) return false;

  lastGoodPzem = m;
  hasLastGoodPzem = true;
  lastGoodPzemMs = now;
  pzemFailCount = 0;
  pzemHoldingLastData = false;
  return true;
}

void softRecoverPzem(uint32_t now) {
  if (now - lastPzemSoftRecoverMs < PZEM_SOFT_RECOVER_GAP) return;

  Serial.println("[PZEM] soft recover: rebind active address...");
  flushSerialRx(PzemSerial);
  nodePzem.begin(activePzemAddr, PzemSerial);
  delay(120);
  flushSerialRx(PzemSerial);

  lastPzemSoftRecoverMs = now;
}

void hardRecoverPzem(uint32_t now) {
  if (now - lastPzemReinitMs < REINIT_GAP_MS) return;

  Serial.println("[PZEM] hard recover: serial reinit + rescan...");
  initPzemSerial();

  bool found = detectPZEMAddress(nodePzem, PzemSerial, activePzemAddr);
  if (found) {
    pzemDetected = true;
    nodePzem.begin(activePzemAddr, PzemSerial);
    delay(120);
    flushSerialRx(PzemSerial);
    Serial.printf("[PZEM] active addr -> 0x%02X\n", activePzemAddr);
  } else {
    pzemDetected = false;
    Serial.println("[PZEM] rescan gagal");
  }

  lastPzemReinitMs = now;
}

bool acquirePzemStable(PhaseMetrics &out, uint8_t &errCode, uint32_t now) {
  out = {};
  pzemHoldingLastData = false;
  errCode = 0;

  if (pzemDetected && readPZEM_WithRetry(nodePzem, PzemSerial, out, errCode)) {
    if (acceptGoodPzemRead(out, now)) return true;
  }

  pzemFailCount++;

  if (pzemDetected && pzemFailCount >= PZEM_SOFT_FAIL_LEVEL) {
    softRecoverPzem(now);

    PhaseMetrics retryData{};
    uint8_t retryErr = 0;
    if (readPZEM_WithRetry(nodePzem, PzemSerial, retryData, retryErr)) {
      if (acceptGoodPzemRead(retryData, now)) {
        out = retryData;
        errCode = retryErr;
        return true;
      }
    } else {
      errCode = retryErr;
    }
  }

  if (pzemFailCount >= PZEM_HARD_FAIL_LEVEL) {
    hardRecoverPzem(now);

    if (pzemDetected) {
      PhaseMetrics retryData{};
      uint8_t retryErr = 0;
      if (readPZEM_WithRetry(nodePzem, PzemSerial, retryData, retryErr)) {
        if (acceptGoodPzemRead(retryData, now)) {
          out = retryData;
          errCode = retryErr;
          return true;
        }
      } else {
        errCode = retryErr;
      }
    }
  }

  if (hasLastGoodPzem && (now - lastGoodPzemMs <= PZEM_HOLD_LAST_MS)) {
    out = lastGoodPzem;
    out.ok = true;
    pzemHoldingLastData = true;
    return true;
  }

  if (pzemFailCount >= PZEM_CLEAR_FAIL_LEVEL) {
    out = {};
    return false;
  }

  if (hasLastGoodPzem) {
    out = lastGoodPzem;
    out.ok = true;
    pzemHoldingLastData = true;
    return true;
  }

  out = {};
  return false;
}

// =========================
// VIBRATION READ
// =========================
bool isReasonableVibration(float speed_mm_s) {
  if (!isfinite(speed_mm_s)) return false;
  if (speed_mm_s < 0.0f) return false;
  if (speed_mm_s > VIB_MAX_REASONABLE_MM_S) return false;
  return true;
}

bool readVibRegister(uint8_t addr, uint16_t reg, bool useHolding,
                     float &speed_mm_s, uint8_t &errCode) {
  speed_mm_s = 0.0f;

  nodeVib.begin(addr, VibSerial);
  delay(20);
  flushSerialRx(VibSerial);

  if (useHolding) errCode = nodeVib.readHoldingRegisters(reg, 1);
  else            errCode = nodeVib.readInputRegisters(reg, 1);

  if (errCode != nodeVib.ku8MBSuccess) return false;

  uint16_t raw = nodeVib.getResponseBuffer(0);
  speed_mm_s = raw / 10.0f;

  return isReasonableVibration(speed_mm_s);
}

bool readVibration_WithRetry(float &speed_mm_s, uint8_t &errCode) {
  for (int k = 0; k < 3; k++) {
    if (readVibRegister(activeVibAddr, activeVibReg, activeUseHolding, speed_mm_s, errCode)) {
      return true;
    }
    delay(80);
    flushSerialRx(VibSerial);
  }
  return false;
}

bool detectVibrationSensor() {
  float speed = 0.0f;
  uint8_t err = 0;

  Serial.println("=== SCAN VIBRATION SENSOR ===");

  for (uint8_t i = 0; i < NUM_VIB_ADDR; i++) {
    uint8_t candAddr = VIB_ADDR_CANDIDATES[i];

    for (uint8_t r = 0; r < NUM_VIB_REG; r++) {
      uint16_t candReg = VIB_REG_CANDIDATES[r];

      bool okHolding = readVibRegister(candAddr, candReg, true, speed, err);
      if (okHolding) {
        activeVibAddr = candAddr;
        activeVibReg = candReg;
        activeUseHolding = true;
        vibDetected = true;

        Serial.printf("[VIB] FOUND | addr=%u | reg=0x%04X | mode=Holding | speed=%.1f mm/s\n",
                      activeVibAddr, activeVibReg, speed);
        return true;
      } else {
        Serial.printf("[SCAN] addr=%u reg=0x%04X Holding FAIL | code=%u (%s)\n",
                      candAddr, candReg, err, modbusResultText(err));
      }

      bool okInput = readVibRegister(candAddr, candReg, false, speed, err);
      if (okInput) {
        activeVibAddr = candAddr;
        activeVibReg = candReg;
        activeUseHolding = false;
        vibDetected = true;

        Serial.printf("[VIB] FOUND | addr=%u | reg=0x%04X | mode=Input | speed=%.1f mm/s\n",
                      activeVibAddr, activeVibReg, speed);
        return true;
      } else {
        Serial.printf("[SCAN] addr=%u reg=0x%04X Input   FAIL | code=%u (%s)\n",
                      candAddr, candReg, err, modbusResultText(err));
      }
    }
  }

  vibDetected = false;
  Serial.println("[VIB] NOT DETECTED");
  return false;
}

bool acceptGoodVibRead(float speed_mm_s, uint32_t now) {
  if (!isReasonableVibration(speed_mm_s)) return false;

  lastGoodVibSpeed = speed_mm_s;
  hasLastGoodVib = true;
  lastGoodVibMs = now;
  vibFailCount = 0;
  vibHoldingLastData = false;
  return true;
}

void softRecoverVib(uint32_t now) {
  if (now - lastVibSoftRecoverMs < VIB_SOFT_RECOVER_GAP) return;

  Serial.println("[VIB] soft recover: rebind active address...");
  flushSerialRx(VibSerial);
  nodeVib.begin(activeVibAddr, VibSerial);
  delay(120);
  flushSerialRx(VibSerial);

  lastVibSoftRecoverMs = now;
}

void hardRecoverVib(uint32_t now) {
  if (now - lastVibReinitMs < REINIT_GAP_MS) return;

  Serial.println("[VIB] hard recover: serial reinit + rescan...");
  initVibration();

  bool found = detectVibrationSensor();
  if (found) {
    nodeVib.begin(activeVibAddr, VibSerial);
    delay(120);
    flushSerialRx(VibSerial);
    Serial.printf("[VIB] active addr=%u reg=0x%04X mode=%s\n",
                  activeVibAddr,
                  activeVibReg,
                  activeUseHolding ? "Holding" : "Input");
  } else {
    vibDetected = false;
    Serial.println("[VIB] rescan gagal");
  }

  lastVibReinitMs = now;
}

bool acquireVibrationStable(float &speedOut, uint8_t &errCode, uint32_t now) {
  speedOut = 0.0f;
  vibHoldingLastData = false;
  errCode = 0;

  if (vibDetected) {
    float liveSpeed = 0.0f;
    if (readVibration_WithRetry(liveSpeed, errCode)) {
      if (acceptGoodVibRead(liveSpeed, now)) {
        speedOut = liveSpeed;
        return true;
      }
    }
  }

  vibFailCount++;

  if (vibDetected && vibFailCount >= VIB_SOFT_FAIL_LEVEL) {
    softRecoverVib(now);

    float retrySpeed = 0.0f;
    uint8_t retryErr = 0;
    if (readVibration_WithRetry(retrySpeed, retryErr)) {
      if (acceptGoodVibRead(retrySpeed, now)) {
        speedOut = retrySpeed;
        errCode = retryErr;
        return true;
      }
    } else {
      errCode = retryErr;
    }
  }

  if (vibFailCount >= VIB_HARD_FAIL_LEVEL) {
    hardRecoverVib(now);

    if (vibDetected) {
      float retrySpeed = 0.0f;
      uint8_t retryErr = 0;
      if (readVibration_WithRetry(retrySpeed, retryErr)) {
        if (acceptGoodVibRead(retrySpeed, now)) {
          speedOut = retrySpeed;
          errCode = retryErr;
          return true;
        }
      } else {
        errCode = retryErr;
      }
    }
  }

  if (hasLastGoodVib && (now - lastGoodVibMs <= VIB_HOLD_LAST_MS)) {
    speedOut = lastGoodVibSpeed;
    vibHoldingLastData = true;
    return true;
  }

  if (vibFailCount >= VIB_CLEAR_FAIL_LEVEL) {
    speedOut = 0.0f;
    return false;
  }

  if (hasLastGoodVib) {
    speedOut = lastGoodVibSpeed;
    vibHoldingLastData = true;
    return true;
  }

  speedOut = 0.0f;
  return false;
}

// =========================
// SETUP
// =========================
void setup() {
  Serial.begin(115200);
  delay(800);
  Serial.println("\n=== ESP32B PZEM + VIB SENDER START ===");

  setupEspNowRadio();

  if (!initEspNowPeer()) {
    Serial.println("[FATAL] ESP-NOW gagal start");
  }

  Serial.printf("[BOOT] warmup %lu ms...\n", (unsigned long)BOOT_WARMUP_MS);
  delay(BOOT_WARMUP_MS);

  initPzemSerial();
  pzemDetected = detectPZEMAddress(nodePzem, PzemSerial, activePzemAddr);
  if (pzemDetected) {
    nodePzem.begin(activePzemAddr, PzemSerial);
    Serial.printf("[PZEM] active addr -> 0x%02X\n", activePzemAddr);
  } else {
    Serial.println("[PZEM] NOT DETECTED");
  }

  hasLastGoodPzem = false;
  pzemHoldingLastData = false;
  lastGoodPzem = {};
  lastGoodPzemMs = 0;

  initVibration();
  detectVibrationSensor();

  hasLastGoodVib = false;
  vibHoldingLastData = false;
  lastGoodVibSpeed = 0.0f;
  lastGoodVibMs = 0;

  Serial.println("ESP32B READY");
  lastSampleMs = millis();
}

// =========================
// LOOP
// =========================
void loop() {
  uint32_t now = millis();

  if (now - lastSampleMs >= SAMPLE_INTERVAL_MS) {
    lastSampleMs = now;

    payload = {};
    payload.seq = ++seq;
    payload.ms  = now / 1000;

    // ---------- PZEM ----------
    uint8_t errPzem = 0;
    bool okPzem = acquirePzemStable(payload.pzemT, errPzem, now);

    // ---------- VIB ----------
    uint8_t errVib = 0;
    float vibSpeed = 0.0f;
    bool okVib = acquireVibrationStable(vibSpeed, errVib, now);

    payload.vib_ok = okVib;
    payload.vib_speed_mm_s = okVib ? vibSpeed : 0.0f;

    // ---------- CRC ----------
    payload.crc = 0;
    payload.crc = simpleCRC16((uint8_t*)&payload, sizeof(BNodePayload));

    // ---------- SEND ----------
    lastSendDone = false;
    lastSendStatus = ESP_NOW_SEND_FAIL;

    esp_err_t res = esp_now_send(ESP32A_MAC, (uint8_t*)&payload, sizeof(BNodePayload));

    Serial.println("--------------------------------------------------");

    if (okPzem && payload.pzemT.ok) {
      Serial.printf("[PZEM] %s | addr=0x%02X | V=%.1f V | I=%.3f A | P=%.1f W | PF=%.2f | F=%.1f Hz | fail=%u\n",
                    pzemHoldingLastData ? "HOLD" : "LIVE",
                    activePzemAddr,
                    payload.pzemT.v,
                    payload.pzemT.i,
                    payload.pzemT.p,
                    payload.pzemT.pf,
                    payload.pzemT.f,
                    pzemFailCount);
    } else {
      Serial.printf("[PZEM] FAIL | addr=0x%02X | code=%u (%s) | fail=%u\n",
                    activePzemAddr, errPzem, modbusResultText(errPzem), pzemFailCount);
    }

    if (okVib) {
      Serial.printf("[VIB ] %s | addr=%u | reg=0x%04X | mode=%s | speed=%.1f mm/s | fail=%u\n",
                    vibHoldingLastData ? "HOLD" : "LIVE",
                    activeVibAddr,
                    activeVibReg,
                    activeUseHolding ? "Holding" : "Input",
                    payload.vib_speed_mm_s,
                    vibFailCount);
    } else {
      Serial.printf("[VIB ] FAIL | code=%u (%s) | fail=%u\n",
                    errVib, modbusResultText(errVib), vibFailCount);
    }

    if (res == ESP_OK) {
      Serial.printf("[ESPNOW] QUEUED | seq=%lu | t=%lu s | target=",
                    payload.seq, payload.ms);
      printMacArray(ESP32A_MAC);
      Serial.println();

      bool gotCb = waitEspNowCallback(ESPNOW_CB_WAIT_MS);
      if (gotCb) {
        Serial.printf("[ESPNOW] STATUS = %s\n",
                      (lastSendStatus == ESP_NOW_SEND_SUCCESS) ? "SUCCESS" : "FAIL");
      } else {
        Serial.println("[ESPNOW] STATUS = no callback timeout");
      }
    } else {
      Serial.printf("[ESPNOW] SEND API FAIL | err=%d\n", res);
    }
  }
}
