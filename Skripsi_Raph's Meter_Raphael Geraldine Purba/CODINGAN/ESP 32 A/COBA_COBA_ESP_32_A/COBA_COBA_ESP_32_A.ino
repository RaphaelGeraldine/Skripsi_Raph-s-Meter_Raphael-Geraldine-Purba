#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <esp_now.h>
#include <TFT_eSPI.h>
#include <Wire.h>
#include <Adafruit_MLX90614.h>
#include <ModbusMaster.h>
#include <math.h>

// =========================
// WIFI + COLAB
// =========================
#define WIFI_SSID     "MULTIMON"
#define WIFI_PASS     "berhasil123"
#define COLAB_ENDPOINT_URL "https://trena-nonenervating-remonstrantly.ngrok-free.dev/ingest"

// =========================
// THINGSBOARD
// =========================
#define THINGSBOARD_TELEMETRY_URL "https://thingsboard.cloud/api/v1/niqyvf3j0pyy8pc8o1jo/telemetry"
static const bool ENABLE_THINGSBOARD = true;
static const uint32_t TB_POST_INTERVAL_MS = 1000;
uint32_t lastTbPostMs = 0;

// =========================
// UART ESP32A
// =========================
static const int PZEM1_RX = 16;
static const int PZEM1_TX = 17;
static const int PZEM2_RX = 25;
static const int PZEM2_TX = 26;
static const uint32_t MODBUS_BAUD = 9600;

// kandidat address PZEM
static const uint8_t PZEM_ADDR_CANDIDATES[] = {0xF8, 1, 2, 4};
static const uint8_t NUM_PZEM_ADDR = sizeof(PZEM_ADDR_CANDIDATES) / sizeof(PZEM_ADDR_CANDIDATES[0]);

uint8_t addrPzemA1 = 0xF8;
uint8_t addrPzemA2 = 0xF8;

// =========================
// TIMING
// =========================
static const uint32_t SAMPLE_INTERVAL_MS = 1000;
static const uint32_t PAGE_INTERVAL_MS   = 2500;
static const uint32_t SPLASH_MS          = 2500;
static const uint32_t WIFI_TIMEOUT_MS    = 15000;
static const uint8_t  TOTAL_SAMPLES      = 60;
static const uint32_t B_LINK_TIMEOUT_MS  = 1500;

// =========================
// OFF / ZERO THRESHOLD
// =========================
static const float PHASE_OFF_VOLT_THRESHOLD = 20.0f;
static const float CURRENT_TINY_THRESHOLD   = 0.02f;
static const float POWER_TINY_THRESHOLD_W   = 2.0f;

// tambahan stabilisasi P dan PF
static const float APPARENT_POWER_MIN_VA = 5.0f;
static const float POWER_OVERSHOOT_RATIO = 1.10f;

// Voltage mode
enum VoltageMode { FROM_VLN_ESTIMATE, FROM_VLL_DIRECT };
static const VoltageMode VOLTAGE_MODE = FROM_VLN_ESTIMATE;

// =========================
// DATA STRUCTS (HARUS SAMA DENGAN ESP32B)
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
  bool  vib_ok;
  uint16_t crc;
};

struct Sample1s {
  uint32_t ts_ms;

  float VRS, VST, VTR, VRN, VSN, VTN;
  float IR, IS, IT;
  float PRkW, PSkW, PTkW;
  float SRkVA, SSkVA, STkVA;
  float QRkVAr, QSkVAr, QTkVAr;
  float pfR, pfS, pfT;
  float fR, fS, fT;

  float vib_speed_mm_s;
  float Tmotor_C;
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
TFT_eSPI tft = TFT_eSPI();
Adafruit_MLX90614 mlx = Adafruit_MLX90614();

HardwareSerial PzemSerial1(1);
HardwareSerial PzemSerial2(2);
ModbusMaster nodeA1, nodeA2;

// =========================
// GLOBAL RUNTIME
// =========================
PhaseMetrics a1{}, a2{};
PhaseMetrics R, S, T;

BNodePayload bPayload{};
uint32_t bLastSeenMs = 0;
portMUX_TYPE payloadMux = portMUX_INITIALIZER_UNLOCKED;

float VRS = 0, VST = 0, VTR = 0;
float VRN = 0, VSN = 0, VTN = 0;

float motorTempC = 0.0f;
bool temp_ok = false;

Sample1s samples[TOTAL_SAMPLES];
uint8_t sampleIdx = 0;

// =========================
// UI STATE MACHINE
// =========================
enum UiState { ST_SPLASH, ST_WIFI, ST_MONITORING, ST_SENDING };
UiState state = ST_SPLASH;

uint32_t stateStartMs = 0;
uint32_t lastSampleMs = 0;
uint32_t lastPageMs   = 0;
uint8_t monitorPage = 0;

// =========================
// HELPER
// =========================
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

void keepWiFiAliveDuringMonitoring() {
  static uint32_t lastReconnectAttempt = 0;

  if (WiFi.status() == WL_CONNECTED) return;
  if (millis() - lastReconnectAttempt < 10000) return;

  lastReconnectAttempt = millis();
  Serial.println("[WiFi] reconnect attempt...");
  WiFi.disconnect();
  delay(200);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
}

// =========================
// FIXED POWER TRIANGLE + PF
// =========================
void normalizePowerAndPF(PhaseMetrics &m) {
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

  if (m.p > (m.s * POWER_OVERSHOOT_RATIO)) {
    m.p = m.s;
  } else if (m.p > m.s) {
    m.p = m.s;
  }

  float pf_calc = (m.s > 0.0f) ? (m.p / m.s) : 0.0f;
  if (!isfinite(pf_calc)) pf_calc = 0.0f;
  if (pf_calc < 0.0f) pf_calc = 0.0f;
  if (pf_calc > 1.0f) pf_calc = 1.0f;
  m.pf = pf_calc;

  if (fabsf(m.p) < POWER_TINY_THRESHOLD_W) {
    m.p = 0.0f;
    m.s = 0.0f;
    m.q = 0.0f;
    m.pf = 0.0f;
    return;
  }

  if (m.f < 45.0f || m.f > 65.0f) {
    m.f = 0.0f;
  }

  float s2 = m.s * m.s;
  float p2 = m.p * m.p;
  float q2 = s2 - p2;
  if (q2 < 0.0f) q2 = 0.0f;
  m.q = sqrtf(q2);
}

void sanitizePhaseMetrics(PhaseMetrics &m) {
  normalizePowerAndPF(m);
}

bool isBLinkFresh() {
  uint32_t seen = 0;
  portENTER_CRITICAL(&payloadMux);
  seen = bLastSeenMs;
  portEXIT_CRITICAL(&payloadMux);
  return (millis() - seen) < B_LINK_TIMEOUT_MS;
}

BNodePayload getCurrentBPayload() {
  BNodePayload cur{};
  uint32_t seen = 0;

  portENTER_CRITICAL(&payloadMux);
  cur = bPayload;
  seen = bLastSeenMs;
  portEXIT_CRITICAL(&payloadMux);

  if ((millis() - seen) < B_LINK_TIMEOUT_MS) return cur;
  return BNodePayload{};
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
    delay(30);
  }
  return false;
}

bool detectPZEMAddress(ModbusMaster &node, HardwareSerial &ser, uint8_t &foundAddr) {
  PhaseMetrics tmp;
  uint8_t err;

  for (uint8_t i = 0; i < NUM_PZEM_ADDR; i++) {
    uint8_t cand = PZEM_ADDR_CANDIDATES[i];
    node.begin(cand, ser);
    delay(50);

    if (readPZEM_WithRetry(node, ser, tmp, err)) {
      if (tmp.v > PHASE_OFF_VOLT_THRESHOLD) {
        foundAddr = cand;
        return true;
      }
    }
  }
  return false;
}

// =========================
// ESP-NOW RX
// =========================
void onEspNowRecv(const esp_now_recv_info_t *info, const uint8_t *data, int len) {
  if (len != (int)sizeof(BNodePayload)) return;

  BNodePayload incoming;
  memcpy(&incoming, data, sizeof(BNodePayload));

  uint16_t recvCRC = incoming.crc;
  incoming.crc = 0;
  uint16_t calcCRC = simpleCRC16((uint8_t*)&incoming, sizeof(BNodePayload));
  if (calcCRC != recvCRC) return;

  portENTER_CRITICAL(&payloadMux);
  bPayload = incoming;
  bLastSeenMs = millis();
  portEXIT_CRITICAL(&payloadMux);

  Serial.printf("[RX-B] seq=%lu | T_ok=%s | vib_ok=%s | V=%.1f | I=%.3f | vib=%.1f\n",
                incoming.seq,
                incoming.pzemT.ok ? "YES" : "NO",
                incoming.vib_ok ? "YES" : "NO",
                incoming.pzemT.v,
                incoming.pzemT.i,
                incoming.vib_speed_mm_s);
}

// =========================
// BUILD 3 PHASE
// =========================
void buildThreePhase() {
  BNodePayload bUse = getCurrentBPayload();

  R = a1;
  S = a2;
  T = {};

  sanitizePhaseMetrics(R);
  sanitizePhaseMetrics(S);

  if (bUse.pzemT.ok) {
    T = bUse.pzemT;
    sanitizePhaseMetrics(T);
  }

  if (VOLTAGE_MODE == FROM_VLN_ESTIMATE) {
    VRN = R.ok ? R.v : 0.0f;
    VSN = S.ok ? S.v : 0.0f;
    VTN = T.ok ? T.v : 0.0f;

    VRS = (VRN > 0.0f) ? 1.732f * VRN : 0.0f;
    VST = (VSN > 0.0f) ? 1.732f * VSN : 0.0f;
    VTR = (VTN > 0.0f) ? 1.732f * VTN : 0.0f;
  } else {
    VRS = R.ok ? R.v : 0.0f;
    VST = S.ok ? S.v : 0.0f;
    VTR = T.ok ? T.v : 0.0f;
    VRN = VSN = VTN = 0.0f;
  }
}

// =========================
// TFT HELPERS
// =========================
void drawHeader(const char* title) {
  tft.fillScreen(TFT_BLACK);
  tft.setTextColor(TFT_CYAN, TFT_BLACK);
  tft.setTextSize(2);
  tft.setCursor(8, 8);
  tft.print(title);
  tft.drawFastHLine(0, 32, tft.width(), TFT_DARKGREY);
  tft.setTextSize(2);
  tft.setTextColor(TFT_WHITE, TFT_BLACK);
}

void draw3(const char* l1, float a, const char* l2, float b, const char* l3, float c, const char* unit) {
  int y = 55;
  tft.setCursor(10, y);    tft.printf("%s: %8.2f %s", l1, a, unit);
  tft.setCursor(10, y+35); tft.printf("%s: %8.2f %s", l2, b, unit);
  tft.setCursor(10, y+70); tft.printf("%s: %8.2f %s", l3, c, unit);
}

void renderSplash() {
  tft.fillScreen(TFT_BLACK);
  tft.setTextColor(TFT_CYAN, TFT_BLACK);
  tft.setTextSize(2);
  tft.setCursor(10, 40);
  tft.print("RAPHAEL GERALDINE PURBA");

  tft.setTextColor(TFT_WHITE, TFT_BLACK);
  tft.setCursor(10, 80);
  tft.print("Teknik Elektro 2022");

  tft.setTextColor(TFT_YELLOW, TFT_BLACK);
  tft.setCursor(10, 130);
  tft.print("SELAMAT DATANG");

  tft.setCursor(10, 160);
  tft.print("RAPH'S METER");
}

void renderWiFiStatus(const char* msg, bool showIP = false) {
  tft.fillScreen(TFT_BLACK);
  tft.setTextColor(TFT_CYAN, TFT_BLACK);
  tft.setTextSize(2);
  tft.setCursor(10, 40);
  tft.print("WiFi STATUS");

  tft.setTextColor(TFT_WHITE, TFT_BLACK);
  tft.setCursor(10, 90);
  tft.print(msg);

  if (showIP && WiFi.status() == WL_CONNECTED) {
    tft.setCursor(10, 130);
    tft.print("IP:");
    tft.setCursor(10, 160);
    tft.print(WiFi.localIP());
  }

  if (WiFi.status() != WL_CONNECTED) {
    tft.setTextColor(TFT_YELLOW, TFT_BLACK);
    tft.setCursor(10, 190);
    tft.print("Konfigurasi ulang WiFi");
  }
}

void renderMonitoringPage(uint8_t p) {
  BNodePayload bUse = getCurrentBPayload();
  bool linkOk = isBLinkFresh();

  float vibSp = bUse.vib_ok ? bUse.vib_speed_mm_s : 0.0f;

  switch (p) {
    case 0:
      drawHeader("1) VLL (Line-Line)");
      draw3("VRS", VRS, "VST", VST, "VTR", VTR, "V");
      break;
    case 1:
      drawHeader("2) VLN (Line-Neutral)");
      draw3("VRN", VRN, "VSN", VSN, "VTN", VTN, "V");
      break;
    case 2:
      drawHeader("3) Current (A)");
      draw3("IR", R.i, "IS", S.i, "IT", T.i, "A");
      break;
    case 3:
      drawHeader("4) Active Power (P)");
      draw3("PR", R.p / 1000.0f, "PS", S.p / 1000.0f, "PT", T.p / 1000.0f, "kW");
      break;
    case 4:
      drawHeader("5) Apparent Power (S)");
      draw3("SR", R.s / 1000.0f, "SS", S.s / 1000.0f, "ST", T.s / 1000.0f, "kVA");
      break;
    case 5:
      drawHeader("6) Reactive Power (Q)");
      draw3("QR", R.q / 1000.0f, "QS", S.q / 1000.0f, "QT", T.q / 1000.0f, "kVAr");
      break;
    case 6:
      drawHeader("7) Power Factor (PF)");
      draw3("pfR", R.pf, "pfS", S.pf, "pfT", T.pf, "");
      break;
    case 7:
      drawHeader("8) Frequency (Hz)");
      draw3("fR", R.f, "fS", S.f, "fT", T.f, "Hz");
      break;
    default:
      drawHeader("9) Vib Speed + Temp");
      tft.setCursor(10, 60);
      tft.printf("Speed : %7.1f mm/s", vibSp);

      tft.drawFastHLine(0, 110, tft.width(), TFT_DARKGREY);
      tft.setCursor(10, 145);
      tft.printf("Temp  : %7.2f C", motorTempC);
      tft.setCursor(10, 180);
      tft.printf("ESP32B: %s", linkOk ? "OK" : "STALE");
      break;
  }
}

void renderDone(bool sendOk) {
  tft.fillScreen(TFT_BLACK);
  tft.setTextColor(TFT_CYAN, TFT_BLACK);
  tft.setTextSize(2);
  tft.setCursor(10, 60);
  tft.print("Batch upload selesai");

  tft.setTextColor(TFT_WHITE, TFT_BLACK);
  tft.setCursor(10, 110);
  tft.print("Colab:");

  tft.setTextColor(sendOk ? TFT_GREEN : TFT_RED, TFT_BLACK);
  tft.setCursor(10, 140);
  tft.print(sendOk ? "BERHASIL" : "GAGAL");

  tft.setTextColor(TFT_WHITE, TFT_BLACK);
  tft.setCursor(10, 190);
  tft.print("Lanjut monitoring...");
}

// =========================
// THINGSBOARD JSON + POST
// =========================
String buildThingsBoardJSON() {
  BNodePayload bUse = getCurrentBPayload();
  bool linkOk = isBLinkFresh();

  float vibSp = bUse.vib_ok ? bUse.vib_speed_mm_s : 0.0f;

  String s;
  s.reserve(1400);

  s += "{";

  s += "\"voltage_rs\":" + String(VRS, 2) + ",";
  s += "\"voltage_st\":" + String(VST, 2) + ",";
  s += "\"voltage_tr\":" + String(VTR, 2) + ",";

  s += "\"voltage_rn\":" + String(VRN, 2) + ",";
  s += "\"voltage_sn\":" + String(VSN, 2) + ",";
  s += "\"voltage_tn\":" + String(VTN, 2) + ",";

  s += "\"current_r\":" + String(R.i, 3) + ",";
  s += "\"current_s\":" + String(S.i, 3) + ",";
  s += "\"current_t\":" + String(T.i, 3) + ",";

  s += "\"active_power_r\":" + String(R.p, 1) + ",";
  s += "\"active_power_s\":" + String(S.p, 1) + ",";
  s += "\"active_power_t\":" + String(T.p, 1) + ",";

  s += "\"apparent_power_r\":" + String(R.s, 1) + ",";
  s += "\"apparent_power_s\":" + String(S.s, 1) + ",";
  s += "\"apparent_power_t\":" + String(T.s, 1) + ",";

  s += "\"reactive_power_r\":" + String(R.q, 1) + ",";
  s += "\"reactive_power_s\":" + String(S.q, 1) + ",";
  s += "\"reactive_power_t\":" + String(T.q, 1) + ",";

  s += "\"frequency_r\":" + String(R.f, 2) + ",";
  s += "\"frequency_s\":" + String(S.f, 2) + ",";
  s += "\"frequency_t\":" + String(T.f, 2) + ",";

  s += "\"power_factor_r\":" + String(R.pf, 2) + ",";
  s += "\"power_factor_s\":" + String(S.pf, 2) + ",";
  s += "\"power_factor_t\":" + String(T.pf, 2) + ",";

  s += "\"temperature_motor\":" + String(motorTempC, 2) + ",";
  s += "\"vibration_speed_mm_s\":" + String(vibSp, 1) + ",";

  s += "\"esp32b_link_ok\":";
  s += (linkOk ? "true" : "false");
  s += ",";

  s += "\"phase_r_ok\":";
  s += (R.ok ? "true" : "false");
  s += ",";

  s += "\"phase_s_ok\":";
  s += (S.ok ? "true" : "false");
  s += ",";

  s += "\"phase_t_ok\":";
  s += (T.ok ? "true" : "false");

  s += "}";

  return s;
}

bool postTelemetryToThingsBoard(const String& json) {
  if (!ENABLE_THINGSBOARD) return false;

  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[TB] skipped: WiFi not connected");
    return false;
  }

  WiFiClientSecure client;
  client.setInsecure();

  HTTPClient http;
  http.setTimeout(7000);

  if (!http.begin(client, THINGSBOARD_TELEMETRY_URL)) {
    Serial.println("[TB] http.begin() FAILED");
    return false;
  }

  http.addHeader("Content-Type", "application/json");
  int code = http.POST(json);

  Serial.printf("[TB] POST -> HTTP %d\n", code);
  String resp = http.getString();
  if (resp.length()) {
    Serial.println("[TB] response:");
    Serial.println(resp);
  }

  http.end();
  return (code >= 200 && code < 300);
}

// =========================
// BUILD JSON BATCH + POST
// =========================
String buildBatchJSON() {
  String s;
  s.reserve(12000);

  s += "{";
  s += "\"device\":\"ESP32A_MULTIMON\",";
  s += "\"n\":" + String(sampleIdx) + ",";
  s += "\"samples\":[";

  for (uint8_t i = 0; i < sampleIdx; i++) {
    const Sample1s &x = samples[i];
    s += "{";
    s += "\"ts_ms\":" + String(x.ts_ms) + ",";
    s += "\"VRS\":" + String(x.VRS, 2) + ",\"VST\":" + String(x.VST, 2) + ",\"VTR\":" + String(x.VTR, 2) + ",";
    s += "\"VRN\":" + String(x.VRN, 2) + ",\"VSN\":" + String(x.VSN, 2) + ",\"VTN\":" + String(x.VTN, 2) + ",";
    s += "\"IR\":" + String(x.IR, 3) + ",\"IS\":" + String(x.IS, 3) + ",\"IT\":" + String(x.IT, 3) + ",";
    s += "\"PR_kW\":" + String(x.PRkW, 3) + ",\"PS_kW\":" + String(x.PSkW, 3) + ",\"PT_kW\":" + String(x.PTkW, 3) + ",";
    s += "\"SR_kVA\":" + String(x.SRkVA, 3) + ",\"SS_kVA\":" + String(x.SSkVA, 3) + ",\"ST_kVA\":" + String(x.STkVA, 3) + ",";
    s += "\"QR_kVAr\":" + String(x.QRkVAr, 3) + ",\"QS_kVAr\":" + String(x.QSkVAr, 3) + ",\"QT_kVAr\":" + String(x.QTkVAr, 3) + ",";
    s += "\"pfR\":" + String(x.pfR, 2) + ",\"pfS\":" + String(x.pfS, 2) + ",\"pfT\":" + String(x.pfT, 2) + ",";
    s += "\"fR\":" + String(x.fR, 2) + ",\"fS\":" + String(x.fS, 2) + ",\"fT\":" + String(x.fT, 2) + ",";
    s += "\"vib_speed_mm_s\":" + String(x.vib_speed_mm_s, 1) + ",";
    s += "\"Tmotor_C\":" + String(x.Tmotor_C, 2);
    s += "}";
    if (i < sampleIdx - 1) s += ",";
  }

  s += "]}";
  return s;
}

bool postBatchToColab(const String& json) {
  if (WiFi.status() != WL_CONNECTED) {
    Serial.println("[COLAB] skipped: WiFi not connected");
    return false;
  }

  WiFiClientSecure client;
  client.setInsecure();

  HTTPClient http;
  http.setTimeout(15000);

  if (!http.begin(client, COLAB_ENDPOINT_URL)) {
    Serial.println("[COLAB] http.begin() FAILED");
    return false;
  }

  http.addHeader("Content-Type", "application/json");
  int code = http.POST(json);

  Serial.printf("[COLAB] POST -> HTTP %d\n", code);
  String resp = http.getString();
  if (resp.length()) {
    Serial.println("[COLAB] response:");
    Serial.println(resp);
  }

  http.end();
  return (code >= 200 && code < 300);
}

// =========================
// SETUP
// =========================
void setup() {
  Serial.begin(115200);
  delay(300);

  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  delay(200);

  Serial.print("ESP32A MAC: ");
  Serial.println(WiFi.macAddress());

  tft.init();
  tft.setRotation(1);
  tft.fillScreen(TFT_BLACK);

  state = ST_SPLASH;
  stateStartMs = millis();
  renderSplash();

  Wire.begin();
  temp_ok = mlx.begin();

  PzemSerial1.begin(MODBUS_BAUD, SERIAL_8N1, PZEM1_RX, PZEM1_TX);
  PzemSerial2.begin(MODBUS_BAUD, SERIAL_8N1, PZEM2_RX, PZEM2_TX);
  delay(300);

  bool foundA1 = detectPZEMAddress(nodeA1, PzemSerial1, addrPzemA1);
  bool foundA2 = detectPZEMAddress(nodeA2, PzemSerial2, addrPzemA2);

  Serial.printf("A1 addr detect: %s -> 0x%02X\n", foundA1 ? "FOUND" : "FAIL", addrPzemA1);
  Serial.printf("A2 addr detect: %s -> 0x%02X\n", foundA2 ? "FOUND" : "FAIL", addrPzemA2);

  nodeA1.begin(addrPzemA1, PzemSerial1);
  nodeA2.begin(addrPzemA2, PzemSerial2);

  if (esp_now_init() != ESP_OK) {
    Serial.println("ESP-NOW init FAILED");
  } else {
    esp_now_register_recv_cb(onEspNowRecv);
    Serial.println("ESP-NOW RX READY");
  }

  lastSampleMs = millis();
  lastPageMs   = millis();
}

// =========================
// LOOP
// =========================
void loop() {
  uint32_t now = millis();

  if (state == ST_SPLASH) {
    if (now - stateStartMs >= SPLASH_MS) {
      state = ST_WIFI;
      stateStartMs = now;

      WiFi.mode(WIFI_STA);
      WiFi.begin(WIFI_SSID, WIFI_PASS);

      Serial.print("WiFi connecting to: ");
      Serial.println(WIFI_SSID);

      renderWiFiStatus("Connecting...");
    }
    return;
  }

  if (state == ST_WIFI) {
    if (WiFi.status() == WL_CONNECTED) {
      Serial.println("WiFi CONNECTED");
      Serial.print("IP: ");
      Serial.println(WiFi.localIP());
      Serial.print("WiFi channel: ");
      Serial.println(WiFi.channel());

      renderWiFiStatus("CONNECTED", true);
      delay(1200);

      state = ST_MONITORING;
      stateStartMs = millis();
      sampleIdx = 0;
      monitorPage = 0;
      lastSampleMs = millis();
      lastPageMs   = millis();
      lastTbPostMs = 0;
      renderMonitoringPage(monitorPage);
      return;
    }

    if (now - stateStartMs < WIFI_TIMEOUT_MS) {
      if ((now / 1000) % 2 == 0) renderWiFiStatus("Connecting...");
      else renderWiFiStatus("Connecting..");
      delay(200);
      return;
    }

    Serial.println("WiFi timeout -> lanjut monitoring offline");
    renderWiFiStatus("OFFLINE MODE", false);
    delay(1000);

    state = ST_MONITORING;
    stateStartMs = millis();
    sampleIdx = 0;
    monitorPage = 0;
    lastSampleMs = millis();
    lastPageMs   = millis();
    lastTbPostMs = 0;
    renderMonitoringPage(monitorPage);
    return;
  }

  if (state == ST_MONITORING) {
    keepWiFiAliveDuringMonitoring();

    bool pageChanged = false;

    if (now - lastPageMs >= PAGE_INTERVAL_MS) {
      lastPageMs = now;
      monitorPage = (monitorPage + 1) % 9;
      pageChanged = true;
    }

    if (now - lastSampleMs >= SAMPLE_INTERVAL_MS) {
      lastSampleMs = now;

      uint8_t errA1 = 0, errA2 = 0;
      PhaseMetrics nowA1{}, nowA2{};

      bool okA1 = readPZEM_WithRetry(nodeA1, PzemSerial1, nowA1, errA1);
      bool okA2 = readPZEM_WithRetry(nodeA2, PzemSerial2, nowA2, errA2);

      if (okA1) {
        a1 = nowA1;
        sanitizePhaseMetrics(a1);
        Serial.printf("[A1] OK V=%.1f I=%.3f S=%.1f P=%.1f F=%.1f PF=%.2f Q=%.1f\n",
                      a1.v, a1.i, a1.s, a1.p, a1.f, a1.pf, a1.q);
      } else {
        a1 = {};
        Serial.printf("[A1] FAIL code=%u (%s) -> set 0\n", errA1, modbusResultText(errA1));
      }

      if (okA2) {
        a2 = nowA2;
        sanitizePhaseMetrics(a2);
        Serial.printf("[A2] OK V=%.1f I=%.3f S=%.1f P=%.1f F=%.1f PF=%.2f Q=%.1f\n",
                      a2.v, a2.i, a2.s, a2.p, a2.f, a2.pf, a2.q);
      } else {
        a2 = {};
        Serial.printf("[A2] FAIL code=%u (%s) -> set 0\n", errA2, modbusResultText(errA2));
      }

      if (temp_ok) {
        float t = mlx.readObjectTempC();
        motorTempC = (!isnan(t) && t > -40 && t < 200) ? t : 0.0f;
      } else {
        motorTempC = 0.0f;
      }

      buildThreePhase();
      BNodePayload bUse = getCurrentBPayload();

      if (sampleIdx < TOTAL_SAMPLES) {
        Sample1s &x = samples[sampleIdx];
        x.ts_ms = now/1000;

        x.VRS = VRS; x.VST = VST; x.VTR = VTR;
        x.VRN = VRN; x.VSN = VSN; x.VTN = VTN;

        x.IR = R.i; x.IS = S.i; x.IT = T.i;

        x.PRkW = R.p / 1000.0f; x.PSkW = S.p / 1000.0f; x.PTkW = T.p / 1000.0f;
        x.SRkVA = R.s / 1000.0f; x.SSkVA = S.s / 1000.0f; x.STkVA = T.s / 1000.0f;
        x.QRkVAr = R.q / 1000.0f; x.QSkVAr = S.q / 1000.0f; x.QTkVAr = T.q / 1000.0f;

        x.pfR = R.pf; x.pfS = S.pf; x.pfT = T.pf;
        x.fR = R.f;   x.fS = S.f;   x.fT = T.f;

        x.vib_speed_mm_s = bUse.vib_ok ? bUse.vib_speed_mm_s : 0.0f;
        x.Tmotor_C       = motorTempC;

        sampleIdx++;
      }

      if (ENABLE_THINGSBOARD && (now - lastTbPostMs >= TB_POST_INTERVAL_MS)) {
        String tbJson = buildThingsBoardJSON();
        bool tbOk = postTelemetryToThingsBoard(tbJson);
        lastTbPostMs = now;
        Serial.printf("[TB] telemetry %s\n", tbOk ? "sent" : "failed/skipped");
      }

      renderMonitoringPage(monitorPage);
      Serial.printf("[sample %u/%u] saved\n", sampleIdx, TOTAL_SAMPLES);

      if (sampleIdx >= TOTAL_SAMPLES) {
        state = ST_SENDING;
        stateStartMs = millis();
      }
      return;
    }

    if (pageChanged) {
      renderMonitoringPage(monitorPage);
    }
    return;
  }

  if (state == ST_SENDING) {
    tft.fillScreen(TFT_BLACK);
    tft.setTextColor(TFT_CYAN, TFT_BLACK);
    tft.setTextSize(2);
    tft.setCursor(10, 80);
    tft.print("Sending to Colab...");
    tft.setCursor(10, 120);
    tft.print("Please wait");

    Serial.println("Sending batch to Colab...");

    String js = buildBatchJSON();
    bool ok = postBatchToColab(js);

    renderDone(ok);
    delay(1200);

    sampleIdx = 0;
    state = ST_MONITORING;
    stateStartMs = millis();
    lastSampleMs = millis();
    lastPageMs   = millis();
    lastTbPostMs = 0;
    monitorPage  = 0;

    return;
  }
}
