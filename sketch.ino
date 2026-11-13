// ĐỀ 3: MẠCH BÁO CHÁY GIA ĐÌNH - ESP32 + MQ2 + DHT22 + MQTT + FreeRTOS
// Core 0: TaskTruyenData  (WiFi + MQTT, đọc dataQueue và publish)
// Core 1: TaskDocData     (đọc cảm biến, in Serial, gửi dataQueue + alertQueue)
// Core 1: TaskCanhBao     (đọc alertQueue, điều khiển quạt / máy bơm / còi)
#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <stdarg.h>

// ---------- WiFi (Wokwi) ----------
const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASS = "";

// ---------- MQTT Broker ----------
const char* MQTT_HOST = "broker.hivemq.com";
const int   MQTT_PORT = 1883;

#define BASE "nhom3/baochay/"
const char* T_GAS        = BASE "gas";
const char* T_TEMP       = BASE "temp";
const char* T_FAN_STATE  = BASE "fan/state";
const char* T_PUMP_STATE = BASE "pump/state";
const char* T_MODE_STATE = BASE "mode/state";
const char* T_ALARM      = BASE "alarm";
const char* T_FAN_SET    = BASE "fan/set";
const char* T_PUMP_SET   = BASE "pump/set";
const char* T_MODE_SET   = BASE "mode/set";

// ---------- Chân ----------
#define PIN_MQ2    34
#define PIN_DHT    15
#define PIN_FAN    26
#define PIN_PUMP   27
#define PIN_BUZZER 25

// ---------- Ngưỡng (gas tính theo ADC đã lọc nhiễu, 0..4095) ----------
const int   GAS_ON   = 2500;
const int   GAS_OFF  = 2000;
const float TEMP_ON  = 50.0;
const float TEMP_OFF = 45.0;

// ---------- Cấu hình đọc gas ----------
const int GAS_SAMPLES = 32;   // số mẫu mỗi lần đo
const int GAS_TRIM    = 4;    // bỏ 4 mẫu thấp nhất + 4 mẫu cao nhất rồi lấy trung bình

// ---------- Dữ liệu & hàng đợi ----------
struct SensorData {
  uint32_t seq;     // số thứ tự mẫu, để theo dõi trong log
  int      gas;     // ADC đã lọc
  float    temp;    // °C
};

QueueHandle_t dataQueue;   // TaskDocData -> TaskTruyenData
QueueHandle_t alertQueue;  // TaskDocData -> TaskCanhBao
SemaphoreHandle_t serialMutex;

// ---------- Trạng thái dùng chung ----------
volatile bool autoMode = true;
volatile bool fanOn    = false;
volatile bool pumpOn   = false;
volatile int  alarmLvl = 0;   // 0=SAFE, 1=WARNING, 2=FIRE

DHT dht(PIN_DHT, DHT22);
WiFiClient espClient;
PubSubClient mqtt(espClient);

const char* alarmText(int a) {
  return a == 2 ? "FIRE" : (a == 1 ? "WARNING" : "SAFE");
}

// ---------- In Serial an toàn giữa các task ----------
// Dạng: [  12.34s][C1][DOC     ] nội dung
void logf(const char* tag, const char* fmt, ...) {
  char msg[160];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(msg, sizeof(msg), fmt, ap);
  va_end(ap);
  if (serialMutex) xSemaphoreTake(serialMutex, portMAX_DELAY);
  Serial.printf("[%7.2fs][C%d][%-8s] %s\n", millis() / 1000.0, xPortGetCoreID(), tag, msg);
  if (serialMutex) xSemaphoreGive(serialMutex);
}

// ---------- Đọc gas: lấy nhiều mẫu, sắp xếp, bỏ cực trị, lấy trung bình ----------
int readGasADC() {
  int s[GAS_SAMPLES];
  for (int i = 0; i < GAS_SAMPLES; i++) {
    s[i] = analogRead(PIN_MQ2);
    delayMicroseconds(300);
  }
  for (int i = 1; i < GAS_SAMPLES; i++) {          // insertion sort
    int k = s[i], j = i - 1;
    while (j >= 0 && s[j] > k) { s[j + 1] = s[j]; j--; }
    s[j + 1] = k;
  }
  long sum = 0;
  for (int i = GAS_TRIM; i < GAS_SAMPLES - GAS_TRIM; i++) sum += s[i];
  return (int)(sum / (GAS_SAMPLES - 2 * GAS_TRIM));
}

void setFan(bool on)  { fanOn = on;  digitalWrite(PIN_FAN,  on ? HIGH : LOW); }
void setPump(bool on) { pumpOn = on; digitalWrite(PIN_PUMP, on ? HIGH : LOW); }

// Nhận lệnh từ Web (chạy trong mqtt.loop() -> Core 0)
void onMessage(char* topic, byte* payload, unsigned int len) {
  String msg = "";
  for (unsigned int i = 0; i < len; i++) msg += (char)payload[i];
  String t = String(topic);
  logf("MQTT-RX", "%s = %s", topic, msg.c_str());

  if (t == T_MODE_SET) {
    autoMode = (msg == "AUTO");
    logf("MQTT-RX", ">> Doi che do: %s", autoMode ? "AUTO" : "MANUAL");
  } else if (t == T_FAN_SET || t == T_PUMP_SET) {
    if (autoMode) {
      logf("MQTT-RX", ">> Bo qua: dang o che do AUTO");
    } else if (t == T_FAN_SET) {
      setFan(msg == "ON");
      logf("MQTT-RX", ">> MANUAL: Quat %s", fanOn ? "BAT" : "TAT");
    } else {
      setPump(msg == "ON");
      logf("MQTT-RX", ">> MANUAL: May bom %s", pumpOn ? "BAT" : "TAT");
    }
  }
}

bool connectMQTT() {
  String id = "esp32_baochay_" + String(esp_random());
  logf("TRUYEN", "Dang ket noi MQTT %s:%d ...", MQTT_HOST, MQTT_PORT);
  if (mqtt.connect(id.c_str())) {
    logf("TRUYEN", "MQTT OK (clientId=%s)", id.c_str());
    mqtt.subscribe(T_FAN_SET);
    mqtt.subscribe(T_PUMP_SET);
    mqtt.subscribe(T_MODE_SET);
    return true;
  }
  logf("TRUYEN", "MQTT loi rc=%d, thu lai sau 2s", mqtt.state());
  return false;
}

// =====================================================
// TASK TRUYỀN DATA - Core 0
// =====================================================
void TaskTruyenData(void* pv) {
  logf("TRUYEN", "Task bat dau tren Core %d", xPortGetCoreID());

  // 1. Kết nối WiFi
  logf("TRUYEN", "Dang ket noi WiFi '%s' ...", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  while (WiFi.status() != WL_CONNECTED) vTaskDelay(pdMS_TO_TICKS(300));
  logf("TRUYEN", "WiFi OK, IP: %s", WiFi.localIP().toString().c_str());

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setCallback(onMessage);

  int lastAlarm = -1, lastFan = -1, lastPump = -1, lastMode = -1;
  SensorData d;

  for (;;) {
    if (WiFi.status() != WL_CONNECTED) {
      logf("TRUYEN", "Mat WiFi, ket noi lai...");
      WiFi.reconnect();
      vTaskDelay(pdMS_TO_TICKS(1000));
      continue;
    }
    if (!mqtt.connected()) {
      lastAlarm = lastFan = lastPump = lastMode = -1;   // publish lại trạng thái sau khi nối lại
      if (!connectMQTT()) { vTaskDelay(pdMS_TO_TICKS(2000)); continue; }
    }
    mqtt.loop();

    // 2. Đọc data Queue và gửi lên broker
    if (xQueueReceive(dataQueue, &d, pdMS_TO_TICKS(50)) == pdTRUE) {
      bool ok1 = mqtt.publish(T_GAS,  String(d.gas).c_str());
      bool ok2 = mqtt.publish(T_TEMP, String(d.temp, 1).c_str());
      logf("TRUYEN", "#%lu gui MQTT: gas=%d temp=%.1f -> %s | con %u mau trong queue",
           (unsigned long)d.seq, d.gas, d.temp, (ok1 && ok2) ? "OK" : "LOI",
           (unsigned)uxQueueMessagesWaiting(dataQueue));
    }

    // 3. Gửi trạng thái khi có thay đổi
    int a = alarmLvl, f = fanOn, p = pumpOn, m = autoMode;
    if (a != lastAlarm) { mqtt.publish(T_ALARM,      alarmText(a),          true); lastAlarm = a;
                          logf("TRUYEN", "Gui canh bao: %s", alarmText(a)); }
    if (f != lastFan)   { mqtt.publish(T_FAN_STATE,  f ? "ON" : "OFF",      true); lastFan = f;
                          logf("TRUYEN", "Gui trang thai Quat: %s", f ? "ON" : "OFF"); }
    if (p != lastPump)  { mqtt.publish(T_PUMP_STATE, p ? "ON" : "OFF",      true); lastPump = p;
                          logf("TRUYEN", "Gui trang thai May bom: %s", p ? "ON" : "OFF"); }
    if (m != lastMode)  { mqtt.publish(T_MODE_STATE, m ? "AUTO" : "MANUAL", true); lastMode = m;
                          logf("TRUYEN", "Gui che do: %s", m ? "AUTO" : "MANUAL"); }
  }
}

// =====================================================
// TASK ĐỌC DATA - Core 1
// =====================================================
void TaskDocData(void* pv) {
  logf("DOC", "Task bat dau tren Core %d", xPortGetCoreID());
  TickType_t last = xTaskGetTickCount();
  uint32_t seq = 0;

  for (;;) {
    SensorData d;
    d.seq  = ++seq;
    d.gas  = readGasADC();
    d.temp = dht.readTemperature();

    if (isnan(d.temp)) {
      logf("DOC", "#%lu LOI doc DHT22, bo qua mau nay", (unsigned long)d.seq);
    } else {
      logf("DOC", "#%lu Gas=%4d ADC (~%4d mV, %3d%%) | Nhiet do=%.1f C",
           (unsigned long)d.seq, d.gas, (int)(d.gas * 3300L / 4095), (int)(d.gas * 100L / 4095), d.temp);

      bool q1 = xQueueSend(dataQueue,  &d, 0) == pdTRUE;
      bool q2 = xQueueSend(alertQueue, &d, 0) == pdTRUE;
      if (!q1) logf("DOC", "CANH BAO: dataQueue day, mat mau #%lu", (unsigned long)d.seq);
      if (!q2) logf("DOC", "CANH BAO: alertQueue day, mat mau #%lu", (unsigned long)d.seq);
    }
    vTaskDelayUntil(&last, pdMS_TO_TICKS(2000));
  }
}

// =====================================================
// TASK CẢNH BÁO - Core 1
// =====================================================
void TaskCanhBao(void* pv) {
  logf("CANHBAO", "Task bat dau tren Core %d", xPortGetCoreID());
  SensorData d;
  bool beepOn = false;

  for (;;) {
    // Chờ mẫu mới tối đa 250 ms; hết thời gian chờ thì vẫn chạy phần còi bíp bên dưới
    if (xQueueReceive(alertQueue, &d, pdMS_TO_TICKS(250)) == pdTRUE) {
      if (autoMode) {
        if (d.gas >= GAS_ON && !fanOn) {
          setFan(true);
          logf("CANHBAO", "#%lu Gas %d >= %d -> BAT QUAT", (unsigned long)d.seq, d.gas, GAS_ON);
        } else if (d.gas <= GAS_OFF && fanOn) {
          setFan(false);
          logf("CANHBAO", "#%lu Gas %d <= %d -> TAT QUAT", (unsigned long)d.seq, d.gas, GAS_OFF);
        }
        if (d.temp >= TEMP_ON && !pumpOn) {
          setPump(true);
          logf("CANHBAO", "#%lu Nhiet do %.1f >= %.0f -> BAT MAY BOM", (unsigned long)d.seq, d.temp, TEMP_ON);
        } else if (d.temp <= TEMP_OFF && pumpOn) {
          setPump(false);
          logf("CANHBAO", "#%lu Nhiet do %.1f <= %.0f -> TAT MAY BOM", (unsigned long)d.seq, d.temp, TEMP_OFF);
        }
      }

      bool gasHigh = d.gas >= GAS_ON, tempHigh = d.temp >= TEMP_ON;
      int newLvl = (gasHigh && tempHigh) ? 2 : ((gasHigh || tempHigh) ? 1 : 0);
      alarmLvl = newLvl;

      // Dòng tổng hợp trạng thái SAU khi đã xử lý
      logf("CANHBAO", "#%lu Quat=%s | May bom=%s | Che do=%s | Canh bao=%s%s",
           (unsigned long)d.seq, fanOn ? "ON " : "OFF", pumpOn ? "ON " : "OFF",
           autoMode ? "AUTO  " : "MANUAL", alarmText(newLvl),
           newLvl == 2 ? " | COI KEU!" : "");
    }

    // Còi: khi FIRE thì bíp 250 ms bật / 250 ms tắt (tone 2 kHz); hết FIRE thì tắt hẳn
    if (alarmLvl == 2) {
      beepOn = !beepOn;
      if (beepOn) tone(PIN_BUZZER, 2000);
      else        noTone(PIN_BUZZER);
    } else if (beepOn) {
      beepOn = false;
      noTone(PIN_BUZZER);
    }
  }
}

void setup() {
  Serial.begin(115200);
  delay(500);

  analogReadResolution(12);
  analogSetPinAttenuation(PIN_MQ2, ADC_11db);   // đo được tới ~3.3V

  pinMode(PIN_FAN, OUTPUT);
  pinMode(PIN_PUMP, OUTPUT);
  pinMode(PIN_BUZZER, OUTPUT);
  dht.begin();

  serialMutex = xSemaphoreCreateMutex();
  dataQueue   = xQueueCreate(10, sizeof(SensorData));
  alertQueue  = xQueueCreate(10, sizeof(SensorData));

  Serial.println();
  Serial.println("==================== BAO CHAY GIA DINH - RTOS ====================");
  Serial.println("Dinh dang log: [thoi gian][Core][Task] noi dung");
  Serial.printf ("Nguong: Gas BAT>=%d / TAT<=%d (ADC) | Nhiet do BAT>=%.0f / TAT<=%.0f (C)\n",
                 GAS_ON, GAS_OFF, TEMP_ON, TEMP_OFF);
  Serial.println("===================================================================");

  
  xTaskCreatePinnedToCore(TaskTruyenData, "TruyenData", 8192, NULL, 1, NULL, 0);
  xTaskCreatePinnedToCore(TaskDocData,    "DocData",    5120, NULL, 2, NULL, 1);
  xTaskCreatePinnedToCore(TaskCanhBao,    "CanhBao",    5120, NULL, 2, NULL, 1);
}

void loop() {
  vTaskDelete(NULL); 
}
