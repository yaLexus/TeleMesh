# Подробное логгирование для отладки
#DEBUG = True

# === ПАРАМЕТРЫ ПОДКЛЮЧЕНИЯ ===
# --------------------- Telegram ---------------------
#SESSION_NAME = "telemesh_session" # ["telemesh_session"]

# --------------------- Meshtastic ---------------------
# Варианты подключения (раскомментируй нужный):
# 1. Через USB. Проверь ls /dev/tty* до и после подключения
#SERIAL_PORT = "/dev/ttyUSB0"
# 2. Через TCP (если Meshtastic MQTT или native TCP)
TCP_HOST = "192.168.1.10"
TCP_PORT = 4403

# === ПАРАМЕТРЫ ФОРМАТИРОВАНИЯ СООБЩЕНИЙ ===
# Ограничение длины сообщения (Meshtastic обычно 200–240 символов) [200]
#MAX_MSG_LEN = 200

# Замена кирилических символов латинскими для экономии места в сообщении [True]
#TRANSLIT_ENABLED = True

# Приветственное сообщение при запуске бота
#GOODBYE_MSG = "⛔️ TeleMesh остановлен" # ["⛔️ TeleMesh остановлен"]

# Задержка между отправкой отдельных частей сообщения в миллисекундах
#MESSAGE_SEND_DELAY = 3000   # [3000]
# Путь к папке с конфигами аккаунтов ТГ
#ACC_BD_PATH = "tg_accounts" # ["."]

# Разрешить отправлять ответы в ТГ
#FORWARD_ENABLED = True
# Подпись сообщений отправляемых в ТГ
#SIGNATURE = """
#📡 Отправлено из меш-сети с помощью 📟 **TeleMesh**"""

# === КОНТРОЛЬ ДОСТАВКИ ===
# Таймаут перед повторной отправкой в секундах
#ACK_TIMEOUT = 60 # [60]
# Количество повторов
#MAX_RETRIES = 3 # [3]
# Уведомление в ТГ о неудачной доставке. Если пустое или отсутствует - уведомления отключены
FAIL_MSG = "❌ Не удалось доставить сообщение в меш-сеть"

# === ПЕРЕСЫЛКА ТЕЛЕМЕТРИИ в лс ===
# Отправлять данные о погоде в Telegram (в админ-канал)
#ENVIRONMENT_TELEGRAM_FORWARD = False # [False]
# Отправлять данные о погоде в Mesh (целевой ноде)
#ENVIRONMENT_MESH_FORWARD = True # [False]

# Включить поддержку реакций TG → Mesh
#REACTIONS_ENABLED = True

# Включить поддержку reply из Mesh → конкретному автору в TG
#REPLY_TRACKING_ENABLED = True

# Время хранения кэша сообщений (в секундах), по умолчанию 24 часа
#MSG_CACHE_TTL = 86400

# Максимальный размер кэша сообщений
#MSG_CACHE_MAX_SIZE = 1000

# Путь и имя файла записной книги
#ADDRESSBOOK_PATH = "addressbook.json"

# --------------------- Telegram Proxy / Шлюз ---------------------
# None = прямое подключение
# Поддерживаемые форматы:
#   TELEGRAM_PROXY = None
#   TELEGRAM_PROXY = ('socks5', '127.0.0.1', 1080)
#   TELEGRAM_PROXY = ('http', 'proxy.example.com', 8080, 'user', 'pass')
#   TELEGRAM_PROXY = {                     # MTProxy (самый частый вариант)
#       'proxy_type': 'mtproxy',
#       'addr': 'ваш_шлюз_из_table_200',
#       'port': 443,
#       'secret': 'ddxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'   # 32-байтный secret
#   }
#TELEGRAM_PROXY = None

#TELEGRAM_BIND_INTERFACE = "wg0"

