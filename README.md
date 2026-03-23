# 📟 TeleMesh

**Telegram ↔ Meshtastic Bridge** — мост для интеграции мессенджера Telegram с mesh-сетью Meshtastic.

---

## 🇷🇺 Описание (Русский)

TeleMesh — это мощный мост между Telegram и Meshtastic, позволяющий пользователям Telegram общаться с участниками mesh-сети и наоборот. Скрипт поддерживает двунаправленную пересылку сообщений, реакций, отслеживание ответов и множество других функций.

### ✨ Возможности

#### 📨 Пересылка сообщений
- **Двунаправленная пересылка** — сообщения из Telegram попадают в Mesh, и наоборот
- **Автоматическое разбиение** — длинные сообщения разбиваются на части с нумерацией `[1/2], [2/2]`
- **Ограничение длины** — настраиваемое ограничение длины сообщений для mesh-сети

#### 😀 Реакции
- **TG → Mesh** — реакции из Telegram пересылаются в Meshtastic
- **Mesh → TG** — реакции из Mesh отправляются автору оригинального сообщения в Telegram
- **Fallback в текст** — если реакция не может быть отправлена нативно, она приходит текстом с подписью

#### ↩️ Отслеживание ответов (Reply Tracking)
- При ответе на сообщение из Mesh в Telegram, ответ отправляется **оригинальному автору** сообщения, а не последнему отправителю
- Кэш сообщений с TTL для корректной маршрутизации ответов

#### 🔄 Транслитерация
- Автоматическая транслитерация кириллицы в латиницу для отображения на устройствах Meshtastic
- Визуально понятное преобразование: `Привет` → `Privet`

#### ⚡ Механизм ACK
- Подтверждение доставки сообщений в Mesh
- Автоматические повторные отправки при отсутствии подтверждения
- Настраиваемое количество попыток и таймауты
- Уведомления о недоставке (опционально)

#### 📊 Телеметрия
- Пересылка телеметрии устройств Meshtastic в Telegram (опционально)
- Поддержка Environment Metrics (температура, влажность и т.д.)

#### 🎮 Управляющие команды
- `!стоп` / `!stop` — приостановить пересылку
- `!старт` / `!start` — возобновить пересылку
- Управление **только из Mesh** для безопасности
- Нормализация команд: `! старт`, `! Cтapт` (лат+кир) → распознаются корректно

#### 📎 Обработка медиа
- Автоматическое определение типа вложения
- Поддерживаемые типы: `[PIC]`, `[GIF]`, `[VIDEO]`, `[AUDIO]`, `[VOICE]`, `[DOC]`
- Сообщения с медиа без текста корректно обрабатываются

#### 🔗 Обработка ссылок
- Извлечение заголовков страниц по URL
- Специальная обработка YouTube ссылок с префиксом `▶️`
- Обычные ссылки с префиксом `🔗`

#### 📝 Подписи
- Настраиваемая подпись к сообщениям из Mesh
- По умолчанию: `📡 Отправлено из меш-сети с помощью 📟 TeleMesh`

#### 🐛 Режим отладки
- Детальное логирование всех операций
- Отладочная информация о пакетах, кэше, маршрутизации

---

## 🇬🇧 Description (English)

TeleMesh is a powerful bridge between Telegram and Meshtastic, allowing Telegram users to communicate with mesh network participants and vice versa. The script supports bidirectional message forwarding, reactions, reply tracking, and many other features.

### ✨ Features

#### 📨 Message Forwarding
- **Bidirectional forwarding** — messages from Telegram go to Mesh, and vice versa
- **Automatic splitting** — long messages are split into parts with numbering `[1/2], [2/2]`
- **Length limiting** — configurable message length limit for mesh network

#### 😀 Reactions
- **TG → Mesh** — reactions from Telegram are forwarded to Meshtastic
- **Mesh → TG** — reactions from Mesh are sent to the original message author in Telegram
- **Text fallback** — if reaction cannot be sent natively, it arrives as text with signature

#### ↩️ Reply Tracking
- When replying to a Mesh message in Telegram, the reply goes to the **original author**, not the last sender
- Message cache with TTL for correct reply routing

#### 🔄 Transliteration
- Automatic Cyrillic to Latin transliteration for display on Meshtastic devices
- Visually understandable conversion: `Привет` → `Privet`

#### ⚡ ACK Mechanism
- Message delivery confirmation in Mesh
- Automatic retries on missing confirmation
- Configurable retry count and timeouts
- Delivery failure notifications (optional)

#### 📊 Telemetry
- Forward Meshtastic device telemetry to Telegram (optional)
- Environment Metrics support (temperature, humidity, etc.)

#### 🎮 Control Commands
- `!stop` / `!стоп` — pause forwarding
- `!start` / `!старт` — resume forwarding
- Control **from Mesh only** for security
- Command normalization: `! start`, `! Cтapт` (mixed Latin+Cyrillic) → recognized correctly

#### 📎 Media Handling
- Automatic attachment type detection
- Supported types: `[PIC]`, `[GIF]`, `[VIDEO]`, `[AUDIO]`, `[VOICE]`, `[DOC]`
- Media-only messages (without text) are handled correctly

#### 🔗 URL Handling
- Page title extraction from URLs
- Special YouTube link handling with `▶️` prefix
- Regular links with `🔗` prefix

#### 📝 Signatures
- Configurable signature for messages from Mesh
- Default: `📡 Отправлено из меш-сети с помощью 📟 TeleMesh`

#### 🐛 Debug Mode
- Detailed logging of all operations
- Debug info about packets, cache, routing

---

## 📦 Installation / Установка

```bash
pip install telethon meshtastic requests pubsub
```

---

## ⚙️ Configuration / Конфигурация

Create a configuration file (e.g., `my_acc.py`):

Создайте файл конфигурации (например, `my_acc.py`):

```python
# Telegram API credentials (get from https://my.telegram.org)
API_ID = 12345
API_HASH = "your-api-hash-here"
PHONE = "+79001234567"

# Meshtastic destination node ID
DEST_NODE_ID = "!69823980"

# Optional: Admin chat ID for notifications
ADMIN_CHAT_ID = 123456789
```

### Optional config.py settings / Опциональные настройки config.py:

```python
# Connection type (choose one)
TCP_HOST = "10.1.11.111"  # IP address of Meshtastic device
TCP_PORT = 4403           # TCP port
# OR
SERIAL_PORT = "/dev/ttyUSB0"  # Serial connection

# Message settings
MAX_MSG_LEN = 200          # Max message length
MESSAGE_SEND_DELAY = 1000  # Delay between messages (ms)

# Features
FORWARD_ENABLED = True     # Enable forwarding on start
DEBUG = False              # Debug mode
FAIL_MSG = "⚠️ Сообщение не доставлено"  # Delivery failure notification

# Signature
SIGNATURE = """
📡 Отправлено из меш-сети с помощью 📟 **TeleMesh**"""
```

---

## 🚀 Usage / Запуск

```bash
python telemesh_v1.4.py my_acc.py
```

---

## 🎮 Commands / Команды

| Command | Description RU | Description EN |
|---------|----------------|----------------|
| `!стоп` / `!stop` | Приостановить пересылку | Pause forwarding |
| `!старт` / `!start` | Возобновить пересылку | Resume forwarding |

> ⚠️ Commands work **from Mesh only** for security reasons.
> 
> ⚠️ Команды работают **только из Mesh** по соображениям безопасности.

---

## 📋 Requirements / Требования

- Python 3.8+
- Telegram API credentials
- Meshtastic device (connected via TCP or Serial)
- Telegram account (bot not required)

---

## 📄 License / Лицензия

MIT License

---

## 🙏 Credits / Благодарности

### 💝 Special Thanks / Особая благодарность

**🇷🇺 Огромная благодарность за помощь в отладке и тестировании этого проекта:**

**Моей любимой жене** — за бесконечное терпение, поддержку и тестирование реакций 👀

**Моему сыну** — за энтузиазм и помощь в проверке работы скрипта

**Сан Санычу** — за ценные советы и активное участие в тестировании mesh-связи

---

**🇬🇧 Huge thanks for help with debugging and testing this project:**

**To my beloved wife** — for endless patience, support, and testing reactions 👀

**To my son** — for enthusiasm and help with script testing

**To San Sanych** — for valuable advice and active participation in mesh communication testing

---

### 🛠️ Technical Credits

- [Telethon](https://github.com/LonamiWebs/Telethon) - Telegram client library
- [Meshtastic](https://github.com/meshtastic) - Mesh network project
- [Meshtastic Python](https://github.com/meshtastic/python) - Meshtastic Python library
