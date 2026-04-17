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

#### 📒 Записная книжка (Contact Book)
- **Добавление контактов** — команда `!add` (или `!добавить`) при ответе на сообщение добавляет пользователя Telegram в записную книжку
- **Список контактов** — команда `!list` (или `!список`) выводит всех сохранённых пользователей с номерами
- **Удаление контактов** — команда `!del` (или `!удалить`) удаляет запись по номеру или текущего пользователя (при ответе)
- **Отправка контактам** — формат `!5 Привет` отправляет сообщение контакту под номером 5
- **Отправка по @username** — формат `@username сообщение` отправляет сообщение пользователю Telegram по его юзернейму
- **Персистентность** — контакты сохраняются в файл `contacts_<ID_ноды>.json` (разные файлы для разных целевых нод)

#### 😀 Реакции
- **TG → Mesh** — реакции из Telegram пересылаются в Meshtastic (поддерживаются как обычные сообщения, так и сообщения, отправленные через `!n` или `@username`)
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

#### 🚀 Дополнительные возможности
- **Флаг `--no-welcome`** — при запуске не отправляет приветственное сообщение в Mesh (полезно для перезапусков)
- **Динамические файлы контактов** — для каждого `DEST_NODE_ID` своя записная книжка, что позволяет использовать один экземпляр скрипта с разными целевыми нодами

### 🌐 Настройки подключения к Telegram (новое)

- **`TELEGRAM_PROXY`** — подключение через прокси или шлюз (MTProxy из таблицы 200, SOCKS5, HTTP и др.)
- **`TELEGRAM_BIND_INTERFACE`** — привязка всего Telegram-трафика к конкретному сетевому интерфейсу (например `awg0`, `wg0`, `tun0`)

---
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

#### 📒 Contact Book
- **Add contacts** — command `!add` (or `!добавить`) when replying to a message adds the Telegram user to the contact book
- **List contacts** — command `!list` (or `!список`) shows all saved users with their slot numbers
- **Delete contacts** — command `!del` (or `!удалить`) deletes a record by slot number or the current user (when replying)
- **Send to contact by slot** — format `!5 Hello` sends a message to the contact in slot 5
- **Send by @username** — format `@username message` sends a message to a Telegram user by their username
- **Persistence** — contacts are stored in `contacts_<node_id>.json` (separate files for different target nodes)

#### 😀 Reactions
- **TG → Mesh** — reactions from Telegram are forwarded to Meshtastic (works for both regular messages and those sent via `!n` or `@username`)
- **Mesh → TG** — reactions from Mesh are sent to the original message author in Telegram
- **Text fallback** — if a reaction cannot be sent natively, it arrives as text with signature

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

#### 🚀 Additional Features
- **`--no-welcome` flag** — suppresses the welcome message sent to Mesh on startup (useful for restarts)
- **Dynamic contact files** — each `DEST_NODE_ID` has its own contact book, allowing one script instance to work with different target nodes

### 🌐 Telegram Connection Options (new)

- **`TELEGRAM_PROXY`** — connect via proxy/gateway (MTProxy from table 200, SOCKS5, HTTP)
- **`TELEGRAM_BIND_INTERFACE`** — bind Telegram traffic to a specific network interface (`awg0`, `wg0`, etc.)

---

#### 🐛 Debug Mode
- Detailed logging of all operations
- Debug info about packets, cache, routing

---

## 📦 Installation / Установка

```bash
pip install telethon meshtastic requests pubsub
