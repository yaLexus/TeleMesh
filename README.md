# 📟 TeleMesh

**Telegram ↔ Meshtastic Bridge** — мост для интеграции мессенджера Telegram с mesh-сетью Meshtastic.

---

## 🇷🇺 Описание (Русский)

TeleMesh — это мощный мост между Telegram и Meshtastic, позволяющий пользователям Telegram общаться с участниками mesh-сети и наоборот.

### ✨ Возможности

(… весь предыдущий текст до раздела **🚀 Дополнительные возможности** остаётся без изменений …)

#### 🌐 Настройки подключения к Telegram (новое)

- **`TELEGRAM_PROXY`** — подключение Telegram через прокси или шлюз  
  Поддерживаются: **MTProxy** (из таблицы 200), SOCKS5, HTTP, HTTPS.  
  Полезно для обхода блокировок и использования выделенных шлюзов.

- **`TELEGRAM_BIND_INTERFACE`** — привязка всего Telegram-трафика к конкретному сетевому интерфейсу  
  Пример: `awg0`, `wg0`, `tun0`, `eth0` и любой другой.  
  Позволяет запускать TeleMesh через определённый VPN/туннель (WireGuard, AmneziaWG и т.д.).

---

## 🇬🇧 Description (English)

(… английская часть до **🚀 Additional Features** …)

#### 🌐 Telegram Connection Settings (new)

- **`TELEGRAM_PROXY`** — connect through proxy or gateway  
  Supports: **MTProxy** (from table 200), SOCKS5, HTTP, HTTPS.

- **`TELEGRAM_BIND_INTERFACE`** — bind all Telegram traffic to a specific network interface  
  Example: `awg0`, `wg0`, `tun0`, `eth0`.  
  Useful when you want TeleMesh to work exclusively through a particular VPN tunnel.

---

## 📝 Примеры настроек в `config.py`

```python
# Прокси / шлюз (MTProxy из таблицы 200)
TELEGRAM_PROXY = {
    'proxy_type': 'mtproxy',
    'addr': 'ваш_шлюз.table200.ru',
    'port': 443,
    'secret': 'ddxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'   # 32 байта
}

# Привязка к интерфейсу (например AmneziaWG)
TELEGRAM_BIND_INTERFACE = "awg0"

# Отключение любой привязки (по умолчанию)
# TELEGRAM_BIND_INTERFACE = None
