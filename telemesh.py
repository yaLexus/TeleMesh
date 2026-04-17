import sys
import os
import asyncio
import logging
import json
from datetime import datetime
from threading import Thread, Lock
import queue
import time
from collections import OrderedDict

from telethon import TelegramClient, events, functions
from telethon.sessions import StringSession
from telethon.tl.types import PeerUser, UpdateMessageReactions, ReactionEmoji
from telethon.tl import types

import meshtastic
from meshtastic.serial_interface import SerialInterface
from meshtastic.tcp_interface import TCPInterface
from pubsub import pub

import re
import requests
from urllib.parse import urlparse
import socket

# --------------------- Версия ---------------------
VERSION = "1.5.009"

# --------------------- Временные переменные ---------------------
API_ID = None
API_HASH = None
PHONE = None
DEST_NODE_ID = None
ADMIN_CHAT_ID = None

# --------------------- Импорт конфигурации ---------------------
try:
    from config import SESSION_NAME
except ImportError:
    SESSION_NAME = "telemesh_session"

try:
    from config import SERIAL_PORT
except ImportError:
    SERIAL_PORT = None

try:
    from config import TCP_HOST
except ImportError:
    TCP_HOST = None

try:
    from config import TCP_PORT
except ImportError:
    TCP_PORT = None

try:
    from config import MAX_MSG_LEN
except ImportError:
    MAX_MSG_LEN = 200

try:
    from config import GOODBYE_MSG
except ImportError:
    GOODBYE_MSG = "⛔️ TeleMesh остановлен"

try:
    from config import MESSAGE_SEND_DELAY
except ImportError:
    MESSAGE_SEND_DELAY = 1000

try:
    from config import ACC_BD_PATH
except ImportError:
    ACC_BD_PATH = "."

try:
    from config import FORWARD_ENABLED
except ImportError:
    FORWARD_ENABLED = True

try:
    from config import SIGNATURE
except ImportError:
    SIGNATURE = """
📡 Отправлено из меш-сети с помощью 📟 **TeleMesh**"""

try:
    from config import DEBUG
except ImportError:
    DEBUG = False

try:
    from config import ACK_TIMEOUT
except ImportError:
    ACK_TIMEOUT = 60

try:
    from config import MAX_RETRIES
except ImportError:
    MAX_RETRIES = 3

try:
    from config import FAIL_MSG
except ImportError:
    FAIL_MSG = ""

try:
    from config import TRANSLIT_ENABLED
except ImportError:
    TRANSLIT_ENABLED = True

try:
    from config import ENVIRONMENT_TELEGRAM_FORWARD
except ImportError:
    ENVIRONMENT_TELEGRAM_FORWARD = False

try:
    from config import ENVIRONMENT_MESH_FORWARD
except ImportError:
    ENVIRONMENT_MESH_FORWARD = False

try:
    from config import REACTIONS_ENABLED
except ImportError:
    REACTIONS_ENABLED = True

try:
    from config import REPLY_TRACKING_ENABLED
except ImportError:
    REPLY_TRACKING_ENABLED = True

try:
    from config import MSG_CACHE_TTL
except ImportError:
    MSG_CACHE_TTL = 86400

try:
    from config import MSG_CACHE_MAX_SIZE
except ImportError:
    MSG_CACHE_MAX_SIZE = 1000

# --------------------- Новые параметры Telegram ---------------------
try:
    from config import TELEGRAM_PROXY
except ImportError:
    TELEGRAM_PROXY = None

try:
    from config import TELEGRAM_BIND_INTERFACE
except ImportError:
    TELEGRAM_BIND_INTERFACE = None

# --------------------- Глобальные переменные ---------------------
last_sender = None
forward_enabled = FORWARD_ENABLED
telegram_queue = queue.Queue()
MY_NODE_ID = None
interface = None

pending_acks = {}
pending_acks_lock = Lock()
ack_event_queue = queue.Queue()

mesh_msg_cache = OrderedDict()
tg_to_mesh_cache = OrderedDict()

contact_book = {}
contact_book_lock = Lock()

blacklist = {}
blacklist_lock = Lock()

EVENT_LOOP = None
NO_WELCOME = False

CONTACT_BOOK_FILE = None
BLACKLIST_FILE = None
STATE_FILE = None

# --------------------- Логирование ---------------------
logging.basicConfig(
    level=logging.DEBUG if DEBUG else logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("TeleMesh")

if DEBUG:
    logger.debug("Режим отладки (DEBUG) ВКЛЮЧЕН.")

if MESSAGE_SEND_DELAY < 1000:
    logger.warning(f"MESSAGE_SEND_DELAY ({MESSAGE_SEND_DELAY}ms) слишком мал. Установлено 1000ms.")
    MESSAGE_SEND_DELAY = 1000

# --------------------- Остальные функции (без изменений) ---------------------
# (load_acc_data, visual_translit, clean_empty_lines, split_message, get_page_title и т.д.)
# ... весь код до async def main() остаётся точно таким же, как у тебя был ...

# --------------------- Telegram ---------------------
client = None

# (все функции handle_new_message, handle_reaction, telegram_worker, ack_monitor_loop — без изменений)

async def main():
    global client, API_ID, API_HASH, PHONE, DEST_NODE_ID, ADMIN_CHAT_ID, EVENT_LOOP, NO_WELCOME, CONTACT_BOOK_FILE, BLACKLIST_FILE, STATE_FILE, forward_enabled

    EVENT_LOOP = asyncio.get_running_loop()

    # ... (весь код до загрузки конфига остаётся без изменений) ...

    try:
        API_ID, API_HASH, PHONE, raw_dest_id, ADMIN_CHAT_ID = load_acc_data(config_file)
        DEST_NODE_ID = normalize_node_id(raw_dest_id)
        logger.info(f"Конфиг загружен. DEST_NODE_ID: !{DEST_NODE_ID}")
        if ADMIN_CHAT_ID:
            logger.info(f"ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
        CONTACT_BOOK_FILE = f"contacts_{DEST_NODE_ID}.json"
        BLACKLIST_FILE = f"blacklist_{DEST_NODE_ID}.json"
        STATE_FILE = os.path.join(ACC_BD_PATH, f"forward_state_{DEST_NODE_ID}.json")
        load_forward_state()
    except Exception as e:
        logger.error(f"Ошибка конфига: {e}")
        sys.exit(1)

    # ====================== TELEGRAM BIND INTERFACE (ТОЛЬКО ДЛЯ TELEGRAM) ======================
    original_socket = None
    if TELEGRAM_BIND_INTERFACE:
        logger.info(f"🔗 Telegram: привязка ТОЛЬКО TelegramClient к интерфейсу {TELEGRAM_BIND_INTERFACE}")
        try:
            original_socket = socket.socket

            def bound_socket(family=socket.AF_INET, type=socket.SOCK_STREAM, proto=0):
                sock = original_socket(family, type, proto)
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE,
                                    TELEGRAM_BIND_INTERFACE.encode('utf-8'))
                    logger.debug(f"Socket TelegramClient успешно привязан к {TELEGRAM_BIND_INTERFACE}")
                except PermissionError:
                    logger.warning(f"⚠️ Не удалось привязать к {TELEGRAM_BIND_INTERFACE} (нужен root или CAP_NET_RAW)")
                except Exception as e:
                    logger.warning(f"Ошибка привязки интерфейса {TELEGRAM_BIND_INTERFACE}: {e}")
                return sock

            socket.socket = bound_socket
        except Exception as e:
            logger.error(f"Не удалось настроить привязку интерфейса: {e}")
    else:
        logger.info("✅ Telegram: прямое подключение (без привязки к интерфейсу)")

    # ====================== СОЗДАНИЕ КЛИЕНТА ======================
    client = TelegramClient(
        SESSION_NAME,
        API_ID,
        API_HASH,
        proxy=TELEGRAM_PROXY
    )

    # Восстанавливаем оригинальный socket.socket, чтобы Meshtastic и requests работали как раньше
    if original_socket is not None:
        socket.socket = original_socket
        logger.debug("Оригинальный socket.socket восстановлен (Meshtastic и requests работают как раньше)")

    # ====================== РЕАКЦИИ ======================
    if REACTIONS_ENABLED:
        @client.on(events.Raw)
        async def raw_event_handler(event):
            if isinstance(event, UpdateMessageReactions):
                await handle_reaction(event)

    await client.start(phone=PHONE)
    me = await client.get_me()
    logger.info(f"Telegram: {me.first_name} ({me.phone})")

    load_contacts()
    load_blacklist()

    asyncio.create_task(meshtastic_connection_manager())
    asyncio.create_task(telegram_worker())
    asyncio.create_task(ack_monitor_loop())

    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        if interface:
            send_to_meshtastic(GOODBYE_MSG, want_ack=False)
    except Exception as e:
        logger.critical(f"Fatal: {e}")
    finally:
        if interface:
            interface.close()
