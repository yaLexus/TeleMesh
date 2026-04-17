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

# --------------------- Версия ---------------------
VERSION = "1.5.009"

# --------------------- Временные переменные ---------------------
API_ID = None
API_HASH = None
PHONE = None
DEST_NODE_ID = None
ADMIN_CHAT_ID = None  # Загружается из файла с API_ID

# --------------------- Импорт конфигурации (Опциональный) ---------------------

# Параметры подключения к Telegram
try:
    from config import SESSION_NAME
except ImportError:
    SESSION_NAME = "telemesh_session"

# Параметры Meshtastic
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

# Общие параметры
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

# Попытка импортировать параметры отладки и таймаутов
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

# Импорт сообщения об ошибке. 
# Если строка не пустая - уведомление включено. Если пустая - выключено.
try:
    from config import FAIL_MSG
except ImportError:
    FAIL_MSG = ""

# Опция включения транслитерации
try:
    from config import TRANSLIT_ENABLED
except ImportError:
    TRANSLIT_ENABLED = True

# Опции телеметрии
try:
    from config import ENVIRONMENT_TELEGRAM_FORWARD
except ImportError:
    ENVIRONMENT_TELEGRAM_FORWARD = False

try:
    from config import ENVIRONMENT_MESH_FORWARD
except ImportError:
    ENVIRONMENT_MESH_FORWARD = False

# --------------------- НОВЫЕ ОПЦИИ КОНФИГУРАЦИИ ---------------------

# Включить поддержку реакций TG → Mesh
try:
    from config import REACTIONS_ENABLED
except ImportError:
    REACTIONS_ENABLED = True

# Включить поддержку reply из Mesh → конкретному автору в TG
try:
    from config import REPLY_TRACKING_ENABLED
except ImportError:
    REPLY_TRACKING_ENABLED = True

# Время хранения кэша сообщений (в секундах), по умолчанию 24 часа
try:
    from config import MSG_CACHE_TTL
except ImportError:
    MSG_CACHE_TTL = 86400

# Максимальный размер кэша сообщений
try:
    from config import MSG_CACHE_MAX_SIZE
except ImportError:
    MSG_CACHE_MAX_SIZE = 1000

# --------------------- ПРОКСИ И ПРИВЯЗКА К ИНТЕРФЕЙСУ ---------------------
try:
    from config import TG_PROXY_TYPE, TG_PROXY_HOST, TG_PROXY_PORT, TG_PROXY_USERNAME, TG_PROXY_PASSWORD
except ImportError:
    TG_PROXY_TYPE = None
    TG_PROXY_HOST = None
    TG_PROXY_PORT = None
    TG_PROXY_USERNAME = None
    TG_PROXY_PASSWORD = None

try:
    from config import TG_INTERFACE
except ImportError:
    TG_INTERFACE = None

# --------------------- Глобальные переменные ---------------------
last_sender = None
forward_enabled = FORWARD_ENABLED
telegram_queue = queue.Queue()
MY_NODE_ID = None
interface = None

# Словарь для отслеживания сообщений, ожидающих обработки
pending_acks = {}
pending_acks_lock = Lock()
ack_event_queue = queue.Queue()

# --------------------- НОВЫЕ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---------------------

# Кэш сопоставления: mesh_packet_id -> {tg_user_id, tg_msg_id, timestamp}
mesh_msg_cache = OrderedDict()
tg_to_mesh_cache = OrderedDict()

# Записная книжка: {slot: {'id': int, 'name': str, 'username': str}}
contact_book = {}
contact_book_lock = Lock()

# Чёрный список: {slot: {'id': int, 'name': str, 'username': str}}
blacklist = {}
blacklist_lock = Lock()

# Ссылка на event loop
EVENT_LOOP = None

# Флаг отключения приветственного сообщения в Mesh (из командной строки)
NO_WELCOME = False

# Пути к файлам
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

# Проверка минимальной задержки отправки
if MESSAGE_SEND_DELAY < 1000:
    logger.warning(f"MESSAGE_SEND_DELAY ({MESSAGE_SEND_DELAY}ms) слишком мал. Установлено минимальное значение 1000ms.")
    MESSAGE_SEND_DELAY = 1000

# --------------------- КАСТОМНЫЙ КЛАСС СОЕДИНЕНИЯ ДЛЯ ПРИВЯЗКИ К ИНТЕРФЕЙСУ ---------------------
import socket
import asyncio
from telethon.network.connection.tcpabridged import ConnectionTcpAbridged

class BindToInterfaceConnection(ConnectionTcpAbridged):
    """Соединение Telethon с привязкой сокета к интерфейсу через SO_BINDTODEVICE."""
    def __init__(self, *args, interface_name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface_name = interface_name

    async def _connect(self, address, timeout=None, ssl=None):
        family = socket.AF_INET
        if ':' in address[0]:
            family = socket.AF_INET6

        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        if self.interface_name:
            try:
                # SO_BINDTODEVICE = 25
                sock.setsockopt(socket.SOL_SOCKET, 25, self.interface_name.encode())
                logger.debug(f"Привязка сокета к интерфейсу {self.interface_name}")
            except Exception as e:
                logger.error(f"Не удалось установить SO_BINDTODEVICE: {e} (требуются права root)")

        loop = asyncio.get_running_loop()
        try:
            await loop.sock_connect(sock, address)
        except Exception:
            sock.close()
            raise

        sock.setblocking(False)
        self._socket = sock

        if ssl:
            try:
                self._socket = await loop.start_tls(self._socket, address, ssl=ssl, server_side=False)
            except Exception:
                self._socket.close()
                raise

        self._connected = True

# --------------------- Функции сохранения/загрузки состояния пересылки ---------------------
def save_forward_state():
    if STATE_FILE is None:
        return
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({'forward_enabled': forward_enabled}, f)
        logger.debug(f"Состояние пересылки сохранено: {forward_enabled}")
    except Exception as e:
        logger.error(f"Ошибка сохранения состояния пересылки: {e}")

def load_forward_state():
    global forward_enabled
    if STATE_FILE is None or not os.path.exists(STATE_FILE):
        return
    try:
        with open(STATE_FILE, 'r') as f:
            data = json.load(f)
            if 'forward_enabled' in data:
                forward_enabled = data['forward_enabled']
                logger.info(f"Загружено состояние пересылки: {'ВКЛЮЧЕНА' if forward_enabled else 'ОТКЛЮЧЕНА'}")
    except Exception as e:
        logger.error(f"Ошибка загрузки состояния пересылки: {e}")

# --------------------- Функции управления кэшем ---------------------
def cache_add_mesh_message(mesh_packet_id, tg_user_id, tg_msg_id):
    if not REPLY_TRACKING_ENABLED:
        return
    while len(mesh_msg_cache) >= MSG_CACHE_MAX_SIZE:
        mesh_msg_cache.popitem(last=False)
    mesh_msg_cache[mesh_packet_id] = {
        'tg_user_id': tg_user_id,
        'tg_msg_id': tg_msg_id,
        'timestamp': time.time()
    }
    while len(tg_to_mesh_cache) >= MSG_CACHE_MAX_SIZE:
        tg_to_mesh_cache.popitem(last=False)
    tg_to_mesh_cache[tg_msg_id] = mesh_packet_id
    logger.debug(f"Кэш: добавлено Mesh[{mesh_packet_id}] → TG[user={tg_user_id}, msg={tg_msg_id}]")

def cache_get_mesh_message(mesh_packet_id):
    if not REPLY_TRACKING_ENABLED:
        return None
    info = mesh_msg_cache.get(mesh_packet_id)
    if info:
        if time.time() - info['timestamp'] > MSG_CACHE_TTL:
            del mesh_msg_cache[mesh_packet_id]
            return None
        return info
    return None

def cache_get_mesh_id_by_tg(tg_msg_id):
    if not REACTIONS_ENABLED:
        return None
    mesh_id = tg_to_mesh_cache.get(tg_msg_id)
    if mesh_id:
        logger.debug(f"Найден mesh_packet_id={mesh_id} для tg_msg_id={tg_msg_id}")
        return mesh_id
    logger.debug(f"tg_msg_id={tg_msg_id} не найден в кэше.")
    return None

def cache_cleanup():
    now = time.time()
    expired_mesh = [k for k, v in mesh_msg_cache.items() if now - v['timestamp'] > MSG_CACHE_TTL]
    for k in expired_mesh:
        del mesh_msg_cache[k]
    expired_tg = [k for k, v in tg_to_mesh_cache.items() if v not in mesh_msg_cache]
    for k in expired_tg:
        del tg_to_mesh_cache[k]
    if expired_mesh or expired_tg:
        logger.debug(f"Кэш очищен: {len(expired_mesh)} mesh записей, {len(expired_tg)} tg записей")

# --------------------- Таблица транслитерации ---------------------
CYR_TO_LAT_VISUAL = str.maketrans({
    'а': 'a', 'А': 'A', 'б': '6', 'Б': '6', 'с': 'c', 'С': 'C',
    'о': 'o', 'О': 'O', 'р': 'p', 'Р': 'P', 'х': 'x', 'Х': 'X',
    'д': 'g', 'Д': 'D', 'е': 'e', 'Е': 'E', 'ё': 'e', 'Ё': 'E',
    'з': '3', 'З': '3', 'у': 'y', 'У': 'Y', 'к': 'k', 'К': 'K',
    'м': 'm', 'М': 'M', 'т': 't', 'Т': 'T', 'и': 'u', 'И': 'U',
    'в': 'B', 'В': 'B', 'н': 'H', 'Н': 'H', 'ь': "ь", 'Ь': "Ь",
})

# --------------------- Таблица маппинга реакций TG → Mesh ---------------------
REACTION_MAPPING = {
    '👍': 0x1F44D, '👎': 0x1F44E, '❤️': 0x2764, '💔': 0x1F494, '😂': 0x1F602,
    '😮': 0x1F62E, '😢': 0x1F622, '😡': 0x1F621, '🔥': 0x1F525, '🎉': 0x1F389,
    '👏': 0x1F44F, '🤔': 0x1F914, '😅': 0x1F605, '🙏': 0x1F64F, '💯': 0x1F4AF,
    '⭐': 0x2B50, '❓': 0x2753, '✅': 0x2705, '❌': 0x274C,
}

def get_emoji_codepoint(emoji_str):
    emoji_clean = emoji_str.replace('\uFE0F', '').replace('\u200D', '')
    if emoji_clean in REACTION_MAPPING:
        return REACTION_MAPPING[emoji_clean]
    if emoji_clean:
        return ord(emoji_clean[0])
    return None

def clean_emoji_for_telegram(emoji_str):
    if not emoji_str:
        return emoji_str
    result = emoji_str.replace('\uFE0F', '').replace('\u200D', '')
    skin_tone_modifiers = ['\U0001F3FB', '\U0001F3FC', '\U0001F3FD', '\U0001F3FE', '\U0001F3FF']
    for modifier in skin_tone_modifiers:
        result = result.replace(modifier, '')
    if result:
        return result[0]
    return emoji_str

# --------------------- Функции для работы с ID нод ---------------------
def normalize_node_id(node_id):
    if node_id is None: return None
    if isinstance(node_id, int):
        return f"{node_id:x}"
    s = str(node_id).strip()
    if s.startswith('!'):
        s = s[1:]
    elif s.lower().startswith('0x'):
        s = s[2:]
    try:
        val = int(s, 16)
        return f"{val:x}"
    except ValueError:
        return s.lower()

def compare_node_ids(id1, id2):
    n1 = normalize_node_id(id1)
    n2 = normalize_node_id(id2)
    if DEBUG: logger.debug(f"Сравнение ID: {id1}->{n1}, {id2}->{n2}")
    return n1 == n2

# --------------------- Нормализация текста для распознавания команд ---------------------
LAT_TO_CYR_MAP = {
    'c': 'с', 'a': 'а', 'o': 'о', 'p': 'р', 'x': 'х', 'y': 'у',
    'e': 'е', 'k': 'к', 'm': 'м', 't': 'т', 'b': 'в', 'h': 'н',
}

def normalize_command_str(text: str) -> str:
    if not text:
        return ""
    result = text.lower()
    if result.startswith('!'):
        result = '!' + result[1:].lstrip()
    if result.startswith('#'):
        result = '#' + result[1:].lstrip()
    for lat, cyr in LAT_TO_CYR_MAP.items():
        result = result.replace(lat, cyr)
    return result

# Команды для записной книжки (префикс !)
COMMAND_START = {normalize_command_str(cmd) for cmd in ['!start', '!старт', '!cтapт']}
COMMAND_STOP = {normalize_command_str(cmd) for cmd in ['!stop', '!стоп', '!cтoп']}
COMMAND_ADD = {normalize_command_str(cmd) for cmd in ['!add', '!добавить', '!дoбaвить']}
COMMAND_LIST = {normalize_command_str(cmd) for cmd in ['!list', '!список', '!cпиcoк']}
COMMAND_DEL = {normalize_command_str(cmd) for cmd in ['!del', '!удалить', '!yдaлить']}

# Команды для чёрного списка (префикс #)
COMMAND_BLACKLIST_ADD = {normalize_command_str(cmd) for cmd in ['#add', '#добавить', '#дoбaвить']}
COMMAND_BLACKLIST_LIST = {normalize_command_str(cmd) for cmd in ['#list', '#список', '#cпиcoк']}
COMMAND_BLACKLIST_DEL = {normalize_command_str(cmd) for cmd in ['#del', '#удалить', '#yдaлить']}

COMMAND_MAP = {
    **{cmd: 'start' for cmd in COMMAND_START},
    **{cmd: 'stop' for cmd in COMMAND_STOP},
    **{cmd: 'add' for cmd in COMMAND_ADD},
    **{cmd: 'list' for cmd in COMMAND_LIST},
    **{cmd: 'del' for cmd in COMMAND_DEL},
    **{cmd: 'blacklist_add' for cmd in COMMAND_BLACKLIST_ADD},
    **{cmd: 'blacklist_list' for cmd in COMMAND_BLACKLIST_LIST},
    **{cmd: 'blacklist_del' for cmd in COMMAND_BLACKLIST_DEL},
}

def parse_mesh_command(text: str):
    if not text:
        return None, None
    norm = normalize_command_str(text)
    parts = norm.split()
    if not parts:
        return None, None
    cmd_str = parts[0]
    arg_str = parts[1] if len(parts) > 1 else None
    if cmd_str in COMMAND_MAP:
        command = COMMAND_MAP[cmd_str]
        arg = None
        if arg_str and arg_str.isdigit():
            arg = int(arg_str)
        return command, arg
    return None, None

# --------------------- Записная книжка ---------------------
def load_contacts():
    global contact_book
    if CONTACT_BOOK_FILE is None:
        return
    file_path = os.path.join(ACC_BD_PATH, CONTACT_BOOK_FILE)
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                contact_book = {int(k): v for k, v in data.items()}
                logger.info(f"Записная книжка загружена из {file_path} ({len(contact_book)} записей)")
        else:
            contact_book = {}
    except Exception as e:
        logger.error(f"Ошибка загрузки записной книжки: {e}")
        contact_book = {}

def save_contacts():
    if CONTACT_BOOK_FILE is None:
        return
    file_path = os.path.join(ACC_BD_PATH, CONTACT_BOOK_FILE)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(contact_book, f, ensure_ascii=False, indent=2)
        logger.debug(f"Записная книжка сохранена в {file_path}")
    except Exception as e:
        logger.error(f"Ошибка сохранения записной книжки: {e}")

def get_next_free_slot(book):
    if not book:
        return 1
    slots = sorted(book.keys())
    for i in range(1, len(slots) + 2):
        if i not in slots:
            return i
    return len(slots) + 1

def add_contact(book, book_lock, tg_user_id, name, username, slot=None):
    with book_lock:
        if slot is None:
            slot = get_next_free_slot(book)
        book[slot] = {'id': tg_user_id, 'name': name, 'username': username}
        if book is contact_book:
            save_contacts()
        else:
            save_blacklist()
        return slot

def delete_contact_by_user_id(book, book_lock, tg_user_id):
    with book_lock:
        for slot, info in list(book.items()):
            if info.get('id') == tg_user_id:
                del book[slot]
                if book is contact_book:
                    save_contacts()
                else:
                    save_blacklist()
                return slot
        return None

def delete_contact_by_slot(book, book_lock, slot):
    with book_lock:
        if slot in book:
            del book[slot]
            if book is contact_book:
                save_contacts()
            else:
                save_blacklist()
            return True
        return False

def get_contact_by_slot(book, slot):
    return book.get(slot)

def format_list(book, title):
    with contact_book_lock if book is contact_book else blacklist_lock:
        if not book:
            return f"{title} пуст."
        lines = [f"{title}:"]
        for slot in sorted(book.keys()):
            info = book[slot]
            name = info.get('name', 'Неизвестно')
            username = info.get('username', '')
            if username:
                lines.append(f"{slot}: {name} (@{username})")
            else:
                lines.append(f"{slot}: {name}")
        return "\n".join(lines)

# --------------------- Чёрный список ---------------------
def load_blacklist():
    global blacklist
    if BLACKLIST_FILE is None:
        return
    file_path = os.path.join(ACC_BD_PATH, BLACKLIST_FILE)
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                blacklist = {int(k): v for k, v in data.items()}
                logger.info(f"Чёрный список загружен из {file_path} ({len(blacklist)} записей)")
        else:
            blacklist = {}
    except Exception as e:
        logger.error(f"Ошибка загрузки чёрного списка: {e}")
        blacklist = {}

def save_blacklist():
    if BLACKLIST_FILE is None:
        return
    file_path = os.path.join(ACC_BD_PATH, BLACKLIST_FILE)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(blacklist, f, ensure_ascii=False, indent=2)
        logger.debug(f"Чёрный список сохранён в {file_path}")
    except Exception as e:
        logger.error(f"Ошибка сохранения чёрного списка: {e}")

def is_user_blacklisted(tg_user_id):
    with blacklist_lock:
        for info in blacklist.values():
            if info.get('id') == tg_user_id:
                return True
        return False

# --------------------- Обработчики команд для чёрного списка ---------------------
async def handle_blacklist_command(command, arg, original_user_id, packet_id):
    try:
        if command == 'blacklist_add':
            if original_user_id is None:
                response = "⚠️ Команда #add должна быть ответом на сообщение из Telegram."
                send_to_meshtastic(response, want_ack=False)
                return
            try:
                user = await client.get_entity(original_user_id)
                name = f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username or str(user.id)
                username = user.username or ""
            except Exception as e:
                logger.error(f"Не удалось получить информацию о пользователе {original_user_id}: {e}")
                response = f"⚠️ Не удалось получить данные пользователя (ID: {original_user_id})."
                send_to_meshtastic(response, want_ack=False)
                return
            slot = add_contact(blacklist, blacklist_lock, original_user_id, name, username, arg)
            response = f"⛔ Пользователь {name} (@{username}) добавлен в чёрный список под номером {slot}."
            send_to_meshtastic(response, want_ack=False)
        elif command == 'blacklist_del':
            if arg is not None:
                if delete_contact_by_slot(blacklist, blacklist_lock, arg):
                    response = f"🗑 Запись №{arg} удалена из чёрного списка."
                else:
                    response = f"⚠️ Запись №{arg} не найдена."
                send_to_meshtastic(response, want_ack=False)
                return
            if original_user_id is None:
                response = "⚠️ Команда #del должна быть ответом на сообщение из Telegram или содержать номер записи."
                send_to_meshtastic(response, want_ack=False)
                return
            slot = delete_contact_by_user_id(blacklist, blacklist_lock, original_user_id)
            if slot is not None:
                response = f"🗑 Пользователь удалён из чёрного списка (слот {slot})."
            else:
                response = "⚠️ Пользователь не найден в чёрном списке."
            send_to_meshtastic(response, want_ack=False)
        elif command == 'blacklist_list':
            response = format_list(blacklist, "⛔ Чёрный список")
            send_to_meshtastic(response, want_ack=False)
    except Exception as e:
        logger.error(f"Ошибка обработки команды {command}: {e}", exc_info=True)
        send_to_meshtastic(f"⚠️ Ошибка при обработке команды: {e}", want_ack=False)

# --------------------- Обработчик команд записной книжки ---------------------
async def handle_contact_command(command, arg, original_user_id, packet_id):
    try:
        if command == 'add':
            if original_user_id is None:
                response = "⚠️ Команда !add должна быть ответом на сообщение из Telegram."
                send_to_meshtastic(response, want_ack=False)
                return
            try:
                user = await client.get_entity(original_user_id)
                name = f"{user.first_name or ''} {user.last_name or ''}".strip() or user.username or str(user.id)
                username = user.username or ""
            except Exception as e:
                logger.error(f"Не удалось получить информацию о пользователе {original_user_id}: {e}")
                response = f"⚠️ Не удалось получить данные пользователя (ID: {original_user_id})."
                send_to_meshtastic(response, want_ack=False)
                return
            slot = add_contact(contact_book, contact_book_lock, original_user_id, name, username, arg)
            response = f"✅ Пользователь {name} (@{username}) добавлен в записную книжку под номером {slot}."
            send_to_meshtastic(response, want_ack=False)
        elif command == 'del':
            if arg is not None:
                if delete_contact_by_slot(contact_book, contact_book_lock, arg):
                    response = f"🗑 Запись №{arg} удалена из записной книжки."
                else:
                    response = f"⚠️ Запись №{arg} не найдена."
                send_to_meshtastic(response, want_ack=False)
                return
            if original_user_id is None:
                response = "⚠️ Команда !del должна быть ответом на сообщение из Telegram или содержать номер записи."
                send_to_meshtastic(response, want_ack=False)
                return
            slot = delete_contact_by_user_id(contact_book, contact_book_lock, original_user_id)
            if slot is not None:
                response = f"🗑 Пользователь удалён из записной книжки (слот {slot})."
            else:
                response = "⚠️ Пользователь не найден в записной книжке."
            send_to_meshtastic(response, want_ack=False)
        elif command == 'list':
            response = format_list(contact_book, "📒 Записная книжка")
            send_to_meshtastic(response, want_ack=False)
    except Exception as e:
        logger.error(f"Ошибка обработки команды {command}: {e}", exc_info=True)
        send_to_meshtastic(f"⚠️ Ошибка при обработке команды: {e}", want_ack=False)

# --------------------- Отправка в Telegram по слоту/юзернейму ---------------------
async def send_to_contact_by_slot(slot: int, message: str, mesh_packet_id: int = None):
    contact = get_contact_by_slot(contact_book, slot)
    if not contact:
        response = f"⚠️ Контакт с номером {slot} не найден в записной книжке."
        send_to_meshtastic(response, want_ack=False)
        return
    tg_user_id = contact['id']
    full_message = format_message_with_signature(message)
    telegram_queue.put({
        'user_id': tg_user_id,
        'message': full_message,
        'mesh_packet_id': mesh_packet_id
    })
    logger.info(f"→ Сообщение добавлено в очередь для контакта {slot} (TG ID: {tg_user_id}, mesh_packet_id={mesh_packet_id})")
    name = contact.get('name', 'пользователь')
    username = contact.get('username', '')
    if username:
        response = f"✅ Сообщение отправлено пользователю {name} (@{username})"
    else:
        response = f"✅ Сообщение отправлено пользователю {name}"
    send_to_meshtastic(response, want_ack=False)

async def send_to_contact_by_username(username: str, message: str, mesh_packet_id: int = None):
    target_user_id = None
    target_name = None
    with contact_book_lock:
        for slot, info in contact_book.items():
            if info.get('username', '').lower() == username.lower():
                target_user_id = info['id']
                target_name = info.get('name', 'пользователь')
                break
    if target_user_id is None:
        try:
            entity = await client.get_entity(f"@{username}")
            target_user_id = entity.id
            target_name = f"{entity.first_name or ''} {entity.last_name or ''}".strip() or entity.username
        except Exception as e:
            logger.error(f"Не удалось найти пользователя @{username}: {e}")
            response = f"⚠️ Пользователь @{username} не найден в Telegram."
            send_to_meshtastic(response, want_ack=False)
            return
    full_message = format_message_with_signature(message)
    telegram_queue.put({
        'user_id': target_user_id,
        'message': full_message,
        'mesh_packet_id': mesh_packet_id
    })
    logger.info(f"→ Сообщение добавлено в очередь для пользователя @{username} (TG ID: {target_user_id}, mesh_packet_id={mesh_packet_id})")
    response = f"✅ Сообщение отправлено пользователю @{username}"
    send_to_meshtastic(response, want_ack=False)

# --------------------- Загрузка параметров ---------------------
def load_acc_data(config_file):
    full_path = os.path.join(ACC_BD_PATH, config_file)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Файл {full_path} не найден")
    with open(full_path, 'r', encoding='utf-8') as f:
        content = f.read()
    namespace = {}
    exec(content, namespace)
    api_id = namespace.get('API_ID')
    api_hash = namespace.get('API_HASH')
    phone = namespace.get('PHONE')
    dest_node_id = namespace.get('DEST_NODE_ID')
    admin_chat_id = namespace.get('ADMIN_CHAT_ID')
    if not all([api_id, api_hash, phone, dest_node_id]):
        raise ValueError("Отсутствуют обязательные параметры в файле конфигурации")
    return api_id, api_hash, phone, dest_node_id, admin_chat_id

# --------------------- Вспомогательные функции ---------------------
def visual_translit(text: str) -> str:
    return text.translate(CYR_TO_LAT_VISUAL)

def clean_empty_lines(text: str) -> str:
    return '\n'.join([line for line in text.split('\n') if line.strip()])

def split_message(text: str, max_bytes: int = 200) -> list[str]:
    if not text: return []
    parts, current, current_bytes = [], "", 0
    for char in text:
        c_bytes = len(char.encode('utf-8'))
        if current_bytes + c_bytes <= max_bytes - 5:
            current += char
            current_bytes += c_bytes
        else:
            parts.append(current)
            current = char
            current_bytes = c_bytes
    if current:
        parts.append(current)
    if len(parts) > 1:
        numbered = []
        for i, p in enumerate(parts, 1):
            prefix = f"[{i}/{len(parts)}]"
            numbered.append(prefix + p if len((prefix + p).encode('utf-8')) <= max_bytes else p)
        return numbered
    return parts

def get_page_title(url: str) -> str:
    try:
        resp = requests.get(url, timeout=6, headers={'User-Agent': 'TeleMeshBot/1.0'})
        match = re.search(r'<title[^>]*>([^<]*)</title>', resp.text, re.IGNORECASE)
        if match:
            title = match.group(1).strip()
            return re.sub(r'\s*-\s*YouTube\s*$', '', title).strip() or "Без названия"
        return "Страница без заголовка"
    except:
        return "Ссылка"

def format_message_with_signature(message: str) -> str:
    return f"{message}\n{SIGNATURE}" if SIGNATURE else message

# --------------------- Meshtastic ---------------------
def on_meshtastic_connect(interface, topic=None):
    global MY_NODE_ID
    try:
        my_info = interface.getMyNodeInfo()
        raw_id = my_info.get('num')
        if raw_id:
            MY_NODE_ID = normalize_node_id(raw_id)
        else:
            logger.warning("Не удалось получить ID текущей ноды из my_info!")
        long_name = "Unknown"
        short_name = "???"
        user_info = my_info.get('user')
        if user_info:
            long_name = user_info.get('longName', 'Unknown')
            short_name = user_info.get('shortName', '???')
        if DEBUG:
            logger.info("Meshtastic подключён → %s", my_info)
        else:
            logger.info(f"Meshtastic подключён. ID: !{MY_NODE_ID}, Name: {long_name} ({short_name})")
        # Формируем приветственное сообщение в Mesh
        msg_lines = [f"📟 TeleMesh v{VERSION}", f"Node: {long_name} ({short_name})"]
        if not NO_WELCOME:
            send_to_meshtastic("\n".join(msg_lines), want_ack=False)
        # Отправляем в Telegram Admin
        if ADMIN_CHAT_ID:
            status_lines = [
                f"📟 TeleMesh v{VERSION} запущен",
                f"Node: {long_name} ({short_name})",
                f"Target ID: !{DEST_NODE_ID}",
            ]
            tg_status = "\n".join(status_lines)
            telegram_queue.put({'user_id': ADMIN_CHAT_ID, 'message': tg_status})
    except Exception as e:
        logger.error(f"Ошибка при получении info ноды: {e}")

def on_meshtastic_receive(packet, interface, topic=None):
    global forward_enabled, last_sender
    try:
        if DEBUG:
            try:
                packet_json = json.dumps(packet, indent=2, default=str, ensure_ascii=False)
                logger.debug(f"--- ВХОДЯЩИЙ ПАКЕТ ---\n{packet_json}\n-------------------")
            except Exception:
                pass
        decoded = packet.get('decoded')
        if not decoded:
            return
        portnum = decoded.get('portnum')
        # ACK
        if str(portnum) in ('3', 'ROUTING_APP'):
            request_id = decoded.get('requestId')
            ack_sender_id = packet.get('fromId') or packet.get('from')
            if request_id:
                if compare_node_ids(ack_sender_id, DEST_NODE_ID):
                    ack_event_queue.put({'type': 'ack_received', 'id': request_id})
                    if DEBUG:
                        logger.debug(f"✓ Получен ACK от целевой ноды [!{normalize_node_id(ack_sender_id)}] для пакета {request_id}")
                else:
                    if DEBUG:
                        logger.debug(f"Получен ACK для {request_id} от [!{normalize_node_id(ack_sender_id)}], но цель [!{DEST_NODE_ID}]. Игнорируем.")
            return
        # Телеметрия
        if ENVIRONMENT_TELEGRAM_FORWARD or ENVIRONMENT_MESH_FORWARD:
            if str(portnum) in ('5', 'TELEMETRY_APP'):
                telemetry = decoded.get('telemetry')
                from_id = packet.get('fromId') or packet.get('from')
                from_id_norm = normalize_node_id(from_id)
                if telemetry and 'environment_metrics' in telemetry:
                    env_data = telemetry['environment_metrics']
                    msg_parts = [f"📡 *Телеметрия от* `!{from_id_norm}`"]
                    if 'temperature' in env_data:
                        msg_parts.append(f"🌡 Температура: *{env_data['temperature']:.1f}°C*")
                    if 'relative_humidity' in env_data:
                        msg_parts.append(f"💧 Влажность: *{env_data['relative_humidity']:.1f}%*")
                    if 'barometric_pressure' in env_data:
                        msg_parts.append(f"📊 Давление: *{env_data['barometric_pressure']:.1f} hPa*")
                    if 'voltage' in env_data:
                        msg_parts.append(f"🔋 Напряжение: *{env_data['voltage']:.2f}V*")
                    if len(msg_parts) > 1:
                        logger.info(f"← Телеметрия (Environment) от !{from_id_norm}")
                        env_msg = "\n".join(msg_parts)
                        if ENVIRONMENT_TELEGRAM_FORWARD and ADMIN_CHAT_ID:
                            telegram_queue.put({'user_id': ADMIN_CHAT_ID, 'message': env_msg})
                            logger.info(f"→ Телеметрия отправлена в Telegram.")
                        if ENVIRONMENT_MESH_FORWARD:
                            send_to_meshtastic(env_msg, want_ack=False)
                            logger.info(f"→ Телеметрия отправлена в Mesh на !{DEST_NODE_ID}.")
                elif telemetry and 'device_metrics' in telemetry:
                    if DEBUG:
                        logger.debug("Получена device_metrics (батарея), игнорируем.")
                else:
                    if DEBUG:
                        logger.debug(f"Пакет телеметрии без environment/device metrics.")
        # Текстовые сообщения
        if str(portnum) not in ('1', 'TEXT_MESSAGE_APP'):
            return
        to_id = packet.get('to') or packet.get('toId')
        if MY_NODE_ID is not None:
            if not compare_node_ids(to_id, MY_NODE_ID):
                logger.debug(f"Сообщение не нам (To: !{normalize_node_id(to_id)}). Игнорируем.")
                return
        payload = decoded.get('payload')
        text = None
        if isinstance(payload, bytes):
            text = payload.decode('utf-8', errors='ignore')
        elif isinstance(payload, str):
            text = payload
        if text:
            text = text.strip()
        from_id = packet.get('fromId') or packet.get('from')
        packet_id = packet.get('id')
        reply_id = decoded.get('replyId')
        emoji_flag = decoded.get('emoji')
        # Реакции
        if emoji_flag and payload:
            try:
                if isinstance(payload, bytes):
                    emoji_char = payload.decode('utf-8', errors='ignore')
                else:
                    emoji_char = str(payload)
                if emoji_char:
                    if DEBUG:
                        logger.debug(f"← Реакция {emoji_char} от !{normalize_node_id(from_id)}")
                    if reply_id and REPLY_TRACKING_ENABLED:
                        original_msg_info = cache_get_mesh_message(reply_id)
                        if original_msg_info:
                            telegram_queue.put({
                                'user_id': original_msg_info['tg_user_id'],
                                'msg_id': original_msg_info['tg_msg_id'],
                                'reaction_emoji': emoji_char
                            })
                    return
            except Exception as e:
                logger.warning(f"Не удалось декодировать emoji: {e}")
        if not text:
            return
        if not compare_node_ids(from_id, DEST_NODE_ID):
            logger.debug(f"Сообщение от !{normalize_node_id(from_id)}, но цель !{DEST_NODE_ID}. Игнорируем.")
            return
        # Команды
        command, arg = parse_mesh_command(text)
        if command in ('start', 'stop'):
            if command == 'stop':
                logger.info(f"← Команда от !{normalize_node_id(from_id)}: {text}")
                if forward_enabled:
                    forward_enabled = False
                    save_forward_state()
                    with pending_acks_lock:
                        pending_acks.clear()
                    send_to_meshtastic("⚠️ Пересылка ОСТАНОВЛЕНА", want_ack=False)
                else:
                    send_to_meshtastic("ℹ️ Пересылка уже остановлена", want_ack=False)
            elif command == 'start':
                logger.info(f"← Команда от !{normalize_node_id(from_id)}: {text}")
                if not forward_enabled:
                    forward_enabled = True
                    save_forward_state()
                    send_to_meshtastic("✅ Пересылка ВОЗОБНОВЛЕНА", want_ack=False)
                else:
                    send_to_meshtastic("ℹ️ Пересылка уже активна", want_ack=False)
            return
        elif command in ('add', 'list', 'del'):
            original_user_id = None
            if reply_id:
                orig = cache_get_mesh_message(reply_id)
                if orig:
                    original_user_id = orig['tg_user_id']
            if EVENT_LOOP:
                asyncio.run_coroutine_threadsafe(
                    handle_contact_command(command, arg, original_user_id, packet_id),
                    EVENT_LOOP
                )
            return
        elif command in ('blacklist_add', 'blacklist_list', 'blacklist_del'):
            original_user_id = None
            if reply_id:
                orig = cache_get_mesh_message(reply_id)
                if orig:
                    original_user_id = orig['tg_user_id']
            if EVENT_LOOP:
                asyncio.run_coroutine_threadsafe(
                    handle_blacklist_command(command, arg, original_user_id, packet_id),
                    EVENT_LOOP
                )
            return
        # Отправка по слоту
        if text.startswith('!'):
            after_excl = text[1:].lstrip()
            match = re.match(r'^(\d+)\s+(.*)', after_excl)
            if match:
                slot = int(match.group(1))
                message = match.group(2).strip()
                if not message:
                    send_to_meshtastic("⚠️ Сообщение после номера не может быть пустым.", want_ack=False)
                    return
                if EVENT_LOOP:
                    asyncio.run_coroutine_threadsafe(
                        send_to_contact_by_slot(slot, message, packet_id),
                        EVENT_LOOP
                    )
                return
        # Отправка по @username
        if text.startswith('@'):
            match = re.match(r'^@([a-zA-Z0-9_]+)\s+(.*)', text)
            if match:
                username = match.group(1)
                message = match.group(2).strip()
                if not message:
                    send_to_meshtastic("⚠️ Сообщение после @username не может быть пустым.", want_ack=False)
                    return
                if EVENT_LOOP:
                    asyncio.run_coroutine_threadsafe(
                        send_to_contact_by_username(username, message, packet_id),
                        EVENT_LOOP
                    )
                return
        # Если пересылка отключена - выходим
        if not forward_enabled:
            return
        logger.info(f"← Meshtastic от !{normalize_node_id(from_id)}: {text}")
        if reply_id and REPLY_TRACKING_ENABLED:
            original_msg_info = cache_get_mesh_message(reply_id)
            if original_msg_info:
                telegram_queue.put({
                    'user_id': original_msg_info['tg_user_id'],
                    'message': format_message_with_signature(text),
                    'msg_id': original_msg_info['tg_msg_id'],
                    'mesh_packet_id': packet_id
                })
                logger.info(f"→ Ответ отправлен в TG пользователю {original_msg_info['tg_user_id']} как reply на msg {original_msg_info['tg_msg_id']}")
            else:
                if last_sender is not None:
                    telegram_queue.put({
                        'user_id': last_sender,
                        'message': format_message_with_signature(text),
                        'mesh_packet_id': packet_id
                    })
                    logger.info(f"→ Сообщение добавлено в очередь для Telegram {last_sender}")
                else:
                    send_to_meshtastic("⚠️ Нет активного чата в Telegram", want_ack=False)
        else:
            if last_sender is not None:
                telegram_queue.put({
                    'user_id': last_sender,
                    'message': format_message_with_signature(text),
                    'mesh_packet_id': packet_id
                })
                logger.info(f"→ Сообщение добавлено в очередь для Telegram {last_sender}")
            else:
                send_to_meshtastic("⚠️ Нет активного чата в Telegram", want_ack=False)
    except Exception as e:
        logger.error(f"Ошибка обработки Meshtastic: {e}", exc_info=True)

def send_to_meshtastic(text: str, want_ack=True, user_id=None, msg_id=None, reply_id=None):
    global interface
    if not interface:
        logger.warning("Попытка отправки без активного подключения.")
        return None
    text = clean_empty_lines(text)
    if TRANSLIT_ENABLED:
        text = visual_translit(text)
    try:
        dest_id_int = int(DEST_NODE_ID, 16)
        result = interface.sendText(
            text=text,
            destinationId=dest_id_int,
            wantAck=want_ack,
            channelIndex=0,
            replyId=reply_id
        )
        packet_id = None
        if result:
            if hasattr(result, 'id'):
                packet_id = result.id
            elif isinstance(result, dict):
                packet_id = result.get("id")
        if packet_id is not None:
            try:
                packet_id = int(packet_id)
            except ValueError:
                pass
        log_text = text[:30] + "..." if len(text) > 30 else text
        logger.info(f"→ Meshtastic TX (ID: {packet_id}, To: !{DEST_NODE_ID}, ACK: {want_ack}, ReplyTo: {reply_id}): {log_text}")
        if packet_id is not None and user_id is not None and msg_id is not None:
            cache_add_mesh_message(packet_id, user_id, msg_id)
        return packet_id
    except Exception as e:
        logger.error(f"Ошибка TX (возможно потеря связи): {e}")
        try:
            if interface:
                interface.close()
        except:
            pass
        interface = None
        return None

def send_reaction_to_meshtastic(mesh_packet_id: int, emoji_codepoint: int, emoji_char: str = None):
    global interface
    if not interface:
        logger.warning("Попытка отправки реакции без активного подключения.")
        return False
    try:
        dest_id_int = int(DEST_NODE_ID, 16)
        if emoji_char:
            emoji_bytes = emoji_char.encode('utf-8')
        else:
            emoji_char = chr(emoji_codepoint)
            emoji_bytes = emoji_char.encode('utf-8')
        from meshtastic.protobuf import portnums_pb2
        if DEBUG:
            logger.debug(f"Отправка реакции: emoji={emoji_char}, bytes={emoji_bytes}, reply_to={mesh_packet_id}, to={dest_id_int}")
        try:
            result = interface.sendData(
                data=emoji_bytes,
                destinationId=dest_id_int,
                portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
                wantAck=False,
                channelIndex=0,
                emojiIndex=1,
                replyId=mesh_packet_id
            )
            if result:
                logger.info(f"→ Реакция отправлена в Mesh (emoji={emoji_char}, reply_to={mesh_packet_id}, to=!{DEST_NODE_ID})")
                return True
            else:
                logger.warning("sendData вернул None")
        except TypeError as te:
            logger.debug(f"sendData не поддерживает нужные параметры: {te}, пробуем _sendPacket...")
            from meshtastic.protobuf import mesh_pb2
            if hasattr(interface, '_sendPacket'):
                try:
                    mesh_packet = mesh_pb2.MeshPacket()
                    mesh_packet.to = dest_id_int
                    mesh_packet.channel = 0
                    mesh_packet.want_ack = False
                    mesh_packet.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
                    mesh_packet.decoded.payload = emoji_bytes
                    mesh_packet.decoded.reply_id = mesh_packet_id
                    mesh_packet.decoded.emoji = 1
                    interface._sendPacket(mesh_packet)
                    logger.info(f"→ Реакция отправлена в Mesh через _sendPacket (emoji={emoji_char}, reply_to={mesh_packet_id})")
                    return True
                except Exception as e:
                    logger.warning(f"_sendPacket не сработал: {e}")
        logger.warning("Не удалось отправить реакцию в Mesh")
        return False
    except Exception as e:
        logger.error(f"Ошибка отправки реакции: {e}")
        return False

def connect_meshtastic():
    global interface
    try:
        if TCP_HOST and TCP_PORT:
            logger.info("Подключение к Meshtastic по TCP: %s", f"{TCP_HOST}:{TCP_PORT}")
            interface = TCPInterface(TCP_HOST, int(TCP_PORT))
        elif SERIAL_PORT:
            logger.info("Подключение к Meshtastic через Serial порт: %s", SERIAL_PORT)
            interface = SerialInterface(devPath=SERIAL_PORT)
        else:
            logger.error("Не заданы параметры подключения. Укажите SERIAL_PORT или TCP_HOST+TCP_PORT в config.py")
            return False
        pub.subscribe(on_meshtastic_connect, "meshtastic.connection.established")
        pub.subscribe(on_meshtastic_receive, "meshtastic.receive")
        logger.info("Meshtastic интерфейс готов")
        return True
    except Exception as e:
        logger.error("Ошибка подключения Meshtastic: %s", e)
        interface = None
        return False

async def meshtastic_connection_manager():
    global interface
    logger.info("Менеджер подключения Meshtastic запущен.")
    cleanup_counter = 0
    while True:
        if interface is None:
            logger.info("Попытка подключения к устройству...")
            if connect_meshtastic():
                logger.info("Подключение успешно установлено.")
            else:
                logger.warning("Не удалось подключиться. Повторная попытка через 5 сек...")
                await asyncio.sleep(5)
        else:
            cleanup_counter += 1
            if cleanup_counter >= 60:
                cache_cleanup()
                cleanup_counter = 0
            await asyncio.sleep(1)

# --------------------- Telegram ---------------------
client = None

async def handle_new_message(event):
    global last_sender, forward_enabled
    if not isinstance(event.peer_id, PeerUser):
        return
    sender = await event.get_sender()
    if sender.bot or sender.id == (await client.get_me()).id:
        return
    last_sender = sender.id
    # Проверяем чёрный список
    if is_user_blacklisted(sender.id):
        logger.info(f"Сообщение от заблокированного пользователя {sender.id} игнорировано.")
        return
    if not forward_enabled:
        return
    text = event.message.message.strip() if event.message.message else ""
    prefix = f"{sender.first_name or ''} {sender.last_name or ''}".strip() + ": "
    text = clean_empty_lines(text)
    url_match = re.search(r'(https?://[^\s]+)', text) if text else None
    processed_text = text
    if url_match:
        url = url_match.group(0)
        title = get_page_title(url)
        parsed = urlparse(url)
        is_yt = parsed.hostname in ('www.youtube.com', 'youtube.com', 'youtu.be')
        processed_text = text.replace(url, f"▶️{title}" if is_yt else f"🔗{title}", 1)
        if len(processed_text) > MAX_MSG_LEN:
            processed_text = processed_text[:MAX_MSG_LEN-5] + "..."
    if not url_match:
        media = ""
        if event.message.photo:
            media = "[PIC]"
        elif event.message.gif:
            media = "[GIF]"
        elif event.message.video:
            media = "[VIDEO]"
        elif event.message.audio:
            media = "[AUDIO]"
        elif event.message.voice:
            media = "[VOICE]"
        elif event.message.document:
            doc = event.message.document
            if hasattr(doc, 'mime_type') and doc.mime_type:
                media = "[" + doc.mime_type.split('/')[0].upper() + "]"
            else:
                media = "[DOC]"
        if media:
            processed_text = media + (" " + processed_text if processed_text else "")
    if not processed_text:
        return
    full_text = visual_translit(prefix + processed_text)
    parts = split_message(full_text, MAX_MSG_LEN)
    reply_to_id = event.message.id
    for i, part in enumerate(parts, 1):
        packet_id = send_to_meshtastic(part, want_ack=True, user_id=sender.id, msg_id=reply_to_id)
        if packet_id is not None:
            with pending_acks_lock:
                pending_acks[packet_id] = {
                    'text': part,
                    'user_id': sender.id,
                    'msg_id': reply_to_id,
                    'retries': 0,
                    'stage': 'wait_ack',
                    'next_action_time': time.time() + ACK_TIMEOUT
                }
            logger.debug(f"Пакет {packet_id} добавлен в очередь (Wait ACK).")
        else:
            logger.warning(f"Не удалось получить ID для сообщения '{part[:30]}...'")
        if i < len(parts):
            await asyncio.sleep(MESSAGE_SEND_DELAY / 1000.0)
    if ADMIN_CHAT_ID:
        await client.send_message(ADMIN_CHAT_ID, f"→ Mesh: {full_text}")

async def handle_reaction(event):
    if not REACTIONS_ENABLED:
        return
    try:
        message_id = event.msg_id
        peer_id = getattr(event, 'peer', None)
        reactions_obj = getattr(event, 'reactions', None)
        if DEBUG:
            logger.debug(f"← TG Reaction: msg_id={message_id}, peer={peer_id}")
        if peer_id and not isinstance(peer_id, PeerUser):
            return
        mesh_packet_id = cache_get_mesh_id_by_tg(message_id)
        if mesh_packet_id is None:
            return
        reactions_list = getattr(reactions_obj, 'results', None) if reactions_obj else None
        if not reactions_list:
            return
        for reaction_count in reactions_list:
            try:
                reaction_obj = getattr(reaction_count, 'reaction', None)
                if not reaction_obj:
                    continue
                emoji = None
                if isinstance(reaction_obj, ReactionEmoji):
                    emoji = reaction_obj.emoticon
                elif hasattr(reaction_obj, 'emoticon'):
                    emoji = reaction_obj.emoticon
                elif isinstance(reaction_obj, str):
                    emoji = reaction_obj
                if emoji:
                    clean_emoji_mesh = clean_emoji_for_telegram(emoji)
                    emoji_codepoint = get_emoji_codepoint(clean_emoji_mesh)
                    if emoji_codepoint:
                        logger.info(f"← Реакция {emoji} в TG на msg {message_id}")
                        success = send_reaction_to_meshtastic(mesh_packet_id, emoji_codepoint, clean_emoji_mesh)
                        if success:
                            logger.info(f"→ Реакция {clean_emoji_mesh} отправлена в Mesh на пакет {mesh_packet_id}")
            except Exception as re:
                logger.debug(f"Ошибка обработки отдельной реакции: {re}")
    except Exception as e:
        logger.error(f"Ошибка обработки реакции: {e}", exc_info=True)

async def telegram_worker():
    while True:
        try:
            item = telegram_queue.get(timeout=0.1)
            if item.get('reaction_emoji'):
                try:
                    emoji = item['reaction_emoji']
                    user_id = item['user_id']
                    msg_id = item['msg_id']
                    clean_emoji = clean_emoji_for_telegram(emoji)
                    logger.debug(f"Отправка реакции: оригинал='{emoji}', очищенный='{clean_emoji}'")
                    peer = await client.get_input_entity(user_id)
                    from telethon.tl.types import ReactionEmoji as TLReactionEmoji
                    await client(functions.messages.SendReactionRequest(
                        peer=peer,
                        msg_id=msg_id,
                        reaction=[TLReactionEmoji(emoticon=clean_emoji)],
                        big=False
                    ))
                    logger.info(f"✓ Реакция {clean_emoji} отправлена в Telegram пользователю {user_id}")
                except Exception as re:
                    logger.error(f"Ошибка отправки реакции в TG: {re}")
                    try:
                        fallback_text = f"💬 Реакция: {item['reaction_emoji']}"
                        fallback_text = format_message_with_signature(fallback_text)
                        await client.send_message(
                            item['user_id'],
                            fallback_text,
                            reply_to=item.get('msg_id')
                        )
                        logger.info(f"✓ Реакция отправлена текстом в Telegram {item['user_id']}")
                    except Exception as e2:
                        logger.error(f"Ошибка отправки реакции текстом: {e2}")
            else:
                msg_id = None
                if 'msg_id' in item:
                    sent_msg = await client.send_message(
                        item['user_id'],
                        item['message'],
                        parse_mode='markdown',
                        reply_to=item['msg_id']
                    )
                    msg_id = sent_msg.id
                    logger.info(f"✓ Отправлено в Telegram {item['user_id']} (reply to {item['msg_id']})")
                else:
                    sent_msg = await client.send_message(
                        item['user_id'],
                        item['message'],
                        parse_mode='markdown'
                    )
                    msg_id = sent_msg.id
                    logger.info(f"✓ Отправлено в Telegram {item['user_id']}")
                mesh_packet_id = item.get('mesh_packet_id')
                if mesh_packet_id is not None and msg_id is not None:
                    tg_to_mesh_cache[msg_id] = mesh_packet_id
                    if mesh_packet_id not in mesh_msg_cache:
                        mesh_msg_cache[mesh_packet_id] = {
                            'tg_user_id': item['user_id'],
                            'tg_msg_id': msg_id,
                            'timestamp': time.time()
                        }
                    logger.debug(f"Кэш обновлён: TG[{msg_id}] → Mesh[{mesh_packet_id}]")
            telegram_queue.task_done()
        except queue.Empty:
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Ошибка отправки TG: {e}")
            await asyncio.sleep(1)

async def ack_monitor_loop():
    logger.info("Монитор ACK запущен.")
    while True:
        try:
            try:
                while True:
                    event = ack_event_queue.get_nowait()
                    if event['type'] == 'ack_received':
                        pid = event['id']
                        with pending_acks_lock:
                            if pid in pending_acks:
                                msg_info = pending_acks[pid]
                                logger.info(f"✓ Доставка подтверждена (ACK) для ID {pid}: \"{msg_info['text'][:30]}...\"")
                                del pending_acks[pid]
            except queue.Empty:
                pass
            now = time.time()
            with pending_acks_lock:
                for pid in list(pending_acks.keys()):
                    msg_info = pending_acks[pid]
                    if now < msg_info['next_action_time']:
                        continue
                    if msg_info['stage'] == 'wait_ack':
                        msg_info['retries'] += 1
                        if msg_info['retries'] > MAX_RETRIES:
                            logger.error(f"❌ Не удалось доставить сообщение после {MAX_RETRIES} попыток: {msg_info['text']}")
                            if FAIL_MSG:
                                telegram_queue.put({
                                    'user_id': msg_info['user_id'],
                                    'message': FAIL_MSG,
                                    'msg_id': msg_info['msg_id']
                                })
                            del pending_acks[pid]
                            continue
                        delay = msg_info['retries'] * ACK_TIMEOUT
                        msg_info['stage'] = 'wait_retry'
                        msg_info['next_action_time'] = now + delay
                        logger.warning(f"Таймаут ACK для {pid}. Попытка {msg_info['retries']}/{MAX_RETRIES}. Повтор через {delay} сек.")
                    elif msg_info['stage'] == 'wait_retry':
                        if not forward_enabled:
                            logger.info(f"Пересылка остановлена, отменяем повторную отправку для ID {pid}")
                            del pending_acks[pid]
                            continue
                        new_pid = send_to_meshtastic(
                            msg_info['text'],
                            want_ack=True,
                            user_id=msg_info['user_id'],
                            msg_id=msg_info['msg_id']
                        )
                        del pending_acks[pid]
                        if new_pid is not None:
                            pending_acks[new_pid] = {
                                'text': msg_info['text'],
                                'user_id': msg_info['user_id'],
                                'msg_id': msg_info['msg_id'],
                                'retries': msg_info['retries'],
                                'stage': 'wait_ack',
                                'next_action_time': now + ACK_TIMEOUT
                            }
                            logger.info(f"Повторная отправка выполнена. Новый ID {new_pid}. Ждем ACK {ACK_TIMEOUT} сек.")
                        else:
                            logger.error("Ошибка повторной отправки (send returned None).")
                            pending_acks[pid] = msg_info
                            pending_acks[pid]['stage'] = 'wait_retry'
                            pending_acks[pid]['next_action_time'] = now + ACK_TIMEOUT
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Ошибка в мониторе ACK: {e}", exc_info=True)
            await asyncio.sleep(5)

async def main():
    global client, API_ID, API_HASH, PHONE, DEST_NODE_ID, ADMIN_CHAT_ID, EVENT_LOOP, NO_WELCOME, CONTACT_BOOK_FILE, BLACKLIST_FILE, STATE_FILE, forward_enabled
    EVENT_LOOP = asyncio.get_running_loop()
    args = sys.argv[1:]
    config_file = None
    for arg in args:
        if arg == '--no-welcome':
            NO_WELCOME = True
        elif not arg.startswith('--'):
            config_file = arg
    if not config_file:
        print(f"Использование: python {sys.argv[0]} <файл_параметров> [--no-welcome]")
        sys.exit(1)
    if not (ENVIRONMENT_TELEGRAM_FORWARD or ENVIRONMENT_MESH_FORWARD):
        logger.info("Трансляция телеметрии отключена.")
    else:
        logger.info(f"Телеметрия: TG={ENVIRONMENT_TELEGRAM_FORWARD}, Mesh={ENVIRONMENT_MESH_FORWARD}")
    if FAIL_MSG:
        logger.info("Уведомления о недоставке включены.")
    else:
        logger.info("Уведомления о недоставке ОТКЛЮЧЕНЫ (FAIL_MSG пуст).")
    if REACTIONS_ENABLED:
        logger.info("Поддержка реакций TG→Mesh ВКЛЮЧЕНА.")
    if REPLY_TRACKING_ENABLED:
        logger.info("Отслеживание reply Mesh→TG ВКЛЮЧЕНО.")
    try:
        API_ID, API_HASH, PHONE, raw_dest_id, ADMIN_CHAT_ID = load_acc_data(config_file)
        DEST_NODE_ID = normalize_node_id(raw_dest_id)
        logger.info(f"Конфиг загружен. DEST_NODE_ID: !{DEST_NODE_ID}")
        if ADMIN_CHAT_ID:
            logger.info(f"ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
        CONTACT_BOOK_FILE = f"contacts_{DEST_NODE_ID}.json"
        BLACKLIST_FILE = f"blacklist_{DEST_NODE_ID}.json"
        STATE_FILE = os.path.join(ACC_BD_PATH, f"forward_state_{DEST_NODE_ID}.json")
        logger.info(f"Файл записной книжки: {CONTACT_BOOK_FILE}")
        logger.info(f"Файл чёрного списка: {BLACKLIST_FILE}")
        load_forward_state()
    except Exception as e:
        logger.error(f"Ошибка конфига: {e}")
        sys.exit(1)
    # Вывод версии и активных опций в лог
    active_options_desc = []
    if forward_enabled: active_options_desc.append("FORWARD_ENABLED (Пересылка)")
    if FAIL_MSG: active_options_desc.append("FAIL_MSG (Уведомления о недоставке)")
    if ENVIRONMENT_TELEGRAM_FORWARD: active_options_desc.append("ENVIRONMENT_TELEGRAM_FORWARD (Телеметрия в Telegram)")
    if ENVIRONMENT_MESH_FORWARD: active_options_desc.append("ENVIRONMENT_MESH_FORWARD (Телеметрия в Mesh)")
    if TRANSLIT_ENABLED: active_options_desc.append("TRANSLIT_ENABLED (Транслитерация)")
    if REACTIONS_ENABLED: active_options_desc.append("REACTIONS_ENABLED (Реакции)")
    if REPLY_TRACKING_ENABLED: active_options_desc.append("REPLY_TRACKING_ENABLED (Отслеживание ответов)")
    if DEBUG: active_options_desc.append("DEBUG (Отладка)")
    logger.info(f"TeleMesh v{VERSION} запущен")
    logger.info(f"Target ID: !{DEST_NODE_ID}")
    if active_options_desc:
        logger.info(f"Активные опции: {', '.join(active_options_desc)}")

    # --------------------- НАСТРОЙКА TELEGRAM С ПРОКСИ И ПРИВЯЗКОЙ К ИНТЕРФЕЙСУ ---------------------
    # Прокси
    proxy_params = None
    if TG_PROXY_TYPE and TG_PROXY_HOST and TG_PROXY_PORT:
        import socks
        proxy_type_map = {
            'socks5': socks.SOCKS5,
            'socks4': socks.SOCKS4,
            'http': socks.HTTP,
        }
        ptype = proxy_type_map.get(TG_PROXY_TYPE.lower())
        if ptype:
            proxy_params = (ptype, TG_PROXY_HOST, TG_PROXY_PORT, TG_PROXY_USERNAME, TG_PROXY_PASSWORD)
            logger.info(f"Telegram прокси настроен: {TG_PROXY_TYPE}://{TG_PROXY_HOST}:{TG_PROXY_PORT}")
        else:
            logger.warning(f"Неподдерживаемый тип прокси: {TG_PROXY_TYPE}")

    # Выбор класса соединения (с привязкой к интерфейсу или стандартный)
    if TG_INTERFACE:
        conn_class = BindToInterfaceConnection
        conn_params = {'interface_name': TG_INTERFACE}
        logger.info(f"Telegram будет использовать интерфейс {TG_INTERFACE} (полная привязка SO_BINDTODEVICE)")
    else:
        from telethon.network.connection.tcpabridged import ConnectionTcpAbridged
        conn_class = ConnectionTcpAbridged
        conn_params = {}

    client = TelegramClient(
        SESSION_NAME, API_ID, API_HASH,
        proxy=proxy_params,
        connection=conn_class,
        connection_params=conn_params
    )

    client.add_event_handler(handle_new_message, events.NewMessage(incoming=True))
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
