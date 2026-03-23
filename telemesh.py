import sys
import os
import asyncio
import logging
import json
from datetime import datetime
from threading import Thread
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
VERSION = "1.4.042"

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

# --------------------- Глобальные переменные ---------------------
last_sender = None
forward_enabled = FORWARD_ENABLED
telegram_queue = queue.Queue()
MY_NODE_ID = None
interface = None

# Словарь для отслеживания сообщений, ожидающих обработки
pending_acks = {}
ack_event_queue = queue.Queue()

# --------------------- НОВЫЕ ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---------------------

# Кэш сопоставления: mesh_packet_id -> {tg_user_id, tg_msg_id, timestamp}
# Используется при получении reply из Mesh для определения автора в TG
mesh_msg_cache = OrderedDict()

# Обратный кэш: tg_msg_id -> mesh_packet_id
# Используется при получении реакций в TG для определения сообщения в Mesh
tg_to_mesh_cache = OrderedDict()

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

# --------------------- Функции управления кэшем ---------------------

def cache_add_mesh_message(mesh_packet_id, tg_user_id, tg_msg_id):
    """Добавляет сопоставление mesh_packet_id → {tg_user_id, tg_msg_id} в кэш."""
    if not REPLY_TRACKING_ENABLED:
        return
    
    # Удаляем старые записи при превышении лимита
    while len(mesh_msg_cache) >= MSG_CACHE_MAX_SIZE:
        mesh_msg_cache.popitem(last=False)
    
    mesh_msg_cache[mesh_packet_id] = {
        'tg_user_id': tg_user_id,
        'tg_msg_id': tg_msg_id,
        'timestamp': time.time()
    }
    
    # Также добавляем в обратный кэш
    while len(tg_to_mesh_cache) >= MSG_CACHE_MAX_SIZE:
        tg_to_mesh_cache.popitem(last=False)
    
    tg_to_mesh_cache[tg_msg_id] = mesh_packet_id
    
    logger.debug(f"Кэш: добавлено Mesh[{mesh_packet_id}] → TG[user={tg_user_id}, msg={tg_msg_id}]")
    logger.debug(f"Размер кэша: mesh_msg_cache={len(mesh_msg_cache)}, tg_to_mesh_cache={len(tg_to_mesh_cache)}")

def cache_get_mesh_message(mesh_packet_id):
    """Получает информацию о TG сообщении по mesh_packet_id."""
    if not REPLY_TRACKING_ENABLED:
        return None
    
    info = mesh_msg_cache.get(mesh_packet_id)
    if info:
        # Проверяем TTL
        if time.time() - info['timestamp'] > MSG_CACHE_TTL:
            del mesh_msg_cache[mesh_packet_id]
            return None
        return info
    return None

def cache_get_mesh_id_by_tg(tg_msg_id):
    """Получает mesh_packet_id по tg_msg_id."""
    if not REACTIONS_ENABLED:
        return None
    
    mesh_id = tg_to_mesh_cache.get(tg_msg_id)
    if mesh_id:
        logger.debug(f"Найден mesh_packet_id={mesh_id} для tg_msg_id={tg_msg_id}")
        return mesh_id
    
    logger.debug(f"tg_msg_id={tg_msg_id} не найден в кэше. Доступные: {list(tg_to_mesh_cache.keys())[-5:]}")
    return None

def cache_cleanup():
    """Периодическая очистка устаревших записей в кэше."""
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
# Meshtastic использует Unicode codepoints для реакций
# Ограниченный набор поддерживаемых реакций
REACTION_MAPPING = {
    '👍': 0x1F44D,  # thumbs up
    '👎': 0x1F44E,  # thumbs down
    '❤️': 0x2764,   # red heart
    '💔': 0x1F494,  # broken heart
    '😂': 0x1F602,  # face with tears of joy
    '😮': 0x1F62E,  # face with open mouth
    '😢': 0x1F622,  # crying face
    '😡': 0x1F621,  # pouting face
    '🔥': 0x1F525,  # fire
    '🎉': 0x1F389,  # party popper
    '👏': 0x1F44F,  # clapping hands
    '🤔': 0x1F914,  # thinking face
    '😅': 0x1F605,  # grinning face with sweat
    '🙏': 0x1F64F,  # folded hands
    '💯': 0x1F4AF,  # hundred points
    '⭐': 0x2B50,   # star
    '❓': 0x2753,   # question mark
    '✅': 0x2705,   # check mark button
    '❌': 0x274C,   # cross mark
}

def get_emoji_codepoint(emoji_str):
    """Получает Unicode codepoint для эмодзи."""
    # Убираем вариант селекторы если есть
    emoji_clean = emoji_str.replace('\uFE0F', '').replace('\u200D', '')
    
    # Проверяем в таблице маппинга
    if emoji_clean in REACTION_MAPPING:
        return REACTION_MAPPING[emoji_clean]
    
    # Если нет в таблице, берём первый codepoint
    if emoji_clean:
        return ord(emoji_clean[0])
    
    return None

def clean_emoji_for_telegram(emoji_str):
    """
    Очищает emoji от модификаторов тона кожи и вариант-селекторов.
    Telegram принимает только базовые emoji.
    """
    if not emoji_str:
        return emoji_str
    
    # Удаляем вариант-селекторы (FE0F)
    result = emoji_str.replace('\uFE0F', '')
    
    # Удаляем Zero-Width Joiner (200D) - используется для составных emoji
    result = result.replace('\u200D', '')
    
    # Удаляем модификаторы тона кожи (Fitzpatrick type 1-6: U+1F3FB to U+1F3FF)
    skin_tone_modifiers = [
        '\U0001F3FB',  # Type 1-2 (🏻)
        '\U0001F3FC',  # Type 3 (🏼)
        '\U0001F3FD',  # Type 4 (🏽)
        '\U0001F3FE',  # Type 5 (🏾)
        '\U0001F3FF',  # Type 6 (🏿)
    ]
    
    for modifier in skin_tone_modifiers:
        result = result.replace(modifier, '')
    
    # Берём только первый базовый emoji символ
    # (могут остаться другие составные части)
    if result:
        # Возвращаем только первый символ (базовый emoji)
        return result[0]
    
    return emoji_str

# --------------------- Функции для работы с ID нод ---------------------
def normalize_node_id(node_id):
    """
    Приводит ID ноды к нижнему регистру шестнадцатеричной строки БЕЗ префикса '!'.
    Принимает int, или str (с префиксами !, 0x или без них).
    """
    if node_id is None: return None
    
    # Если это число (обычно прилетает из пакетов как int)
    if isinstance(node_id, int):
        return f"{node_id:x}"
        
    s = str(node_id).strip()
    
    # Убираем известные префиксы
    if s.startswith('!'):
        s = s[1:]
    elif s.lower().startswith('0x'):
        s = s[2:]
    
    # Пробуем интерпретировать оставшееся как число
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

def normalize_command_text(text: str) -> str:
    """
    Нормализация текста команды для сравнения.
    
    1. Удаляет пробелы и любые пробельные символы после '!'
       Пример: '! старт' -> '!старт', '!  старт' -> '!старт'
    
    2. Заменяет похожие латинские буквы на кириллицу
       Проблема: на Meshtastic устройствах при вводе могут смешиваться
       латинские и кириллические символы, которые выглядят одинаково.
       Пример: '! Cтapт' содержит: C (лат), т (кир), a (лат), p (лат), т (кир)
    
    3. Приводит к нижнему регистру
    """
    # Маппинг латинских букв на кириллические аналоги
    lat_to_cyr = {
        'a': 'а', 'c': 'с', 'e': 'е', 'o': 'о', 'p': 'р', 
        'x': 'х', 'y': 'у'
    }
    
    result = text
    
    # Если начинается с '!', удаляем все пробельные символы после неё
    if result.startswith('!'):
        # Пропускаем '!' и удаляем все пробельные символы в начале остатка
        result = '!' + result[1:].lstrip()
    
    # Приводим к нижнему регистру
    result = result.lower()
    
    # Заменяем латинские буквы на кириллицу
    for lat, cyr in lat_to_cyr.items():
        result = result.replace(lat, cyr)
    
    return result

# --------------------- Загрузка параметров ---------------------
def load_acc_data(config_file):
    full_path = os.path.join(ACC_BD_PATH, config_file)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Файл {full_path} не найден")
    with open(full_path, 'r', encoding='utf-8') as f: content = f.read()
    namespace = {}
    exec(content, namespace)
    api_id = namespace.get('API_ID')
    api_hash = namespace.get('API_HASH')
    phone = namespace.get('PHONE')
    dest_node_id = namespace.get('DEST_NODE_ID')
    admin_chat_id = namespace.get('ADMIN_CHAT_ID')  # Опционально
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
            current += char; current_bytes += c_bytes
        else:
            parts.append(current); current = char; current_bytes = c_bytes
    if current: parts.append(current)
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
    except: return "Ссылка"

def format_message_with_signature(message: str) -> str:
    return f"{message}\n{SIGNATURE}" if SIGNATURE else message

# --------------------- Meshtastic ---------------------

def on_meshtastic_connect(interface, topic=None):
    global MY_NODE_ID
    try:
        my_info = interface.getMyNodeInfo()
        
        # Получаем ID ноды (num - это int)
        raw_id = my_info.get('num')
        if raw_id:
            MY_NODE_ID = normalize_node_id(raw_id) # Сохраняем как hex string без !
        else: 
            logger.warning("Не удалось получить ID текущей ноды из my_info!")

        # Получаем имена ноды
        long_name = "Unknown"
        short_name = "???"
        user_info = my_info.get('user')
        if user_info:
            long_name = user_info.get('longName', 'Unknown')
            short_name = user_info.get('shortName', '???')

        # Логирование в зависимости от режима DEBUG
        if DEBUG:
            logger.info("Meshtastic подключён → %s", my_info)
        else:
            logger.info(f"Meshtastic подключён. ID: !{MY_NODE_ID}, Name: {long_name} ({short_name})")

        # Формируем список активных опций (только True)
        active_options = []
        if forward_enabled: active_options.append("FORWARD_ENABLED")
        if FAIL_MSG: active_options.append("FAIL_MSG")
        if ENVIRONMENT_TELEGRAM_FORWARD: active_options.append("ENVIRONMENT_TELEGRAM_FORWARD")
        if ENVIRONMENT_MESH_FORWARD: active_options.append("ENVIRONMENT_MESH_FORWARD")
        if TRANSLIT_ENABLED: active_options.append("TRANSLIT_ENABLED")
        if REACTIONS_ENABLED: active_options.append("REACTIONS_ENABLED")
        if REPLY_TRACKING_ENABLED: active_options.append("REPLY_TRACKING_ENABLED")
        if DEBUG: active_options.append("DEBUG")

        # Формируем приветственное сообщение
        msg_lines = [
            f"📟 TeleMesh v{VERSION}",
            f"Node: {long_name} ({short_name})"
        ]
        if active_options:
            msg_lines.append("\n".join(active_options))
        
        welcome_msg = "\n".join(msg_lines)
        
        # Отправляем в Mesh
        send_to_meshtastic(welcome_msg, want_ack=False)
        
        # Отправляем в Telegram Admin
        if ADMIN_CHAT_ID:
            status_lines = [
                f"📟 TeleMesh v{VERSION} запущен",
                f"Node: {long_name} ({short_name})",
                f"Target ID: !{DEST_NODE_ID}",
                f"\nEnabled options:",
                "\n".join(active_options)
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
            except Exception: pass

        decoded = packet.get('decoded')
        if not decoded: return

        portnum = decoded.get('portnum')
        
        # 1. Обработка ACK (Routing App)
        # Интересуют только ACK от нашей целевой ноды
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
        
        # 2. Обработка телеметрии (Environment Telemetry)
        # РЕЖИМ СНИФФЕРА: Ловим все пакеты из эфира, не проверяя отправителя или получателя.
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
                        
                        if ENVIRONMENT_TELEGRAM_FORWARD:
                            if ADMIN_CHAT_ID:
                                telegram_queue.put({'user_id': ADMIN_CHAT_ID, 'message': env_msg})
                                logger.info(f"→ Телеметрия отправлена в Telegram.")
                        
                        if ENVIRONMENT_MESH_FORWARD:
                            send_to_meshtastic(env_msg, want_ack=False)
                            logger.info(f"→ Телеметрия отправлена в Mesh на !{DEST_NODE_ID}.")
                    else:
                        if DEBUG:
                            logger.debug("Телеметрия environment_metrics пуста (нет нужных полей).")
                
                elif telemetry and 'device_metrics' in telemetry:
                    if DEBUG:
                        logger.debug("Получена device_metrics (батарея), игнорируем.")
                else:
                    if DEBUG:
                        logger.debug(f"Пакет телеметрии без environment/device metrics.")

        # 3. Обработка текстовых сообщений
        if str(portnum) not in ('1', 'TEXT_MESSAGE_APP'): return
        
        # Сначала проверяем, что сообщение адресовано НАМ
        to_id = packet.get('to') or packet.get('toId')
        if MY_NODE_ID is not None:
            if not compare_node_ids(to_id, MY_NODE_ID):
                logger.debug(f"Сообщение не нам (To: !{normalize_node_id(to_id)}). Игнорируем.")
                return

        payload = decoded.get('payload')
        text = None
        if isinstance(payload, bytes): text = payload.decode('utf-8', errors='ignore')
        elif isinstance(payload, str): text = payload
        if text: text = text.strip()
        
        from_id = packet.get('fromId') or packet.get('from')
        packet_id = packet.get('id')  # ID пакета в Mesh
        
        # Получаем reply_id если есть (сообщение является ответом)
        reply_id = decoded.get('replyId')
        
        # Получаем emoji флаг и проверяем, является ли это реакцией
        # emoji: 1 означает что payload содержит emoji символ
        emoji_flag = decoded.get('emoji')
        
        # Если это реакция (emoji flag = 1 и есть payload с emoji)
        if emoji_flag and payload:
            try:
                # Payload содержит UTF-8 байты emoji
                if isinstance(payload, bytes):
                    emoji_char = payload.decode('utf-8', errors='ignore')
                else:
                    emoji_char = str(payload)
                
                if emoji_char:
                    if DEBUG:
                        logger.debug(f"← Реакция {emoji_char} от !{normalize_node_id(from_id)}")
                    
                    # Ищем исходное сообщение в кэше по reply_id
                    if reply_id and REPLY_TRACKING_ENABLED:
                        original_msg_info = cache_get_mesh_message(reply_id)
                        if original_msg_info:
                            # Отправляем реакцию автору исходного сообщения через очередь
                            telegram_queue.put({
                                'user_id': original_msg_info['tg_user_id'],
                                'msg_id': original_msg_info['tg_msg_id'],
                                'reaction_emoji': emoji_char
                            })
                            if DEBUG:
                                logger.debug(f"→ Реакция {emoji_char} добавлена в очередь для TG пользователю {original_msg_info['tg_user_id']}")
                        else:
                            logger.debug(f"Реакция на сообщение {reply_id}, но оно не найдено в кэше")
                    return
            except Exception as e:
                logger.warning(f"Не удалось декодировать emoji: {e}")
        
        # Если нет текста и это не реакция - выходим
        if not text: return
        
        # ГЛАВНАЯ ПРОВЕРКА: Реагируем только если отправитель = DEST_NODE_ID
        if not compare_node_ids(from_id, DEST_NODE_ID):
            logger.debug(f"Сообщение от !{normalize_node_id(from_id)}, но цель !{DEST_NODE_ID}. Игнорируем.")
            return
        
        # ПРИОРИТЕТ: Нормализация и проверка управляющих команд
        # Сначала нормализуем текст для проверки команд
        text_normalized = normalize_command_text(text)
        
        if text_normalized in ('!stop', '!стоп'):
            logger.info(f"← Команда от !{normalize_node_id(from_id)}: {text}")
            if forward_enabled:
                forward_enabled = False
                send_to_meshtastic("⚠️ Пересылка ОСТАНОВЛЕНА", want_ack=False)
            else: 
                send_to_meshtastic("ℹ️ Пересылка уже остановлена", want_ack=False)
            return
        elif text_normalized in ('!start', '!старт'):
            logger.info(f"← Команда от !{normalize_node_id(from_id)}: {text}")
            if not forward_enabled:
                forward_enabled = True
                send_to_meshtastic("✅ Пересылка ВОЗОБНОВЛЕНА", want_ack=False)
            else: 
                send_to_meshtastic("ℹ️ Пересылка уже активна", want_ack=False)
            return
        
        # Если пересылка отключена - выходим (команды уже обработаны выше)
        if not forward_enabled: return
        
        logger.info(f"← Meshtastic от !{normalize_node_id(from_id)}: {text}")
        
        # Обработка reply из Mesh
        if reply_id and REPLY_TRACKING_ENABLED:
            original_msg_info = cache_get_mesh_message(reply_id)
            if original_msg_info:
                # Отправляем ответ конкретному автору как reply
                telegram_queue.put({
                    'user_id': original_msg_info['tg_user_id'],
                    'message': format_message_with_signature(text),
                    'msg_id': original_msg_info['tg_msg_id'],
                    'mesh_packet_id': packet_id  # Сохраняем для кэша
                })
                logger.info(f"→ Ответ отправлен в TG пользователю {original_msg_info['tg_user_id']} как reply на msg {original_msg_info['tg_msg_id']}")
            else:
                # Сообщение не найдено в кэше - отправляем последнему отправителю
                logger.debug(f"Reply на сообщение {reply_id}, но оно не найдено в кэше. Отправка last_sender.")
                if last_sender is not None:
                    telegram_queue.put({
                        'user_id': last_sender, 
                        'message': format_message_with_signature(text),
                        'mesh_packet_id': packet_id  # Сохраняем для кэша
                    })
                    logger.info(f"→ Сообщение добавлено в очередь для Telegram {last_sender}")
                else:
                    send_to_meshtastic("⚠️ Нет активного чата в Telegram", want_ack=False)
        else:
            # Обычное сообщение (не reply)
            if last_sender is not None:
                telegram_queue.put({
                    'user_id': last_sender, 
                    'message': format_message_with_signature(text),
                    'mesh_packet_id': packet_id  # Сохраняем для кэша
                })
                logger.info(f"→ Сообщение добавлено в очередь для Telegram {last_sender}")
            else:
                send_to_meshtastic("⚠️ Нет активного чата в Telegram", want_ack=False)
                
    except Exception as e:
        logger.error(f"Ошибка обработки Meshtastic: {e}", exc_info=True)

def send_to_meshtastic(text: str, want_ack=True, user_id=None, msg_id=None, reply_id=None):
    """Отправка сообщения в Meshtastic. Возвращает ID пакета или None."""
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
            replyId=reply_id  # Передаём reply_id если есть
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

        # Сокращаем текст в логе до 30 символов для единообразия
        log_text = text[:30] + "..." if len(text) > 30 else text
        logger.info(f"→ Meshtastic TX (ID: {packet_id}, To: !{DEST_NODE_ID}, ACK: {want_ack}, ReplyTo: {reply_id}): {log_text}")
        
        # Сохраняем сопоставление для reply из Mesh → TG
        if packet_id is not None and user_id is not None and msg_id is not None:
            logger.debug(f"Сохраняем в кэш: packet_id={packet_id}, user_id={user_id}, msg_id={msg_id}")
            cache_add_mesh_message(packet_id, user_id, msg_id)
        else:
            logger.debug(f"НЕ сохраняем в кэш: packet_id={packet_id}, user_id={user_id}, msg_id={msg_id}")
        
        return packet_id
        
    except Exception as e:
        logger.error(f"Ошибка TX (возможно потеря связи): {e}")
        try:
            if interface: interface.close()
        except: pass
        interface = None
        return None

def send_reaction_to_meshtastic(mesh_packet_id: int, emoji_codepoint: int, emoji_char: str = None):
    """Отправка реакции (emoji) на сообщение в Meshtastic.
    
    В Meshtastic протоколе:
    - emoji: 1 - это ФЛАГ, указывающий что это реакция
    - payload - содержит UTF-8 байты самого эмодзи
    - reply_id - ID пакета, на который реагируем
    """
    global interface
    
    if not interface:
        logger.warning("Попытка отправки реакции без активного подключения.")
        return False
    
    try:
        dest_id_int = int(DEST_NODE_ID, 16)
        
        # Получаем UTF-8 байты эмодзи
        if emoji_char:
            # Если передали сам эмодзи, используем его
            emoji_bytes = emoji_char.encode('utf-8')
        else:
            # Иначе конвертируем из codepoint
            emoji_char = chr(emoji_codepoint)
            emoji_bytes = emoji_char.encode('utf-8')
        
        from meshtastic.protobuf import portnums_pb2
        
        # Используем sendData с параметрами emojiIndex и replyId
        # emojiIndex=1 означает что это реакция (флаг)
        # data (emoji_bytes) идёт в payload
        # replyId устанавливает reply_id в пакете
        
        if DEBUG:
            logger.debug(f"Отправка реакции: emoji={emoji_char}, bytes={emoji_bytes}, reply_to={mesh_packet_id}, to={dest_id_int}")
        
        try:
            result = interface.sendData(
                data=emoji_bytes,
                destinationId=dest_id_int,
                portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
                wantAck=False,
                channelIndex=0,
                emojiIndex=1,  # ФЛАГ что это реакция
                replyId=mesh_packet_id
            )
            
            if result:
                logger.info(f"→ Реакция отправлена в Mesh (emoji={emoji_char}, reply_to={mesh_packet_id}, to=!{DEST_NODE_ID})")
                return True
            else:
                logger.warning("sendData вернул None")
        except TypeError as te:
            # Если sendData не поддерживает emojiIndex/replyId, пробуем другой метод
            logger.debug(f"sendData не поддерживает нужные параметры: {te}, пробуем _sendPacket...")
            
            from meshtastic.protobuf import mesh_pb2
            
            # Метод: Используем низкоуровневый _sendPacket
            if hasattr(interface, '_sendPacket'):
                try:
                    mesh_packet = mesh_pb2.MeshPacket()
                    mesh_packet.to = dest_id_int
                    mesh_packet.channel = 0
                    mesh_packet.want_ack = False
                    
                    mesh_packet.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
                    mesh_packet.decoded.payload = emoji_bytes
                    mesh_packet.decoded.reply_id = mesh_packet_id
                    mesh_packet.decoded.emoji = 1  # ФЛАГ реакции
                    
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
            address = f"{TCP_HOST}:{TCP_PORT}"
            logger.info("Подключение к Meshtastic по TCP: %s", address)
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
    """Бесконечный цикл поддержания соединения с Meshtastic."""
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
            # Периодическая очистка кэша (каждые 60 циклов = ~1 минута)
            cleanup_counter += 1
            if cleanup_counter >= 60:
                cache_cleanup()
                cleanup_counter = 0
            
            await asyncio.sleep(1)

# --------------------- Telegram ---------------------
client = None

async def handle_new_message(event):
    global last_sender, forward_enabled
    if not isinstance(event.peer_id, PeerUser): return
    sender = await event.get_sender()
    if sender.bot or sender.id == (await client.get_me()).id: return
    last_sender = sender.id
    
    # Управление пересылкой только через Mesh
    if not forward_enabled:
        await event.reply("⚠️ Пересылка остановлена.")
        return

    text = event.message.message.strip() if event.message.message else ""
    prefix = f"{sender.first_name or ''} {sender.last_name or ''}".strip() + ": "
    text = clean_empty_lines(text)
    
    # Сначала обрабатываем медиа (до проверки на пустой текст!)
    url_match = re.search(r'(https?://[^\s]+)', text) if text else None
    processed_text = text
    
    if url_match:
        url = url_match.group(0)
        title = get_page_title(url)
        parsed = urlparse(url)
        is_yt = parsed.hostname in ('www.youtube.com', 'youtube.com', 'youtu.be')
        processed_text = text.replace(url, f"▶️{title}" if is_yt else f"🔗{title}", 1)
        if len(processed_text) > MAX_MSG_LEN: processed_text = processed_text[:MAX_MSG_LEN-5] + "..."

    if not url_match:
        # Проверяем медиа-вложения
        media = ""
        if event.message.photo: media = "[PIC]"
        elif event.message.gif: media = "[GIF]"
        elif event.message.video: media = "[VIDEO]"
        elif event.message.audio: media = "[AUDIO]"
        elif event.message.voice: media = "[VOICE]"
        elif event.message.document: 
            doc = event.message.document
            if hasattr(doc, 'mime_type') and doc.mime_type:
                media = "[" + doc.mime_type.split('/')[0].upper() + "]"
            else:
                media = "[DOC]"
        if media: processed_text = media + (" " + processed_text if processed_text else "")

    # Если после всей обработки нет текста - выходим
    if not processed_text: return

    full_text = visual_translit(prefix + processed_text)
    parts = split_message(full_text, MAX_MSG_LEN)
    
    reply_to_id = event.message.id
    
    for i, part in enumerate(parts, 1):
        packet_id = send_to_meshtastic(part, want_ack=True, user_id=sender.id, msg_id=reply_to_id)
        
        if packet_id is not None:
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
            # Сокращаем текст в предупреждении до 30 символов
            logger.warning(f"Не удалось получить ID для сообщения '{part[:30]}...'")
        
        if i < len(parts): await asyncio.sleep(MESSAGE_SEND_DELAY / 1000.0)
    
    if ADMIN_CHAT_ID: await client.send_message(ADMIN_CHAT_ID, f"→ Mesh: {full_text}")

async def handle_reaction(event):
    """Обработка реакций в Telegram."""
    if not REACTIONS_ENABLED:
        return
    
    try:
        # Структура UpdateMessageReactions:
        # event.reactions - это MessageReactions объект с атрибутом results
        # event.reactions.results - список ReactionCount
        # ReactionCount.reaction - ReactionEmoji с атрибутом emoticon
        
        message_id = event.msg_id
        peer_id = getattr(event, 'peer', None)
        reactions_obj = getattr(event, 'reactions', None)
        
        if DEBUG:
            logger.debug(f"← TG Reaction: msg_id={message_id}, peer={peer_id}")
        
        # Проверяем, что это личное сообщение
        if peer_id and not isinstance(peer_id, PeerUser):
            if DEBUG:
                logger.debug(f"Реакция не в личном чате, пропускаем")
            return
        
        # Ищем сообщение в кэше для получения mesh_packet_id
        mesh_packet_id = cache_get_mesh_id_by_tg(message_id)
        if mesh_packet_id is None:
            if DEBUG:
                logger.debug(f"Реакция на сообщение {message_id}, но оно не найдено в кэше Mesh")
                logger.debug(f"Текущий кэш tg_to_mesh_cache: {list(tg_to_mesh_cache.keys())}")
            return
        
        # Получаем список реакций из MessageReactions.results
        reactions_list = getattr(reactions_obj, 'results', None) if reactions_obj else None
        
        if not reactions_list:
            if DEBUG:
                logger.debug("Список реакций пуст")
            return
        
        # Итерируем по реакциям
        for reaction_count in reactions_list:
            try:
                # reaction_count - это ReactionCount
                # reaction_count.reaction - это ReactionEmoji
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
                
                if DEBUG:
                    logger.debug(f"Извлечён emoji: {emoji}")
                
                if emoji:
                    # Очищаем emoji от модификаторов для Mesh
                    clean_emoji_mesh = clean_emoji_for_telegram(emoji)
                    emoji_codepoint = get_emoji_codepoint(clean_emoji_mesh)
                    
                    if emoji_codepoint:
                        logger.info(f"← Реакция {emoji} в TG на msg {message_id}")
                        
                        # Отправляем реакцию в Mesh (передаём и codepoint, и сам emoji)
                        success = send_reaction_to_meshtastic(mesh_packet_id, emoji_codepoint, clean_emoji_mesh)
                        if success:
                            logger.info(f"→ Реакция {clean_emoji_mesh} отправлена в Mesh на пакет {mesh_packet_id}")
                        else:
                            logger.warning(f"Не удалось отправить реакцию {clean_emoji_mesh} в Mesh")
            except Exception as re:
                logger.debug(f"Ошибка обработки отдельной реакции: {re}")
                    
    except Exception as e:
        logger.error(f"Ошибка обработки реакции: {e}", exc_info=True)

async def telegram_worker():
    while True:
        try:
            item = telegram_queue.get(timeout=0.1)
            
            # Проверяем, это реакция или обычное сообщение
            if item.get('reaction_emoji'):
                # Это реакция - отправляем через MTProto API
                try:
                    emoji = item['reaction_emoji']
                    user_id = item['user_id']
                    msg_id = item['msg_id']
                    
                    # Очищаем emoji от модификаторов
                    clean_emoji = clean_emoji_for_telegram(emoji)
                    
                    logger.debug(f"Отправка реакции: оригинал='{emoji}', очищенный='{clean_emoji}'")
                    
                    # Получаем InputPeer для пользователя
                    peer = await client.get_input_entity(user_id)
                    
                    # Создаём ReactionEmoji объект
                    from telethon.tl.types import ReactionEmoji as TLReactionEmoji
                    
                    # Отправляем реакцию через SendReactionRequest
                    await client(functions.messages.SendReactionRequest(
                        peer=peer,
                        msg_id=msg_id,
                        reaction=[TLReactionEmoji(emoticon=clean_emoji)],
                        big=False
                    ))
                    logger.info(f"✓ Реакция {clean_emoji} отправлена в Telegram пользователю {user_id}")
                except Exception as re:
                    logger.error(f"Ошибка отправки реакции в TG: {re}")
                    # Если реакция не отправилась, отправляем текстом как fallback
                    try:
                        fallback_text = f"💬 Реакция: {item['reaction_emoji']}"
                        # Добавляем подпись если есть
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
                # Обычное сообщение
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
                
                # Сохраняем в кэш для обратных реакций (TG → Mesh)
                # Связываем: tg_msg_id → mesh_packet_id
                mesh_packet_id = item.get('mesh_packet_id')
                if mesh_packet_id is not None and msg_id is not None:
                    # Добавляем в обратный кэш: tg_msg_id → mesh_packet_id
                    tg_to_mesh_cache[msg_id] = mesh_packet_id
                    
                    # Также обновляем основную запись если её нет
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
                        if pid in pending_acks:
                            msg_info = pending_acks[pid]
                            logger.info(f"✓ Доставка подтверждена (ACK) для ID {pid}: \"{msg_info['text'][:30]}...\"")
                            del pending_acks[pid]
            except queue.Empty:
                pass

            now = time.time()
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
    global client, API_ID, API_HASH, PHONE, DEST_NODE_ID, ADMIN_CHAT_ID
    
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
    
    if len(sys.argv) < 2:
        print(f"Использование: python {sys.argv[0]} <файл_параметров>")
        sys.exit(1)
    
    try:
        API_ID, API_HASH, PHONE, raw_dest_id, ADMIN_CHAT_ID = load_acc_data(sys.argv[1])
        DEST_NODE_ID = normalize_node_id(raw_dest_id)
        logger.info(f"Конфиг загружен. DEST_NODE_ID: !{DEST_NODE_ID}")
        if ADMIN_CHAT_ID:
            logger.info(f"ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
    except Exception as e:
        logger.error(f"Ошибка конфига: {e}")
        sys.exit(1)
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    client.add_event_handler(handle_new_message, events.NewMessage(incoming=True))
    
    # Добавляем обработчик реакций через Raw updates
    if REACTIONS_ENABLED:
        @client.on(events.Raw)
        async def raw_event_handler(event):
            if isinstance(event, UpdateMessageReactions):
                await handle_reaction(event)
    
    await client.start(phone=PHONE)
    
    me = await client.get_me()
    logger.info(f"Telegram: {me.first_name} ({me.phone})")
    
    asyncio.create_task(meshtastic_connection_manager())
    asyncio.create_task(telegram_worker())
    asyncio.create_task(ack_monitor_loop())
    
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        if interface: send_to_meshtastic(GOODBYE_MSG, want_ack=False)
    except Exception as e:
        logger.critical(f"Fatal: {e}")
    finally:
        if interface: interface.close()
