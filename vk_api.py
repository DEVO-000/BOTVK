# -*- coding: utf-8 -*-
"""
vk_api.py — VK Bots Long Poll API + слой совместимости с python-telegram-bot.

Этот модуль заменяет библиотеку python-telegram-bot для бота во «ВКонтакте»,
сохраняя знакомый интерфейс (Update / Message / CallbackQuery / фильтры /
ConversationHandler / JobQueue / Application), поэтому вся бизнес-логика
main.py переносится практически без изменений.

Реализовано:
  * Long Poll сервера VK (groups.getLongPollServer + цикл a_check, version=3);
  * Тротлинг вызовов VK API (защита от ошибки 6 «too many requests»);
  * Отправка/редактирование/удаление сообщений, отправка фотографий (QR-коды);
  * Инлайн- и обычные клавиатуры с учётом официальных лимитов VK:
      - обычная: до 40 кнопок, 10 рядов, до 5 кнопок в ряду;
      - инлайн:  до 10 кнопок, 6 рядов, до 5 кнопок в ряду
      (слишком большие инлайн-клавиатуры автоматически разбиваются на
       несколько сообщений-«продолжений»);
  * Callback-кнопки (message_event + messages.sendMessageEventAnswer);
  * Автоподтверждение callback-событий (кнопка не «крутится» вечно);
  * Проверка подписки на группу (groups.isMember) вместо getChatMember;
  * Свой JobQueue (run_once / run_repeating / run_daily);
  * ConversationHandler (entry_points / states / fallbacks / allow_reentry);
  * Конвертация Markdown/HTML-текста Telegram в чистый текст VK
      (VK не поддерживает форматирование в сообщениях ботов).

Переменные окружения:
  VK_TOKEN          — ключ доступа сообщества (обязателен);
  VK_GROUP_ID       — числовой id сообщества (обязателен для long poll);
  VK_API_VERSION    — версия VK API (по умолчанию 5.154);
"""

import os
import io
import re
import json
import html as _html
import random
import asyncio
import logging
import time
import mimetypes
from datetime import datetime, timedelta, time as dt_time

import aiohttp

logger = logging.getLogger(__name__)

VK_API_BASE = "https://api.vk.com/method/"
VK_API_VERSION = os.environ.get("VK_API_VERSION", "5.154")

# Минимальный интервал между вызовами VK API (сек). VK допускает ~20 запросов
# в секунду на токен; держим небольшой запас. При ошибке 6 делаем ретраи.
_MIN_CALL_INTERVAL = 0.055
# Таймаут одного HTTP-вызова к VK API.
_API_TIMEOUT = 25

# ============================================================================
# === ОШИБКИ (совместимы по именам с telegram.error) ===
# ============================================================================


class TelegramError(Exception):
    """Базовая ошибка, аналог telegram.error.TelegramError."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class BadRequest(TelegramError):
    """Некорректный запрос (VK: код 100 и подобные)."""


class Forbidden(TelegramError):
    """Пользователь запретил сообщения от сообщества (VK: 900/901/1021)."""


class NetworkError(TelegramError):
    """Сетевой сбой при обращении к VK API."""


class RetryAfter(TelegramError):
    """VK попросил подождать (ошибка 6 — flood control)."""

    def __init__(self, retry_after):
        super().__init__(f"Flood control: retry after {retry_after}s")
        self.retry_after = retry_after


# Псевдонимы, используемые в main.py как telegram.error.BadRequest/Forbidden
TGBadRequest = BadRequest
TGForbidden = Forbidden


class VkApiError(Exception):
    """Ответ VK API с ошибкой (error_code, error_msg)."""

    def __init__(self, code, msg):
        super().__init__(f"VK API error {code}: {msg}")
        self.code = code
        self.msg = msg


# ============================================================================
# === РЕЖИМЫ ФОРМАТИРОВАНИЯ (ParseMode) ===
# ============================================================================


class ParseMode:
    HTML = "HTML"
    MARKDOWN = "Markdown"
    MARKDOWN_V2 = "MarkdownV2"


_MD_BOLD = re.compile(r"\*\*([^*\n]+)\*\*")
_MD_ITAL = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")
_MD_UND = re.compile(r"__([^_\n]+)__")
_MD_STRIKE = re.compile(r"~~([^~\n]+)~~")
_MD_CODE = re.compile(r"```\s*([\s\S]*?)```")
_MD_CODE2 = re.compile(r"`([^`\n]+)`")
_MD_LINK = re.compile(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)")
_MD_URL_AUTOLINK = re.compile(r"<(https?://[^>\s]+)>")


def _strip_markdown(text: str) -> str:
    """Убирает Markdown-разметку, оставляя чистый текст (VK её не рендерит)."""
    if not text:
        return text
    t = _MD_LINK.sub(lambda m: f"{m.group(1)} — {m.group(2)}", text)
    t = _MD_URL_AUTOLINK.sub(lambda m: m.group(1), t)
    t = _MD_CODE.sub(lambda m: m.group(1), t)
    t = _MD_CODE2.sub(lambda m: m.group(1), t)
    t = _MD_BOLD.sub(lambda m: m.group(1), t)
    t = _MD_UND.sub(lambda m: m.group(1), t)
    t = _MD_ITAL.sub(lambda m: m.group(1), t)
    t = _MD_STRIKE.sub(lambda m: m.group(1), t)
    # Экранированные спецсимволы MarkdownV2: \* \_ \[ \] \( \) ...
    t = re.sub(r"\\([_*\[\]()~`>#+\-=|{}.!\\])", r"\1", t)
    return t


_HTML_TAG = re.compile(r"</?(?:b|strong|i|em|u|ins|s|strike|del|code|pre|a|blockquote)\b[^>]*>", re.I)
_HTML_BR = re.compile(r"<br\s*/?>", re.I)
_HTML_LINK = re.compile(r'<a\s[^>]*href=["\']([^"\']+)["\'][^>]*>([\s\S]*?)</a>', re.I)


def _strip_html(text: str) -> str:
    if not text:
        return text
    t = _HTML_BR.sub("\n", text)
    t = _HTML_LINK.sub(lambda m: f"{m.group(2)} — {m.group(1)}", t)
    t = _HTML_TAG.sub("", t)
    t = _html.unescape(t)
    return t


def vk_text(text, parse_mode=None) -> str:
    """Приводит текст Telegram (Markdown/HTML) к чистому тексту VK."""
    if text is None:
        return ""
    t = str(text)
    if parse_mode == ParseMode.HTML:
        t = _strip_html(t)
    elif parse_mode in (ParseMode.MARKDOWN, ParseMode.MARKDOWN_V2):
        t = _strip_markdown(t)
    else:
        # Без parse_mode Telegram тоже не рендерил разметку — но на всякий
        # случай уберём HTML-теги, если они случайно остались в тексте.
        if "<" in t and ">" in t:
            t = _strip_html(t)
    # VK не любит управляющие символы.
    t = "".join(ch for ch in t if ch == "\n" or ch == "\t" or ch >= " ")
    return t


# ============================================================================
# === КНОПКИ И КЛАВИАТУРЫ ===
# ============================================================================

# Официальные лимиты VK (dev.vk.com → Клавиатуры для ботов):
VK_INLINE_MAX_BUTTONS = 10   # инлайн: до 10 кнопок
VK_INLINE_MAX_ROWS = 6       # инлайн: до 6 рядов
VK_KB_MAX_BUTTONS = 40       # обычная: до 40 кнопок
VK_KB_MAX_ROWS = 10          # обычная: до 10 рядов
VK_MAX_PER_ROW = 5           # до 5 кнопок в ряду (обе)
VK_LABEL_MAX = 40            # максимальная длина подписи кнопки
VK_PAYLOAD_MAX = 255         # максимальная длина payload (JSON-строка)

# Практические пределы для красивой вёрстки: не более 4 кнопок в ряду.
_CHUNK_PER_ROW = 4


def _pick_color(label: str, explicit=None):
    """Подбирает цвет VK-кнопки по эмодзи-префиксу подписи."""
    if explicit:
        return explicit
    if label.startswith(("✅", "🎉", "⭐", "💎", "💰", "🛒", "💳", "🟢", "🔓", "🎁")):
        return "positive"
    if label.startswith(("❌", "🗑", "🚫", "🔴", "⛔")):
        return "negative"
    if label.startswith(("⬅️", "◀", "🔙", "↩", "🔄", "ℹ", "❓", "👁")):
        return "secondary"
    return "primary"


def _chunk_rows(rows, per_row=_CHUNK_PER_ROW):
    """Разбивает ряды длиннее per_row на несколько коротких рядов."""
    out = []
    for row in rows:
        if not row:
            continue
        for i in range(0, len(row), per_row):
            out.append(row[i:i + per_row])
    return out


def _mk_button_json(button, color=None):
    """Сериализует кнопку совместимости в JSON-объект VK."""
    label = (button.text or button.label or "").strip()
    if len(label) > VK_LABEL_MAX:
        label = label[: VK_LABEL_MAX - 1] + "…"
    color = color or getattr(button, "vk_color", None) or _pick_color(label)
    action = {}
    cb = getattr(button, "callback_data", None)
    url = getattr(button, "url", None)
    if cb:
        payload = json.dumps({"c": str(cb)}, ensure_ascii=False)
        if len(payload.encode("utf-8")) > VK_PAYLOAD_MAX:
            # Редкий случай: обрезаем callback_data.
            payload = json.dumps({"c": str(cb)[:200]}, ensure_ascii=False)
        action = {"type": "callback", "payload": payload, "label": label}
        btn = {"action": action, "color": color}
    elif url:
        action = {"type": "open_url", "link": str(url), "label": label}
        # Для open_url VK игнорирует color — не отправляем его.
        btn = {"action": action}
    else:
        action = {"type": "text", "label": label}
        btn = {"action": action, "color": color}
    return btn


class InlineKeyboardButton:
    """Аналог telegram.InlineKeyboardButton."""

    def __init__(self, text=None, callback_data=None, url=None, label=None,
                 vk_color=None):
        self.text = text if text is not None else (label or "")
        self.label = self.text
        self.callback_data = callback_data
        self.url = url
        self.vk_color = vk_color


class KeyboardButton:
    """Аналог telegram.KeyboardButton (для обычных клавиатур)."""

    def __init__(self, text, vk_color=None):
        self.text = text
        self.label = text
        self.vk_color = vk_color


class InlineKeyboardMarkup:
    """Аналог telegram.InlineKeyboardMarkup.

    Дополнительно умеет разбивать слишком длинную клавиатуру на несколько
    «экранов» (VK ограничивает инлайн-клавиатуру 10 кнопками / 6 рядами).
    """

    def __init__(self, inline_keyboard=None, keyboard=None):
        rows = inline_keyboard if inline_keyboard is not None else (keyboard or [])
        # Нормализуем: элементы ряда могут быть InlineKeyboardButton или строка.
        self.rows = []
        for row in rows:
            norm_row = []
            for btn in row:
                if isinstance(btn, InlineKeyboardButton):
                    norm_row.append(btn)
                elif isinstance(btn, str):
                    norm_row.append(InlineKeyboardButton(btn, callback_data=btn))
                else:
                    norm_row.append(btn)
            if norm_row:
                self.rows.append(norm_row)

    # --- совместимость с python-telegram-bot -------------------------------
    @property
    def inline_keyboard(self):
        return self.rows

    def to_vk_chunks(self):
        """Возвращает список JSON-строк клавиатур (обычно из одного элемента).

        Если кнопок больше, чем разрешено VK для инлайн-клавиатуры, клавиатура
        делится на несколько частей — их надо отправить отдельными сообщениями.
        """
        rows = _chunk_rows(self.rows)
        chunks = []
        # Формируем чанки: не более VK_INLINE_MAX_BUTTONS кнопок и 6 рядов.
        current_rows, count = [], 0
        for row in rows:
            if (
                count + len(row) > VK_INLINE_MAX_BUTTONS
                or len(current_rows) + 1 > VK_INLINE_MAX_ROWS
            ) and current_rows:
                chunks.append(current_rows)
                current_rows, count = [], 0
            current_rows.append(row)
            count += len(row)
        if current_rows:
            chunks.append(current_rows)
        if not chunks:
            return [json.dumps({"inline": True, "buttons": []}, ensure_ascii=False)]
        result = []
        for chunk in chunks:
            buttons = [[_mk_button_json(b) for b in row] for row in chunk]
            result.append(
                json.dumps({"inline": True, "buttons": buttons}, ensure_ascii=False)
            )
        return result

    def to_vk(self):
        return self.to_vk_chunks()[0]

    @property
    def needs_split(self):
        return len(self.to_vk_chunks()) > 1

    # Для отладки
    def __repr__(self):
        total = sum(len(r) for r in self.rows)
        return f"<InlineKeyboardMarkup buttons={total}>"


class ReplyKeyboardMarkup:
    """Аналог telegram.ReplyKeyboardMarkup (обычная клавиатура под полем ввода)."""

    def __init__(self, keyboard=None, resize_keyboard=None,
                 one_time_keyboard=None, is_persistent=None, input_field_placeholder=None):
        self.raw_rows = keyboard or []
        self.one_time = bool(one_time_keyboard) if one_time_keyboard is not None else False
        self.resize_keyboard = resize_keyboard
        self.is_persistent = is_persistent

    @property
    def keyboard(self):
        return self.raw_rows

    def to_vk(self):
        """JSON обычной клавиатуры VK с соблюдением лимитов 40/10/5."""
        rows = []
        for row in self.raw_rows:
            norm = []
            for btn in row:
                if isinstance(btn, str):
                    norm.append(KeyboardButton(btn))
                else:
                    norm.append(btn)
            if norm:
                rows.append(norm)
        rows = _chunk_rows(rows)
        # Уплотняем до 5 кнопок в ряду, если иначе не влезаем в лимит VK.
        flat_count = sum(len(r) for r in rows)
        if len(rows) > VK_KB_MAX_ROWS or flat_count > VK_KB_MAX_BUTTONS:
            flat = [btn for row in rows for btn in row]
            rows = [flat[i:i + 5] for i in range(0, len(flat), 5)]
        if len(rows) > VK_KB_MAX_ROWS:
            logger.warning(
                "ReplyKeyboardMarkup: клавиатура превышает лимит VK (10 рядов) — "
                "обрезаю. Пользователю стоит скрыть лишние кнопки в настройках."
            )
            rows = rows[:VK_KB_MAX_ROWS]
        if sum(len(r) for r in rows) > VK_KB_MAX_BUTTONS:
            flat = [btn for row in rows for btn in row][:VK_KB_MAX_BUTTONS]
            rows = [flat[i:i + 5] for i in range(0, len(flat), 5)]
        buttons = []
        for row in rows:
            jrow = []
            for btn in row:
                label = (btn.text or "")[:VK_LABEL_MAX]
                color = getattr(btn, "vk_color", None) or _pick_color(label)
                jrow.append({
                    "action": {"type": "text", "label": label},
                    "color": color,
                })
            buttons.append(jrow)
        return json.dumps(
            {"one_time": self.one_time, "buttons": buttons}, ensure_ascii=False
        )

    def __repr__(self):
        return f"<ReplyKeyboardMarkup rows={len(self.raw_rows)}>"


class ReplyKeyboardRemove:
    """Аналог telegram.ReplyKeyboardRemove — в VK заменяется пустой клавиатурой."""

    def to_vk(self):
        return json.dumps({"one_time": True, "buttons": []}, ensure_ascii=False)


class ForceReply:
    """Заглушка для совместимости (VK не поддерживает force reply)."""

    def to_vk(self):
        return json.dumps({"one_time": True, "buttons": []}, ensure_ascii=False)


class LabeledPrice:
    """Совместимая заглушка telegram.LabeledPrice (оплата идёт вне VK)."""

    def __init__(self, label="", amount=0):
        self.label = label
        self.amount = amount


# ============================================================================
# === ОБЪЕКТЫ СООБЩЕНИЙ ===
# ============================================================================


class User:
    """Аналог telegram.User (данные из users.get)."""

    def __init__(self, id, first_name="", last_name="", username=""):
        self.id = int(id)
        self.first_name = first_name or ""
        self.last_name = last_name or ""
        self.username = username or ""
        self.is_bot = False

    @property
    def full_name(self):
        parts = [p for p in (self.first_name, self.last_name) if p]
        return " ".join(parts) or f"User{self.id}"

    def __repr__(self):
        return f"<User {self.id} {self.full_name!r}>"


class Chat:
    """Аналог telegram.Chat — приватный диалог с сообществом."""

    def __init__(self, id):
        self.id = int(id)
        self.type = "private"
        self.first_name = ""
        self.last_name = ""

    def __repr__(self):
        return f"<Chat {self.id}>"


class PhotoSize:
    """Обёртка над размером фото VK: file_id — это прямой URL картинки."""

    def __init__(self, url, width=0, height=0):
        self.file_id = url
        self.url = url
        self.width = width
        self.height = height


def _photo_sizes_from_vk(photo_obj):
    sizes = photo_obj.get("sizes") or []
    # Сортировка по площади, от меньшего к большему.
    def area(s):
        return (s.get("width") or 0) * (s.get("height") or 0)
    sizes = sorted(sizes, key=area)
    return [PhotoSize(s.get("url", ""), s.get("width", 0), s.get("height", 0))
            for s in sizes]


class File:
    """Аналог telegram.File — скачивание по URL (file_id у нас и есть URL)."""

    def __init__(self, file_id, bot=None):
        self.file_id = file_id
        self._bot = bot

    async def download_as_bytearray(self):
        buf = io.BytesIO()
        await self.download_to_memory(out=buf)
        return buf.getvalue()

    async def download_to_drive(self, custom_path=None):
        data = await self.download_as_bytearray()
        path = custom_path or f"vkfile_{random.getrandbits(48)}"
        with open(path, "wb") as f:
            f.write(data)
        return path

    async def download_to_memory(self, out=None):
        if out is None:
            out = io.BytesIO()
        if not self._bot:
            raise NetworkError("File: бот недоступен для скачивания")
        data = await self._bot._download_bytes(self.file_id)
        out.write(data)
        return out


class Message:
    """Аналог telegram.Message."""

    def __init__(self, bot, chat_id, message_id=None, text="", from_user=None,
                 date=None, photo=None, conversation_message_id=None,
                 caption=None, attachments=None, ref=None, payload=None):
        self._bot = bot
        self.message_id = message_id
        self.conversation_message_id = conversation_message_id
        self.chat_id = chat_id
        self.chat = Chat(chat_id)
        self.chat_id_int = int(chat_id)
        self.text = text
        self.caption = caption
        self.from_user = from_user
        self.date = date or datetime.utcnow()
        self.photo = photo or []
        self.attachments = attachments or []
        self.ref = ref
        self.vk_payload = payload
        self.content_type = "photo" if self.photo else "text"

    # --- утилиты ------------------------------------------------------------
    @property
    def id(self):
        return self.conversation_message_id or self.message_id

    async def reply_text(self, text, reply_markup=None, parse_mode=None,
                         quote=None, **kwargs):
        return await self._bot.send_message(
            self.chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode
        )

    async def reply_markdown(self, text, reply_markup=None, **kwargs):
        return await self._bot.send_message(
            self.chat_id, text, reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN,
        )

    async def reply_html(self, text, reply_markup=None, **kwargs):
        return await self._bot.send_message(
            self.chat_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML
        )

    async def reply_photo(self, photo, caption=None, parse_mode=None,
                          reply_markup=None, **kwargs):
        return await self._bot.send_photo(
            self.chat_id, photo, caption=caption, reply_markup=reply_markup
        )

    async def edit_text(self, text, reply_markup=None, parse_mode=None, **kwargs):
        return await self._bot.edit_message_text(
            self.chat_id, self.id, text, reply_markup=reply_markup,
            parse_mode=parse_mode,
        )

    async def edit_caption(self, caption, reply_markup=None, **kwargs):
        return await self._bot.edit_message_text(
            self.chat_id, self.id, caption or "", reply_markup=reply_markup
        )

    async def delete(self):
        return await self._bot.delete_message(self.chat_id, self.id)

    def __repr__(self):
        return f"<Message chat={self.chat_id} id={self.id} text={self.text[:30]!r}>"


class CallbackQuery:
    """Аналог telegram.CallbackQuery (событие message_event от VK)."""

    def __init__(self, bot, event_id, user_id, peer_id, data, message=None,
                 answer_sent=False):
        self._bot = bot
        self.id = event_id
        self.event_id = event_id
        self.from_user = User(user_id)
        self.user_id = user_id
        self.peer_id = peer_id
        self.data = data
        self.message = message
        self._answered = answer_sent

    async def answer(self, text=None, show_alert=False, cache_time=None, url=None,
                     **kwargs):
        """Показывает всплающее уведомление (VK: show_snackbar)."""
        if self._answered:
            return
        self._answered = True
        event_data = {}
        if text:
            # Лимит текста snackbar в VK ~90 символов.
            event_data = {"type": "show_snackbar", "text": str(text)[:90]}
        try:
            await self._bot._call(
                "messages.sendMessageEventAnswer",
                event_id=self.event_id,
                user_id=self.user_id,
                peer_id=self.peer_id,
                event_data=json.dumps(event_data, ensure_ascii=False),
            )
        except Exception as e:
            logger.debug(f"sendMessageEventAnswer failed: {e}")

    async def edit_message_text(self, text, reply_markup=None, parse_mode=None,
                                **kwargs):
        if self.message is None:
            raise BadRequest("Нет сообщения для редактирования")
        return await self._bot.edit_message_text(
            self.message.chat_id, self.message.id, text,
            reply_markup=reply_markup, parse_mode=parse_mode,
        )

    async def delete_message(self):
        if self.message is None:
            return
        return await self._bot.delete_message(self.message.chat_id, self.message.id)

    def __repr__(self):
        return f"<CallbackQuery data={self.data!r}>"


class ChatMember:
    """Аналог telegram.ChatMember для groups.isMember."""

    def __init__(self, status):
        self.status = status


class PreCheckoutQuery:
    """Заглушка для совместимости (в VK предчеков нет)."""

    def __init__(self, *args, **kwargs):
        self.invoice_payload = ""


class SuccessfulPayment:
    """Заглушка для совместимости."""

    def __init__(self, invoice_payload="", total_amount=0, currency="RUB"):
        self.invoice_payload = invoice_payload
        self.total_amount = total_amount
        self.currency = currency


class Update:
    """Аналог telegram.Update."""

    # Список типов апдейтов (для совместимости с run_polling(allowed_updates=...)).
    ALL_TYPES = ["message", "callback_query"]

    def __init__(self, message=None, callback_query=None):
        self.message = message
        self.edited_message = None
        self.callback_query = callback_query
        self.pre_checkout_query = None
        self.effective_message = message or (
            callback_query.message if callback_query else None
        )
        if message is not None and message.from_user is not None:
            self.effective_user = message.from_user
            self.effective_chat = Chat(message.chat_id)
        elif callback_query is not None:
            self.effective_user = callback_query.from_user
            self.effective_chat = Chat(callback_query.peer_id)
        else:
            self.effective_user = None
            self.effective_chat = None

    @property
    def effective_message_id(self):
        m = self.effective_message
        return m.id if m else None

    def __repr__(self):
        if self.message:
            return f"<Update message={self.message!r}>"
        return f"<Update callback={self.callback_query!r}>"


# ============================================================================
# === ФИЛЬТРЫ ===
# ============================================================================


class BaseFilter:
    def check(self, update) -> bool:
        raise NotImplementedError

    def __and__(self, other):
        return _AndFilter(self, other)

    def __or__(self, other):
        return _OrFilter(self, other)

    def __invert__(self):
        return _NotFilter(self)


class _AndFilter(BaseFilter):
    def __init__(self, a, b):
        self.a, self.b = a, b

    def check(self, update):
        return self.a.check(update) and self.b.check(update)


class _OrFilter(BaseFilter):
    def __init__(self, a, b):
        self.a, self.b = a, b

    def check(self, update):
        return self.a.check(update) or self.b.check(update)


class _NotFilter(BaseFilter):
    def __init__(self, f):
        self.f = f

    def check(self, update):
        return not self.f.check(update)


def _update_text(update) -> str:
    msg = update.effective_message
    if msg is None:
        return ""
    return msg.text or ""


class _TextFilter(BaseFilter):
    def check(self, update):
        msg = update.effective_message
        return bool(msg and (msg.text or "").strip())


class _CommandFilter(BaseFilter):
    def check(self, update):
        t = _update_text(update)
        return bool(t) and t.startswith("/")


class _PhotoFilter(BaseFilter):
    def check(self, update):
        msg = update.effective_message
        return bool(msg and msg.photo)


class _AllFilter(BaseFilter):
    def check(self, update):
        return True


class _SuccessfulPaymentFilter(BaseFilter):
    """В VK успешные платежи приходят не через апдейты сообщений."""

    def check(self, update):
        return False


class _UserFilter(BaseFilter):
    def __init__(self, user_id=None):
        self.user_id = user_id

    def check(self, update):
        if not self.user_id:
            return True
        u = update.effective_user
        return bool(u and int(u.id) == int(self.user_id))


class _ChatFilter(BaseFilter):
    def __init__(self, chat_id=None):
        self.chat_id = chat_id

    def check(self, update):
        if not self.chat_id:
            return True
        c = update.effective_chat
        return bool(c and int(c.id) == int(self.chat_id))


class _CallbackQueryFilter(BaseFilter):
    def check(self, update):
        return update.callback_query is not None


class _RegexFilter(BaseFilter):
    def __init__(self, pattern):
        if isinstance(pattern, str):
            self.rx = re.compile(pattern)
        else:
            self.rx = pattern

    def check(self, update):
        if update.callback_query is not None:
            data = update.callback_query.data or ""
            return bool(self.rx.search(data))
        t = _update_text(update)
        return bool(t) and bool(self.rx.search(t))


class _Filters:
    TEXT = _TextFilter()
    COMMAND = _CommandFilter()
    PHOTO = _PhotoFilter()
    ALL = _AllFilter()
    SUCCESSFUL_PAYMENT = _SuccessfulPaymentFilter()
    CALLBACK_QUERY = _CallbackQueryFilter()

    @staticmethod
    def Regex(pattern):
        return _RegexFilter(pattern)

    @staticmethod
    def User(user_id=None):
        return _UserFilter(user_id)

    @staticmethod
    def Chat(chat_id=None):
        return _ChatFilter(chat_id)


filters = _Filters()


# ============================================================================
# === ХЕНДЛЕРЫ ===
# ============================================================================


class BaseHandler:
    def check_update(self, update) -> bool:
        raise NotImplementedError

    async def handle_update(self, update, context):
        raise NotImplementedError


class CommandHandler(BaseHandler):
    """Команда вида /start или /start аргументы."""

    def __init__(self, command, callback, filters=None):
        self.command = command.lstrip("/").lower()
        self.callback = callback
        self.filters = filters

    def check_update(self, update) -> bool:
        msg = update.message
        if msg is None:
            return False
        t = (msg.text or "").strip()
        if not t.startswith("/"):
            return False
        parts = t.split(maxsplit=1)
        cmd = parts[0][1:].split("@")[0].lower()
        if cmd != self.command:
            return False
        if self.filters is not None and not self.filters.check(update):
            return False
        return True

    async def handle_update(self, update, context):
        t = (update.message.text or "").strip()
        parts = t.split(maxsplit=1)
        args = parts[1].split() if len(parts) > 1 else []
        # Поддержка VK deep-link: если /start пришёл без аргументов, но у
        # сообщения есть ref (из ссылки vk.com/write-<group>?ref=...).
        if not args and self.command == "start":
            ref = getattr(update.message, "ref", None)
            if ref:
                args = [str(ref)]
        context.args = args
        return await self.callback(update, context)


class MessageHandler(BaseHandler):
    def __init__(self, filters, callback):
        self.filters = filters
        self.callback = callback

    def check_update(self, update) -> bool:
        if update.message is None:
            return False
        return bool(self.filters and self.filters.check(update))

    async def handle_update(self, update, context):
        return await self.callback(update, context)


class CallbackQueryHandler(BaseHandler):
    def __init__(self, callback, pattern=None):
        self.callback = callback
        self.pattern = pattern
        if isinstance(pattern, str):
            self.rx = re.compile(pattern)
        else:
            self.rx = pattern

    def check_update(self, update) -> bool:
        cq = update.callback_query
        if cq is None:
            return False
        if self.rx is None:
            return True
        return bool(self.rx.search(cq.data or ""))

    async def handle_update(self, update, context):
        return await self.callback(update, context)


class PreCheckoutQueryHandler(BaseHandler):
    """Заглушка для совместимости: в VK предчеков нет, никогда не срабатывает."""

    def __init__(self, callback=None):
        self.callback = callback

    def check_update(self, update) -> bool:
        return False

    async def handle_update(self, update, context):
        return None


class ConversationHandler(BaseHandler):
    """Упрощённый аналог telegram.ext.ConversationHandler.

    Поддерживает entry_points / states / fallbacks / allow_reentry —
    как в оригинальном боте. Ключ разговора — chat_id (приватный диалог).
    """

    END = -1
    # PTB-совместимое имя
    TIMEOUT = -2

    def __init__(self, entry_points=None, states=None, fallbacks=None,
                 allow_reentry=False, per_chat=True, per_user=True,
                 per_message=False, conversation_timeout=None, name=None,
                 persistent=None, **kwargs):
        self.entry_points = entry_points or []
        self.states = states or {}
        self.fallbacks = fallbacks or []
        self.allow_reentry = allow_reentry
        self._conversations = {}  # chat_id -> state

    def check_update(self, update) -> bool:
        chat = update.effective_chat
        if chat is None:
            return False
        key = chat.id
        active = key in self._conversations
        if not active and not self._matches_any(update):
            return False
        return True

    def _matches_any(self, update) -> bool:
        for h in self.entry_points:
            if h.check_update(update):
                return True
        for h in self.fallbacks:
            if h.check_update(update):
                return True
        return False

    async def handle_update(self, update, context):
        chat = update.effective_chat
        key = chat.id
        new_state = None
        handled = False

        # 1) entry_points: если разговора нет или allow_reentry.
        active = key in self._conversations
        if not active or self.allow_reentry:
            for h in self.entry_points:
                if h.check_update(update):
                    if active and not self.allow_reentry:
                        break
                    new_state = await h.handle_update(update, context)
                    handled = True
                    break

        # 2) обработчики текущего состояния.
        if not handled and active:
            state = self._conversations.get(key)
            for h in self.states.get(state, []):
                if h.check_update(update):
                    new_state = await h.handle_update(update, context)
                    handled = True
                    break

        # 3) fallbacks.
        if not handled and active:
            for h in self.fallbacks:
                if h.check_update(update):
                    new_state = await h.handle_update(update, context)
                    handled = True
                    break

        if not handled:
            return None

        # Применяем новое состояние.
        if new_state is None:
            # Состояние не меняется (PTB-семантика: None = остаёмся).
            if not active and handled:
                # entry_point вернул None без состояния — не создаём разговор.
                self._conversations.pop(key, None)
            return None
        if new_state == ConversationHandler.END:
            self._conversations.pop(key, None)
        else:
            self._conversations[key] = new_state
        return new_state

    def get_state(self, chat_id):
        return self._conversations.get(chat_id)


# ============================================================================
# === JOBS / ПЛАНИРОВЩИК ===
# ============================================================================


class Job:
    def __init__(self, callback, name=None, data=None, job_queue=None,
                 next_run=None, interval=None):
        self.callback = callback
        self.name = name
        self.data = data
        self.job_queue = job_queue
        self.next_run = next_run
        self.interval = interval
        self.removed = False

    def schedule_removal(self):
        self.removed = True

    @property
    def is_removed(self):
        return self.removed

    def __repr__(self):
        return f"<Job {self.name!r} next={self.next_run}>"


class _JobContext:
    """Контекст для job-колбэков (аналог PTB CallbackContext)."""

    def __init__(self, bot, job, application):
        self.bot = bot
        self.job = job
        self.application = application
        self.error = None
        self.args = None
        self.user_data = {}
        self.chat_data = {}
        self.bot_data = application.bot_data if application else {}


class JobQueue:
    """Асинхронный планировщик с API как у PTB JobQueue (APScheduler)."""

    def __init__(self, application):
        self.application = application
        self._jobs = []
        self._task = None
        self._lock = asyncio.Lock()

    def run_once(self, callback, when, data=None, name=None, chat_id=None,
                 user_id=None, job_kwargs=None):
        if isinstance(when, datetime):
            next_run = when
        elif isinstance(when, (int, float)):
            next_run = datetime.utcnow() + timedelta(seconds=max(0.0, float(when)))
        else:
            next_run = when
        job = Job(callback, name=name, data=data, job_queue=self,
                  next_run=next_run, interval=None)
        self._jobs.append(job)
        return job

    def run_repeating(self, callback, interval, first=None, data=None, name=None,
                      chat_id=None, user_id=None, job_kwargs=None):
        if first is None:
            first = interval
        if isinstance(first, datetime):
            next_run = first
        else:
            next_run = datetime.utcnow() + timedelta(seconds=float(first or 0))
        job = Job(callback, name=name, data=data, job_queue=self,
                  next_run=next_run, interval=float(interval))
        self._jobs.append(job)
        return job

    def run_daily(self, callback, time, data=None, name=None, chat_id=None,
                  user_id=None, job_kwargs=None):
        """Ежедневный запуск в указанное UTC-время (time — datetime.time)."""
        now = datetime.utcnow()
        run_at = now.replace(hour=time.hour, minute=time.minute, second=0,
                             microsecond=0)
        if run_at <= now:
            run_at += timedelta(days=1)
        job = Job(callback, name=name, data=data, job_queue=self,
                  next_run=run_at, interval=86400.0)
        self._jobs.append(job)
        return job

    def jobs(self):
        return [j for j in self._jobs if not j.removed]

    def get_jobs_by_name(self, name):
        return [j for j in self._jobs if j.name == name and not j.removed]

    # PTB-стиль
    def get_jobs_by_name_legacy(self, name):
        return self.get_jobs_by_name(name)

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        while True:
            try:
                await asyncio.sleep(0.5)
                now = datetime.utcnow()
                for job in list(self._jobs):
                    if job.removed:
                        self._jobs.remove(job)
                        continue
                    if job.next_run is None or now < job.next_run:
                        continue
                    # Время сработать.
                    if job.interval is None:
                        job.removed = True
                    else:
                        # Двигаем next_run, пока он в прошлом (чтобы не копить
                        # пропущенные срабатывания после «зависания» процесса).
                        job.next_run = job.next_run + timedelta(seconds=job.interval)
                        while job.next_run <= now:
                            job.next_run += timedelta(seconds=job.interval)
                    ctx = _JobContext(
                        self.application.bot, job, self.application
                    )
                    try:
                        await job.callback(ctx)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.error(f"JobQueue job {job.name}: {e}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"JobQueue loop error: {e}")


# ============================================================================
# === КОНТЕКСТ ===
# ============================================================================


class CallbackContext:
    """Аналог telegram.ext.CallbackContext.DEFAULT_TYPE."""

    def __init__(self, bot, application, update=None, job=None, error=None,
                 args=None):
        self.bot = bot
        self.application = application
        self.job = job
        self.error = error
        self.args = args
        self._user_id = None
        if update is not None:
            if update.effective_user is not None:
                self._user_id = int(update.effective_user.id)
            if update.message is not None:
                self.chat_data = application.chat_data.setdefault(
                    update.effective_chat.id, {}
                ) if update.effective_chat else {}
            else:
                self.chat_data = {}
        else:
            self.chat_data = {}
        if self._user_id is not None:
            self.user_data = application.user_data.setdefault(self._user_id, {})
        else:
            self.user_data = {}

    @property
    def chat_data_dict(self):
        return self.chat_data


class ContextTypes:
    """Аналог telegram.ext.ContextTypes (DEFAULT_TYPE — для аннотаций)."""

    DEFAULT_TYPE = CallbackContext


# ============================================================================
# === VK API КЛИЕНТ И БОТ ===
# ============================================================================


class Bot:
    """VK-клиент с методами, совместимыми с telegram.Bot."""

    def __init__(self, token=None, group_id=None):
        self._token = token or os.environ.get("VK_TOKEN") or ""
        self._group_id = group_id or os.environ.get("VK_GROUP_ID") or ""
        self._session = None
        self._call_lock = asyncio.Lock()
        self._last_call_ts = 0.0
        self._me = None
        self._users_cache = {}
        # Реестры для редактирования сообщений:
        #   _msg_kind[(peer, id)] = 'conv' | 'real'
        #   _remap[(peer, id)]   = новый реальный id после delete+resend
        self._msg_kind = {}
        self._remap = {}
        # Продолжения инлайн-клавиатур (сообщения с «хвостом» кнопок):
        self._continuations = {}
        self._lp_server = None
        self._lp_key = None
        self._lp_ts = None
        self._running = False
        self._event_callback = None

    # ------------------------------------------------------------------ init
    async def initialize(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT)
            )
        try:
            me = await self._call("groups.getById",
                                  group_id=self._group_id,
                                  fields="screen_name")
            groups = me.get("groups") or me
            if isinstance(groups, list) and groups:
                g = groups[0]
                self._me = User(
                    id=-int(g.get("id", 0)),
                    first_name=g.get("name", "Bot"),
                    username=g.get("screen_name", ""),
                )
            logger.info(f"VK Bot: сообщество {self._me and self._me.username}")
        except Exception as e:
            logger.warning(f"groups.getById не удался (не критично): {e}")
            self._me = User(id=-int(self._group_id or 0), first_name="Bot")

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------- транспорт
    async def _call(self, method, _retries=5, **params):
        """Вызов VK API с троттлингом и ретраями на flood control."""
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT)
            )
        params = {k: v for k, v in params.items() if v is not None}
        if "v" not in params:
            params["v"] = VK_API_VERSION
        params["access_token"] = self._token
        url = VK_API_BASE + method
        for attempt in range(_retries):
            async with self._call_lock:
                now = time.monotonic()
                delta = now - self._last_call_ts
                if delta < _MIN_CALL_INTERVAL:
                    await asyncio.sleep(_MIN_CALL_INTERVAL - delta)
                self._last_call_ts = time.monotonic()
            try:
                async with self._session.post(url, data=params) as resp:
                    data = await resp.json(content_type=None)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt >= _retries - 1:
                    raise NetworkError(f"{method}: {e}")
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            if "error" in data:
                err = data["error"]
                code = err.get("error_code", 0)
                msg = err.get("error_msg", "")
                if code == 6:  # too many requests
                    await asyncio.sleep(0.4 * (attempt + 1))
                    continue
                if code in (900, 901, 1021, 902):
                    raise Forbidden(f"{method}: {msg}")
                if code == 100:
                    raise BadRequest(f"{method}: {msg}")
                raise VkApiError(code, f"{method}: {msg}")
            return data.get("response", data)
        raise NetworkError(f"{method}: превышено число попыток")

    async def _download_bytes(self, url, timeout=30):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT)
            )
        async with self._session.get(
            url, timeout=aiohttp.ClientTimeout(total=timeout)
        ) as resp:
            resp.raise_for_status()
            return await resp.read()

    # ------------------------------------------------------------------ info
    async def get_me(self):
        if self._me is None:
            await self.initialize()
        return self._me

    @property
    def username(self):
        return self._me.username if self._me else ""

    async def get_users(self, user_ids):
        """users.get с кэшем (для имён отправителей)."""
        need = []
        cached = {}
        for uid in user_ids:
            uid = int(uid)
            hit = self._users_cache.get(uid)
            if hit and time.time() - hit[1] < 3600:
                cached[uid] = hit[0]
            else:
                need.append(uid)
        if need:
            try:
                resp = await self._call(
                    "users.get", user_ids=",".join(str(u) for u in need),
                    fields="screen_name",
                )
                for u in resp:
                    uid = int(u.get("id", 0))
                    user = User(
                        uid, u.get("first_name", ""), u.get("last_name", ""),
                        u.get("screen_name", "") or "",
                    )
                    self._users_cache[uid] = (user, time.time())
                    cached[uid] = user
            except Exception as e:
                logger.warning(f"users.get({need}) не удался: {e}")
        return [cached.get(int(u)) or User(int(u)) for u in user_ids]

    async def get_user_info(self, user_id):
        res = await self.get_users([user_id])
        return res[0] if res else User(user_id)

    # --------------------------------------------------------------- подписка
    async def is_member(self, group_id, user_id):
        """groups.isMember → bool."""
        try:
            resp = await self._call(
                "groups.isMember", group_id=int(group_id), user_id=int(user_id)
            )
            if isinstance(resp, dict):
                return bool(resp.get("is_member"))
            return bool(resp)
        except VkApiError as e:
            logger.warning(f"isMember({group_id},{user_id}): {e}")
            raise
        except (Forbidden, BadRequest) as e:
            logger.warning(f"isMember({group_id},{user_id}): {e}")
            raise

    async def get_chat_member(self, chat_id, user_id):
        """Совместимость с PTB: проверка членства в группе/канале."""
        gid = None
        try:
            gid = int(str(chat_id).lstrip("@").lstrip("club").lstrip("-"))
        except Exception:
            gid = int(self._group_id or 0)
        if not gid:
            return ChatMember("member")
        try:
            ok = await self.is_member(gid, user_id)
            return ChatMember("member" if ok else "left")
        except Exception:
            # Проверить нельзя (приватность/нет прав) — считаем «не знаю».
            return ChatMember(None)

    # ------------------------------------------------------------ сообщения
    async def send_message(self, chat_id, text, reply_markup=None,
                           parse_mode=None, disable_web_page_preview=None,
                           reply_to_message_id=None, **kwargs):
        """messages.send (+авто-разбиение больших инлайн-клавиатур)."""
        text = vk_text(text, parse_mode)
        keyboards = None
        if reply_markup is not None:
            if isinstance(reply_markup, InlineKeyboardMarkup):
                keyboards = reply_markup.to_vk_chunks()
            elif isinstance(reply_markup, ReplyKeyboardMarkup):
                keyboards = [reply_markup.to_vk()]
            elif isinstance(reply_markup, ReplyKeyboardRemove):
                keyboards = [reply_markup.to_vk()]
        random_id = random.getrandbits(63) * (1 if random.random() < 0.5 else -1)
        kb = keyboards[0] if keyboards else None
        message_id = await self._send_raw(
            chat_id, text, kb, random_id, reply_to=reply_to_message_id
        )
        # Хвост инлайн-клавиатуры — отдельными сообщениями-продолжениями.
        extra_ids = []
        if keyboards and len(keyboards) > 1:
            for i, kb_part in enumerate(keyboards[1:], start=2):
                rid = random.getrandbits(63)
                try:
                    mid = await self._send_raw(
                        chat_id, f"↕️ Кнопки (часть {i})", kb_part, rid
                    )
                    extra_ids.append(mid)
                except Exception as e:
                    logger.warning(f"Не удалось отправить продолжение клавиатуры: {e}")
            self._continuations[(int(chat_id), message_id)] = extra_ids
        # Первое сообщение возвращаем как Message.
        user = None
        try:
            peer_uid = int(chat_id)
            user = User(peer_uid) if peer_uid > 0 else None
        except Exception:
            pass
        msg = Message(self, int(chat_id), message_id=message_id, text=text,
                      from_user=user)
        self._msg_kind[(int(chat_id), message_id)] = "real"
        return msg

    async def _send_raw(self, chat_id, text, kb_json, random_id, reply_to=None):
        params = {
            "peer_id": int(chat_id),
            "message": (text or "").strip() or " ",
            "random_id": random_id,
        }
        if kb_json:
            params["keyboard"] = kb_json
        if reply_to:
            params["reply_to"] = int(reply_to)
        resp = await self._call("messages.send", **params)
        # VK возвращает id сообщения (int) или список.
        if isinstance(resp, list):
            return int(resp[0]) if resp else 0
        return int(resp)

    async def edit_message_text(self, chat_id, message_id, text,
                                reply_markup=None, parse_mode=None, **kwargs):
        """messages.edit (+фолбэк: удалить и прислать заново)."""
        text = vk_text(text, parse_mode)
        peer = int(chat_id)
        target = int(message_id)
        # После фолбэка «delete+resend» реальный id мог измениться.
        target = self._remap.get((peer, target), target)
        kind = self._msg_kind.get((peer, target), "conv")
        kb_json = None
        if reply_markup is not None:
            if isinstance(reply_markup, InlineKeyboardMarkup):
                kb_json = reply_markup.to_vk()
            elif isinstance(reply_markup, (ReplyKeyboardMarkup, ReplyKeyboardRemove)):
                kb_json = reply_markup.to_vk()
        # Удаляем старые продолжения клавиатуры — их перешлём заново.
        old_conts = self._continuations.pop((peer, target), [])
        params = {
            "peer_id": peer,
            "message": (text or "").strip() or " ",
            "keyboard": kb_json,
        }
        if kind == "real":
            params["message_id"] = target
        else:
            params["conversation_message_id"] = target
        try:
            await self._call("messages.edit", **params)
            message_id_out = target
        except (VkApiError, BadRequest, Forbidden) as e:
            # Фолбэк: удаляем исходное сообщение и присылаем новое.
            logger.debug(f"edit_message_text fallback ({e}) — resend")
            try:
                await self._delete_raw(peer, target, kind)
            except Exception:
                pass
            sent = await self.send_message(
                chat_id, text, reply_markup=reply_markup
            )
            self._remap[(peer, target)] = sent.message_id
            return sent
        # Пересылаем продолжения клавиатуры (если были).
        if kb_json and reply_markup is not None and isinstance(
            reply_markup, InlineKeyboardMarkup
        ):
            chunks = reply_markup.to_vk_chunks()
            if len(chunks) > 1:
                extra_ids = []
                for i, kb_part in enumerate(chunks[1:], start=2):
                    try:
                        rid = random.getrandbits(63)
                        mid = await self._send_raw(
                            peer, f"↕️ Кнопки (часть {i})", kb_part, rid
                        )
                        extra_ids.append(mid)
                    except Exception as e:
                        logger.warning(f"edit: продолжение клавиатуры: {e}")
                self._continuations[(peer, target)] = extra_ids
        # Удаляем старые продолжения, которых нет в новом наборе.
        for mid in old_conts:
            try:
                await self._delete_raw(peer, mid, "real")
            except Exception:
                pass
        return Message(self, peer, message_id=message_id_out, text=text)

    async def delete_message(self, chat_id, message_id):
        peer = int(chat_id)
        target = self._remap.get((peer, int(message_id)), int(message_id))
        kind = self._msg_kind.get((peer, target), "conv")
        conts = self._continuations.pop((peer, target), [])
        ok = True
        try:
            await self._delete_raw(peer, target, kind)
        except Exception as e:
            logger.debug(f"delete_message({peer},{target}): {e}")
            ok = False
        for mid in conts:
            try:
                await self._delete_raw(peer, mid, "real")
            except Exception:
                pass
        return ok

    async def _delete_raw(self, peer, mid, kind):
        if kind == "real":
            await self._call(
                "messages.delete", peer_id=peer, message_ids=int(mid),
                delete_for_all=1,
            )
        else:
            await self._call(
                "messages.delete", peer_id=peer, cmids=int(mid),
                delete_for_all=1,
            )

    async def send_chat_action(self, chat_id, action="typing", **kwargs):
        """messages.setActivity — индикатор «печатает…»."""
        try:
            await self._call(
                "messages.setActivity", peer_id=int(chat_id), type="typing"
            )
        except Exception as e:
            logger.debug(f"setActivity: {e}")

    async def send_invoice(self, *args, **kwargs):
        """В VK встроенных инвойсов нет — оплата идёт через QR/ссылку."""
        raise BadRequest(
            "Инвойсы не поддерживаются в VK — используйте платёжный модуль "
            "самозанятого (QR-код / ссылка)."
        )

    # ----------------------------------------------------------- фотографии
    async def send_photo(self, chat_id, photo, caption=None, parse_mode=None,
                         reply_markup=None, **kwargs):
        """Отправляет фото (bytes/путь/URL) + подпись."""
        data = None
        filename = "photo.jpg"
        if isinstance(photo, (bytes, bytearray)):
            data = bytes(photo)
        elif isinstance(photo, io.BytesIO):
            photo.seek(0)
            data = photo.read()
        elif isinstance(photo, str) and photo.startswith("http"):
            data = await self._download_bytes(photo)
        elif isinstance(photo, str) and os.path.exists(photo):
            with open(photo, "rb") as f:
                data = f.read()
            filename = os.path.basename(photo)
        elif isinstance(photo, str):
            data = photo.encode("utf-8")
        if data is None:
            raise BadRequest("send_photo: неподдерживаемый источник фото")
        attachment = await self.upload_photo(chat_id, data, filename)
        caption = vk_text(caption, parse_mode)
        kb_json = None
        if reply_markup is not None:
            if isinstance(reply_markup, InlineKeyboardMarkup):
                kb_json = reply_markup.to_vk()
            elif isinstance(reply_markup, (ReplyKeyboardMarkup, ReplyKeyboardRemove)):
                kb_json = reply_markup.to_vk()
        random_id = random.getrandbits(63)
        resp = await self._call(
            "messages.send", peer_id=int(chat_id),
            message=(caption or "").strip() or " ",
            attachment=attachment, random_id=random_id, keyboard=kb_json,
        )
        message_id = int(resp) if not isinstance(resp, list) else int(resp[0])
        self._msg_kind[(int(chat_id), message_id)] = "real"
        return Message(self, int(chat_id), message_id=message_id, text=caption)

    async def upload_photo(self, peer_id, data, filename="photo.jpg"):
        """Загрузка фото в сообщения VK: сервер → upload → save."""
        upload = await self._call(
            "photos.getMessagesUploadServer", peer_id=int(peer_id)
        )
        upload_url = upload.get("upload_url")
        if not upload_url:
            raise BadRequest("getMessagesUploadServer: нет upload_url")
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=_API_TIMEOUT)
            )
        form = aiohttp.FormData()
        form.add_field(
            "photo", data, filename=filename,
            content_type="image/jpeg",
        )
        async with self._session.post(upload_url, data=form) as resp:
            up = await resp.json(content_type=None)
        saved = await self._call(
            "photos.saveMessagesPhoto",
            server=up.get("server"), photo=up.get("photo"), hash=up.get("hash"),
            group_id=int(self._group_id) if self._group_id else None,
        )
        if isinstance(saved, list) and saved:
            p = saved[0]
            owner = p.get("owner_id", 0)
            pid = p.get("id", 0)
            key = p.get("access_key", "")
            att = f"photo{owner}_{pid}"
            if key:
                att += f"_{key}"
            return att
        raise BadRequest("saveMessagesPhoto: пустой ответ")

    async def get_file(self, file_id, **kwargs):
        """file_id у нас — прямой URL; возвращаем File с методом скачивания."""
        return File(file_id, bot=self)

    # ---------------------------------------------------------- long polling
    async def start_longpoll(self, event_callback):
        """Запускает бесконечный цикл Bots Long Poll VK."""
        self._event_callback = event_callback
        self._running = True
        while self._running:
            try:
                if not self._lp_server or not self._lp_key:
                    await self._fetch_lp_server()
                updates = await self._poll_once()
                if updates is None:
                    continue
                for upd in updates:
                    try:
                        await self._event_callback(upd.get("type"), upd.get("object"))
                    except Exception as e:
                        logger.error(f"Диспетчер события {upd.get('type')}: {e}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Long poll error: {e}")
                await asyncio.sleep(3)

    def stop(self):
        self._running = False

    async def _fetch_lp_server(self):
        resp = await self._call(
            "groups.getLongPollServer",
            group_id=int(self._group_id) if self._group_id else None,
        )
        self._lp_server = resp.get("server")
        self._lp_key = resp.get("key")
        self._lp_ts = resp.get("ts")
        logger.info("VK Long Poll сервер получен.")

    async def _poll_once(self):
        url = (
            f"{self._lp_server}?act=a_check&key={self._lp_key}"
            f"&ts={self._lp_ts}&wait=25&mode=2&version=3"
        )
        async with self._session.get(
            url, timeout=aiohttp.ClientTimeout(total=35)
        ) as resp:
            data = await resp.json(content_type=None)
        if data.get("failed") is not None:
            failed = data.get("failed")
            logger.warning(f"VK Long Poll failed={failed}")
            if failed == 1:
                # История устарела — обновляем ts.
                self._lp_ts = data.get("ts") or self._lp_ts
                return []
            # failed 2/3 — пересоздаём сессию.
            self._lp_server = None
            self._lp_key = None
            await asyncio.sleep(1)
            return None
        self._lp_ts = data.get("ts", self._lp_ts)
        return data.get("updates", [])

    async def answer_callback_query(self, callback_query_id, text=None,
                                    show_alert=False, **kwargs):
        """Совместимость с PTB (в основном используется query.answer())."""
        try:
            await self._call(
                "messages.sendMessageEventAnswer",
                event_id=str(callback_query_id),
                user_id=0, peer_id=0,
                event_data=json.dumps(
                    {"type": "show_snackbar", "text": (text or "")[:90]},
                    ensure_ascii=False,
                ),
            )
        except Exception:
            pass


# ============================================================================
# === APPLICATION (диспетчер) ===
# ============================================================================


class Application:
    """Аналог telegram.ext.Application: хендлеры + JobQueue + Long Poll."""

    def __init__(self, bot):
        self.bot = bot
        self._handlers = []        # список BaseHandler в порядке добавления
        self._error_handlers = []
        self._post_init_hook = None
        self._post_shutdown_hook = None
        self._running = False
        # Хранилища контекста (как в PTB, в памяти процесса):
        self.bot_data = {}
        self.user_data = {}
        self.chat_data = {}
        self.job_queue = JobQueue(self)
        # Семафор параллельной обработки апдейтов.
        self._update_semaphore = asyncio.Semaphore(16)

    # ------------------------------------------------------------ builder API
    @classmethod
    def builder(cls):
        return ApplicationBuilder(cls)

    def add_handler(self, handler, group=0):
        self._handlers.append(handler)

    def add_error_handler(self, callback):
        self._error_handlers.append(callback)

    # ------------------------------------------------------------- run_polling
    async def run_polling_async(self, drop_pending_updates=False, **kwargs):
        self._running = True
        await self.bot.initialize()
        await self.job_queue.start()
        if self._post_init_hook:
            try:
                await self._post_init_hook(self)
            except Exception as e:
                logger.error(f"post_init error: {e}")
        logger.info("VK-бот запущен (Long Poll).")
        try:
            await self.bot.start_longpoll(self._on_vk_event)
        finally:
            if self._post_shutdown_hook:
                try:
                    await self._post_shutdown_hook(self)
                except Exception:
                    pass
            await self.bot.close()

    def run_polling(self, allowed_updates=None, drop_pending_updates=False,
                    poll_interval=0.0, timeout=30, **kwargs):
        try:
            asyncio.run(self.run_polling_async(
                drop_pending_updates=drop_pending_updates
            ))
        except KeyboardInterrupt:
            logger.info("Остановка по Ctrl+C.")
        finally:
            self._running = False

    # Точки жизненного цикла.
    def post_init(self, callback):
        self._post_init_hook = callback

    # ------------------------------------------------------------- обработка
    async def _on_vk_event(self, event_type, obj):
        """Превращает событие VK в Update и прогоняет через хендлеры."""
        if event_type == "message_new":
            update = await self._mk_update_message(obj.get("message") or {})
            if update is None:
                return
        elif event_type == "message_event":
            update = await self._mk_update_callback(obj or {})
            if update is None:
                return
        elif event_type in ("message_allow", "message_deny", "message_typing_state"):
            return
        else:
            logger.debug(f"VK event {event_type} пропущен.")
            return
        # Параллельная обработка апдейтов (concurrent_updates=True).
        asyncio.create_task(self._process_update_safe(update))

    async def _process_update_safe(self, update):
        async with self._update_semaphore:
            try:
                await self._process_update(update)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"process_update {update!r}: {e}", exc_info=True)

    async def _process_update(self, update):
        # Автоподтверждение callback-события, если хендлер забыл answer().
        cq = update.callback_query
        try:
            for handler in self._handlers:
                try:
                    if not handler.check_update(update):
                        continue
                except Exception as e:
                    logger.error(f"check_update {handler!r}: {e}")
                    continue
                context = CallbackContext(
                    self.bot, self, update=update
                )
                try:
                    await handler.handle_update(update, context)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    await self._dispatch_error(update, e)
                break
            else:
                # Ни один хендлер не подошёл — тихо игнорируем.
                pass
        finally:
            if cq is not None and not cq._answered:
                try:
                    await cq.answer()
                except Exception:
                    pass

    async def _dispatch_error(self, update, error):
        logger.error(f"Ошибка хендлера: {error}", exc_info=error)
        if not self._error_handlers:
            return
        ctx = CallbackContext(self.bot, self, update=update)
        ctx.error = error
        for cb in self._error_handlers:
            try:
                await cb(update, ctx)
            except Exception as e:
                logger.error(f"error_handler сам упал: {e}")

    # -------------------------------------------------- конструирование Update
    async def _mk_update_message(self, msg_raw):
        """message_new → Update(Update.message)."""
        try:
            peer_id = int(msg_raw.get("peer_id", 0))
            from_id = int(msg_raw.get("from_id", 0))
            # Игнорируем собственные исходящие события.
            if msg_raw.get("out"):
                return None
            # В групповых беседах peer_id != from_id — бот рассчитан на личку.
            if peer_id != from_id and peer_id > 2000000000:
                # Беседа: вежливо отказываем один раз.
                try:
                    await self.bot.send_message(
                        peer_id,
                        "👋 Бот работает только в личных сообщениях. "
                        "Напишите мне в личку!",
                    )
                except Exception:
                    pass
                return None
            user = None
            if from_id > 0:
                user = await self.bot.get_user_info(from_id)
            else:
                user = await self.bot.get_me()
            text = msg_raw.get("text") or ""
            # VK deep-link: если пользователь перешёл по ссылке вида
            # https://vk.com/write-<group>?ref=<code>, то В ПЕРВОМ сообщении
            # приходит поле ref. Конвертируем его в «/start <ref>» — так же,
            # как Telegram передаёт deep-link payload, и вся реферальная
            # логика main.py работает без изменений.
            ref = msg_raw.get("ref")
            if ref:
                ref = str(ref)
                if not text.strip().startswith("/start"):
                    text = f"/start {ref}"
                else:
                    # /start уже есть — дописываем ref аргументом, если его нет.
                    parts = text.split(maxsplit=1)
                    if len(parts) < 2:
                        text = f"/start {ref}"
            photo_sizes = []
            for att in msg_raw.get("attachments", []) or []:
                if isinstance(att, dict) and att.get("type") == "photo":
                    photo_sizes += _photo_sizes_from_vk(att.get("photo") or {})
            cmid = msg_raw.get("conversation_message_id")
            mid = msg_raw.get("id")
            msg = Message(
                self.bot, peer_id, message_id=mid, text=text, from_user=user,
                date=datetime.utcfromtimestamp(msg_raw.get("date", 0)),
                photo=photo_sizes, conversation_message_id=cmid,
                caption=msg_raw.get("caption"),
                attachments=msg_raw.get("attachments", []) or [],
                ref=msg_raw.get("ref"),
                payload=msg_raw.get("payload"),
            )
            if mid:
                self.bot._msg_kind[(peer_id, mid)] = "real"
            if cmid:
                self.bot._msg_kind[(peer_id, cmid)] = "conv"
            return Update(message=msg)
        except Exception as e:
            logger.error(f"mk_update_message: {e}")
            return None

    async def _mk_update_callback(self, obj):
        """message_event → Update(Update.callback_query)."""
        try:
            event_id = str(obj.get("event_id", ""))
            user_id = int(obj.get("user_id", 0))
            peer_id = int(obj.get("peer_id", user_id))
            raw_payload = obj.get("payload")
            data = ""
            if isinstance(raw_payload, str):
                try:
                    parsed = json.loads(raw_payload)
                    data = parsed.get("c", "") if isinstance(parsed, dict) else str(parsed)
                except Exception:
                    data = raw_payload
            elif isinstance(raw_payload, dict):
                data = raw_payload.get("c", "") or raw_payload.get("data", "")
            else:
                data = str(raw_payload or "")
            cmid = obj.get("conversation_message_id")
            # Сообщение-носитель кнопки (для edit_message_text).
            user = await self.bot.get_user_info(user_id) if user_id > 0 else None
            message = Message(
                self.bot, peer_id, message_id=cmid, text="",
                from_user=user, conversation_message_id=cmid,
            )
            if cmid:
                self.bot._msg_kind[(peer_id, cmid)] = "conv"
            cq = CallbackQuery(
                self.bot, event_id, user_id, peer_id, data, message=message
            )
            cq.from_user = user or User(user_id)
            return Update(callback_query=cq)
        except Exception as e:
            logger.error(f"mk_update_callback: {e}")
            return None

    # ------------------------------------------------------------- совместимость
    def create_task(self, coro):
        return asyncio.create_task(coro)

    async def initialize(self):
        await self.bot.initialize()

    async def shutdown(self):
        self.bot.stop()
        await self.bot.close()


class ApplicationBuilder:
    """Аналог telegram.ext.ApplicationBuilder (fluent-интерфейс)."""

    def __init__(self, app_cls=Application):
        self._app_cls = app_cls
        self._token = None
        self._group_id = None
        self._post_init = None
        self._concurrent = False
        self._request = None
        self._get_updates_request = None

    def token(self, token):
        self._token = token
        return self

    def bot_token(self, token):
        return self.token(token)

    def post_init(self, callback):
        self._post_init = callback
        return self

    def concurrent_updates(self, value=True):
        self._concurrent = value
        return self

    def request(self, value):
        # Совместимость: параметры HTTP-пула PTB игнорируются (свой пул aiohttp).
        return self

    def get_updates_request(self, value):
        return self

    def job_queue(self, value=None):
        return self

    def build(self):
        bot = Bot(token=self._token)
        app = self._app_cls(bot)
        if self._post_init:
            app.post_init(self._post_init)
        return app


# ============================================================================
# === ПЛАТЕЖИ (самозанятость): Т-Банк + «Мой налог» ===
# ============================================================================
# Оплата строится так: пользователь переводит деньги по QR-коду или ссылке
# самозанятого (Т-Банк «Самозанятые» / статичная ссылка из «Мой налог»),
# в комментарии к переводу указывает сгенерированный код платежа PAY-XXXX.
# Подтверждение либо автоматическое (Т-Банк API, если настроен), либо
# вручную разработчиком в панели. Чек выдаёт «Мой налог» (НПД) — бот
# отправляет пользователю квитанцию и ссылку на чек npd.nalog.ru.

TBANK_API_BASE = os.environ.get("TBANK_API_BASE", "https://openapi.tbank.ru")
TBANK_API_TOKEN = os.environ.get("TBANK_API_TOKEN", "")


class TBankSelfEmployedClient:
    """Клиент API Т-Банка для самозанятых (создание ссылок/чеков).

    Работает, если задан TBANK_API_TOKEN (токен из личного кабинета
    Т-Банка, раздел «Самозанятые» → API). Если токена нет — бот работает
    в ручном режиме (статичная ссылка/QR + код платежа).
    """

    def __init__(self, token=None, base=None):
        self.token = token or TBANK_API_TOKEN
        self.base = (base or TBANK_API_BASE).rstrip("/")

    @property
    def enabled(self):
        return bool(self.token)

    async def _post(self, path, payload):
        if not self.enabled:
            raise BadRequest("Т-Банк API не настроен (нет TBANK_API_TOKEN)")
        url = f"{self.base}{path}"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20)
        ) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                text = await resp.text()
                try:
                    return resp.status, json.loads(text)
                except Exception:
                    return resp.status, {"raw": text}

    async def create_payment_link(self, amount_rub, payment_code, description=""):
        """Создаёт платёжную ссылку для самозанятого.

        Возвращает dict: {ok, url, receipt_url?}. Эндпоинт соответствует
        OpenAPI Т-Банка «Самозанятые» (актуальный путь можно переопределить
        переменной окружения TBANK_LINK_PATH, по умолчанию
        /self-employed/api/v1/payment-link).
        """
        path = os.environ.get(
            "TBANK_LINK_PATH", "/self-employed/api/v1/payment-link"
        )
        status, body = await self._post(path, {
            "amount": int(amount_rub * 100),  # в копейках
            "comment": payment_code,
            "description": description or "Пополнение баланса бота",
        })
        if status // 100 == 2:
            url = (body or {}).get("link") or (body or {}).get("url") or \
                  (body or {}).get("paymentLink") or ""
            receipt = (body or {}).get("receiptUrl") or (body or {}).get("receipt_url") or ""
            return {"ok": bool(url), "url": url, "receipt_url": receipt}
        logger.warning(f"TBank create_payment_link: HTTP {status}: {body}")
        return {"ok": False, "url": "", "receipt_url": ""}

    async def check_payment(self, payment_code):
        """Проверяет статус оплаты по коду платежа.

        Возвращает dict: {paid: bool, receipt_url: str}.
        Эндпоинт: /self-employed/api/v1/payment-status (настраивается
        переменной окружения TBANK_STATUS_PATH).
        """
        path = os.environ.get(
            "TBANK_STATUS_PATH", "/self-employed/api/v1/payment-status"
        )
        status, body = await self._post(path, {"comment": payment_code})
        if status // 100 == 2:
            b = body or {}
            paid = bool(b.get("paid") or b.get("status") in ("PAID", "paid", "CONFIRMED"))
            receipt = b.get("receiptUrl") or b.get("receipt_url") or ""
            return {"paid": paid, "receipt_url": receipt}
        return {"paid": False, "receipt_url": ""}


def npd_receipt_url(receipt_date: str, receipt_number: str) -> str:
    """Ссылка на проверку чека НПД («Мой налог»).

    Формат: https://npd.nalog.ru/check/YYYYMMDDTHHMM/<номер чека>
    receipt_date — «YYYY-MM-DDTHH:MM» или «YYYY-MM-DD HH:MM» либо полная
    дата чека из «Мой налог».
    """
    if not receipt_number:
        return ""
    d = (receipt_date or "").strip().replace(" ", "T").replace("-", "").replace(":", "")
    # Ожидаем вид YYYYMMDDTHHMM.
    d = re.sub(r"^(\d{8})T?(\d{0,4}).*$", r"\1T\2", d)
    number = str(receipt_number).strip()
    return f"https://npd.nalog.ru/check/{d}/{number}"


def qr_png_bytes(data: str, box_size=8, border=2):
    """Генерирует PNG QR-кода (для кнопки оплаты). qrcode + Pillow."""
    import qrcode
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=box_size,
        border=border,
    )
    qr.add_data(data)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf.getvalue()
