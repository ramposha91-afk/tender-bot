"""
Telegram-бот мониторинга тендеров по металлолому
Источник данных: Tenderplan API
Запуск на Render: Background Worker
Start command: python main.py

ENV:
- BOT_TOKEN
- TENDERPLAN_TOKEN
"""

import asyncio
import csv
import html
import io
import logging
import os
import re
from datetime import datetime
from typing import Any, Optional

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# =============================================================================
# НАСТРОЙКИ
# =============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TENDERPLAN_TOKEN = os.getenv("TENDERPLAN_TOKEN", "").strip()

DB_PATH = os.getenv("DB_PATH", "tenders.db")
UPDATE_INTERVAL_MINUTES = int(os.getenv("UPDATE_INTERVAL_MINUTES", "30"))

TENDERPLAN_API = "https://tenderplan.ru/api"

REQUEST_TIMEOUT = 40
REQUEST_DELAY = 1.2
MAX_RETRIES = 3

# Точные поисковые фразы — каждая должна целиком встречаться в названии
SEARCH_TERMS: list[str] = [
    "металлолом",
    "лом черных металлов",
    "лом цветных металлов",
    "прием металлолома",
    "реализация металлолома",
    "вывоз металлолома",
    "стружка металлическая",
    "отходы черных металлов",
    "отходы цветных металлов",
    "демонтаж металлоконструкций",
    "лом чермет",
    "лом цветмет",
]

# Мусор
FORBIDDEN_PATTERNS: list[str] = [
    r"канцеляр",
    r"канцтовар",
    r"бумаг[аиу]",
    r"молок",
    r"мяс",
    r"рыб",
    r"овощ",
    r"фрукт",
    r"продукт[ыа] питан",
    r"хлеб",
    r"кондитер",
    r"медицин",
    r"лекарств",
    r"фармацевт",
    r"одежд",
    r"обув",
    r"мебел",
    r"посуд",
    r"хозтовар",
    r"хозяйствен",
    r"картридж",
    r"тонер",
    r"швабр",
    r"моющ",
    r"дезинфиц",
    r"ремонт дорог",
    r"ремонт помещ",
    r"ремонт здан",
    r"благоустройств",
    r"асфальт",
    r"услуги охран",
    r"страхован",
    r"сигнализац",
    r"клапан",
    r"шнур",
]

# Паттерны
GOOD_PATTERNS: list[str] = [
    r"металлолом",
    r"\bлом\s+(черн|цветн|алюмини|мед|латун|чермет|цветмет|нержав|чугун|стал)",
    r"(черн|цветн).{0,8}металлолом",
    r"(прием|приём|реализац|вывоз|закупк|сдач).{0,15}лом",
    r"лом.{0,10}(черн|цветн|металл)",
    r"струж.{0,20}(металл|алюмин|медн|латун)",
    r"отход.{0,20}(черн|цветн).{0,10}металл",
    r"металлоконструкц.{0,20}(демонтаж|вывоз|утилиз)",
    r"(демонтаж|вывоз).{0,20}металлоконструкц",
    r"чермет",
    r"цветмет",
]


# =============================================================================
# ЛОГИ
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

_update_lock = asyncio.Lock()


# =============================================================================
# УТИЛИТЫ
# =============================================================================

def clean_html(text: Any) -> str:
    if text is None:
        return ""
    text = str(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return " ".join(text.split()).strip()


def normalize_text(text: str) -> str:
    return clean_html(text).lower().replace("ё", "е")


def is_relevant_tender(title: str, keyword: str = "") -> bool:
    """
    Мягкий фильтр:
    - учитывает и название тендера, и поисковую фразу;
    - режет явный мусор;
    - пропускает всё, где есть признаки металлолома/металла/стружки/отходов металла.
    """
    text = normalize_text(f"{title} {keyword}")

    if not text.strip():
        return False

    if any(re.search(p, text) for p in FORBIDDEN_PATTERNS):
        return False

    return any(re.search(p, text) for p in GOOD_PATTERNS)


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_price(price: Any) -> str:
    price = safe_float(price)
    if price is None:
        return "не указана"
    if price >= 1_000_000_000:
        return f"{price / 1_000_000_000:.2f} млрд ₽"
    if price >= 1_000_000:
        return f"{price / 1_000_000:.2f} млн ₽"
    if price >= 1_000:
        return f"{price / 1_000:.1f} тыс. ₽"
    return f"{price:.2f} ₽"


def fmt_ts_ms(ts_ms: Any) -> Optional[str]:
    if not ts_ms:
        return None
    try:
        ts = int(ts_ms)
        # Tenderplan обычно отдаёт миллисекунды.
        if ts > 10_000_000_000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return None


def status_name(status_code: Any) -> str:
    mapping = {
        1: "Активен",
        2: "На рассмотрении",
        3: "Завершён",
        4: "Отменён",
        5: "Не состоялся",
    }
    try:
        return mapping.get(int(status_code), str(status_code or "не указан"))
    except Exception:
        return str(status_code or "не указан")


def escape(s: Any) -> str:
    return html.escape(str(s or ""))


# =============================================================================
# БД
# =============================================================================

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS tenders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_id TEXT UNIQUE,
                title TEXT,
                keyword TEXT,
                price REAL,
                deadline TEXT,
                status_code INTEGER,
                status TEXT,
                url TEXT,
                kind TEXT,
                type TEXT,
                placing_way TEXT,
                currency TEXT,
                published_at TEXT,
                created_at TEXT,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY,
                alerts_on INTEGER DEFAULT 1,
                joined_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_tenders_status ON tenders(status_code);
            CREATE INDEX IF NOT EXISTS idx_tenders_keyword ON tenders(keyword);
            CREATE INDEX IF NOT EXISTS idx_tenders_price ON tenders(price);
            CREATE INDEX IF NOT EXISTS idx_tenders_updated ON tenders(updated_at);
            CREATE INDEX IF NOT EXISTS idx_tenders_published ON tenders(published_at);
            """
        )
        await db.commit()
    logger.info("БД готова: %s", DB_PATH)


async def subscribe(chat_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)",
            (chat_id,),
        )
        await db.commit()


async def get_subscribers() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id FROM subscribers WHERE alerts_on=1")
        rows = await cur.fetchall()
        return [int(r[0]) for r in rows]


async def upsert_tender(t: dict[str, Any]) -> bool:
    """
    True = новый тендер.
    """
    now = datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT tender_id FROM tenders WHERE tender_id=?",
            (t["tender_id"],),
        )
        exists = await cur.fetchone()

        if exists:
            await db.execute(
                """
                UPDATE tenders
                   SET title=?,
                       keyword=?,
                       price=?,
                       deadline=?,
                       status_code=?,
                       status=?,
                       url=?,
                       kind=?,
                       type=?,
                       placing_way=?,
                       currency=?,
                       published_at=?,
                       updated_at=?
                 WHERE tender_id=?
                """,
                (
                    t.get("title"),
                    t.get("keyword"),
                    t.get("price"),
                    t.get("deadline"),
                    t.get("status_code"),
                    t.get("status"),
                    t.get("url"),
                    t.get("kind"),
                    t.get("type"),
                    t.get("placing_way"),
                    t.get("currency"),
                    t.get("published_at"),
                    now,
                    t["tender_id"],
                ),
            )
            await db.commit()
            return False

        await db.execute(
            """
            INSERT INTO tenders
                (tender_id, title, keyword, price, deadline, status_code, status, url,
                 kind, type, placing_way, currency, published_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                t.get("tender_id"),
                t.get("title"),
                t.get("keyword"),
                t.get("price"),
                t.get("deadline"),
                t.get("status_code"),
                t.get("status"),
                t.get("url"),
                t.get("kind"),
                t.get("type"),
                t.get("placing_way"),
                t.get("currency"),
                t.get("published_at"),
                now,
                now,
            ),
        )
        await db.commit()
        return True


async def get_tenders(limit: int = 15, only_active: bool = False) -> list[dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if only_active:
            cur = await db.execute(
                """
                SELECT * FROM tenders
                 WHERE COALESCE(status_code, 1) IN (1, 2)
                 ORDER BY updated_at DESC
                 LIMIT ?
                """,
                (limit,),
            )
        else:
            cur = await db.execute(
                "SELECT * FROM tenders ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            )
        return [dict(r) for r in await cur.fetchall()]


async def get_summary() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        c1 = await db.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                SUM(price) AS total_price,
                AVG(price) AS avg_price,
                SUM(CASE WHEN COALESCE(status_code, 1) IN (1, 2) THEN 1 ELSE 0 END) AS active_count,
                SUM(CASE WHEN COALESCE(status_code, 1) IN (3, 4, 5) THEN 1 ELSE 0 END) AS done_count
            FROM tenders
            """
        )
        totals = dict(await c1.fetchone())

        c2 = await db.execute(
            """
            SELECT keyword, COUNT(*) AS cnt, SUM(price) AS total
              FROM tenders
             GROUP BY keyword
             ORDER BY cnt DESC
             LIMIT 12
            """
        )
        by_keyword = [dict(r) for r in await c2.fetchall()]

        c3 = await db.execute(
            """
            SELECT status, COUNT(*) AS cnt, SUM(price) AS total
              FROM tenders
             GROUP BY status
             ORDER BY cnt DESC
            """
        )
        by_status = [dict(r) for r in await c3.fetchall()]

        c4 = await db.execute(
            """
            SELECT substr(COALESCE(published_at, created_at), 4, 7) AS month,
                   COUNT(*) AS cnt,
                   SUM(price) AS total
              FROM tenders
             GROUP BY month
             ORDER BY month DESC
             LIMIT 12
            """
        )
        by_month = [dict(r) for r in await c4.fetchall()]

        c5 = await db.execute(
            """
            SELECT
                SUM(CASE WHEN price IS NULL OR price=0 THEN 1 ELSE 0 END) AS no_price,
                SUM(CASE WHEN price > 0 AND price < 1000000 THEN 1 ELSE 0 END) AS p_0_1,
                SUM(CASE WHEN price >= 1000000 AND price < 10000000 THEN 1 ELSE 0 END) AS p_1_10,
                SUM(CASE WHEN price >= 10000000 AND price < 100000000 THEN 1 ELSE 0 END) AS p_10_100,
                SUM(CASE WHEN price >= 100000000 THEN 1 ELSE 0 END) AS p_100
              FROM tenders
            """
        )
        price_ranges = dict(await c5.fetchone())

        return {
            "totals": totals,
            "by_keyword": by_keyword,
            "by_status": by_status,
            "by_month": by_month,
            "price_ranges": price_ranges,
        }


async def export_csv_bytes() -> bytes:
    rows = await get_tenders(limit=5000, only_active=False)

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow(
        [
            "ID",
            "Название",
            "Ключ",
            "Цена",
            "Срок подачи",
            "Статус",
            "Опубликовано",
            "Тип",
            "Способ размещения",
            "Ссылка",
        ]
    )

    for r in rows:
        writer.writerow(
            [
                r.get("tender_id"),
                r.get("title"),
                r.get("keyword"),
                r.get("price"),
                r.get("deadline"),
                r.get("status"),
                r.get("published_at"),
                r.get("type"),
                r.get("placing_way"),
                r.get("url"),
            ]
        )

    # BOM для нормального открытия кириллицы в Excel.
    return ("\ufeff" + output.getvalue()).encode("utf-8")


# =============================================================================
# TENDERPLAN API
# =============================================================================

def api_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {TENDERPLAN_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


async def tp_search(
    session: aiohttp.ClientSession,
    keyword: str,
    page: int = 1,
    count: int = 50,
) -> list[dict[str, Any]]:
    payload = {
        "words": {"value": keyword},
        "statuses": [],
        "condition": "and",
        "page": page,
        "count": count,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.post(
                f"{TENDERPLAN_API}/search/list",
                json=payload,
                headers=api_headers(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                body = await resp.text()

                if resp.status != 200:
                    logger.warning(
                        "Tenderplan '%s': HTTP %s, body=%s",
                        keyword,
                        resp.status,
                        body[:500],
                    )
                    if resp.status == 429 and attempt < MAX_RETRIES:
                        await asyncio.sleep(10)
                        continue
                    return []

                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    logger.warning("Tenderplan '%s': не JSON: %s", keyword, body[:500])
                    return []

                tenders = data.get("tenders", []) if isinstance(data, dict) else []
                logger.info(
                    "Tenderplan '%s': HTTP 200, ключи ответа=%s, найдено raw=%d",
                    keyword,
                    list(data.keys()) if isinstance(data, dict) else type(data),
                    len(tenders),
                )

                if tenders:
                    logger.info(
                        "Tenderplan '%s': пример полей 1-го тендера=%s",
                        keyword,
                        list(tenders[0].keys()),
                    )
                    logger.info(
                        "Tenderplan '%s': пример названия=%s",
                        keyword,
                        clean_html(tenders[0].get("orderName", ""))[:300],
                    )

                return tenders

        except asyncio.TimeoutError:
            logger.warning("Tenderplan '%s': timeout, attempt=%d", keyword, attempt)
        except Exception as e:
            logger.warning("Tenderplan '%s': error=%s", keyword, e)

        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 * attempt)

    return []


def parse_tender(item: dict[str, Any], keyword: str) -> Optional[dict[str, Any]]:
    tender_id = item.get("_id") or item.get("id") or item.get("tenderId")
    if not tender_id:
        return None

    title = clean_html(item.get("orderName") or item.get("name") or item.get("title"))
    if not title:
        return None

    status_code = item.get("status", 1)

    return {
        "tender_id": str(tender_id),
        "title": title,
        "keyword": keyword,
        "price": safe_float(item.get("maxPrice") or item.get("price") or item.get("startPrice")),
        "deadline": fmt_ts_ms(item.get("submissionCloseDateTime") or item.get("endDate")),
        "status_code": int(status_code) if str(status_code).isdigit() else None,
        "status": status_name(status_code),
        "url": f"https://tenderplan.ru/app/analytics/tender/{tender_id}",
        "kind": str(item.get("kind") or ""),
        "type": str(item.get("type") or ""),
        "placing_way": str(item.get("placingWay") or ""),
        "currency": str(item.get("currency") or "RUB"),
        "published_at": fmt_ts_ms(item.get("publicationDateTime") or item.get("publishDate")),
    }


# =============================================================================
# ФОРМАТИРОВАНИЕ
# =============================================================================

def format_tender_card(t: dict[str, Any]) -> str:
    return (
        f"🏭 <b>Название:</b> {escape(t.get('title'))}\n"
        f"💰 <b>Начальная цена:</b> {fmt_price(t.get('price'))}\n"
        f"⏰ <b>Срок подачи:</b> {escape(t.get('deadline') or 'не указан')}\n"
        f"📌 <b>Статус:</b> {escape(t.get('status') or 'не указан')}\n"
        f"🏛 <b>Площадка:</b> Tenderplan\n"
        f"🔗 <a href=\"{escape(t.get('url'))}\">Открыть тендер</a>"
    )


def format_summary(data: dict[str, Any]) -> str:
    totals = data["totals"]
    pr = data["price_ranges"]

    lines = [
        "📊 <b>Аналитика по найденным тендерам</b>",
        "",
        f"Всего в базе: <b>{totals.get('total_count') or 0}</b>",
        f"Актуальные/на рассмотрении: <b>{totals.get('active_count') or 0}</b>",
        f"Завершённые/отменённые: <b>{totals.get('done_count') or 0}</b>",
        f"Сумма НМЦ: <b>{fmt_price(totals.get('total_price'))}</b>",
        f"Средняя НМЦ: <b>{fmt_price(totals.get('avg_price'))}</b>",
        "",
        "💰 <b>Диапазоны цен:</b>",
        f"без цены/0: {pr.get('no_price') or 0}",
        f"до 1 млн ₽: {pr.get('p_0_1') or 0}",
        f"1–10 млн ₽: {pr.get('p_1_10') or 0}",
        f"10–100 млн ₽: {pr.get('p_10_100') or 0}",
        f"100+ млн ₽: {pr.get('p_100') or 0}",
    ]

    if data["by_keyword"]:
        lines += ["", "🔑 <b>По ключевым словам:</b>"]
        for r in data["by_keyword"][:10]:
            lines.append(
                f"• {escape(r.get('keyword'))}: {r.get('cnt') or 0} / {fmt_price(r.get('total'))}"
            )

    if data["by_status"]:
        lines += ["", "📌 <b>По статусам:</b>"]
        for r in data["by_status"]:
            lines.append(
                f"• {escape(r.get('status'))}: {r.get('cnt') or 0} / {fmt_price(r.get('total'))}"
            )

    if data["by_month"]:
        lines += ["", "📅 <b>По месяцам:</b>"]
        for r in data["by_month"][:6]:
            lines.append(
                f"• {escape(r.get('month'))}: {r.get('cnt') or 0} / {fmt_price(r.get('total'))}"
            )

    return "\n".join(lines)


# =============================================================================
# ОБНОВЛЕНИЕ
# =============================================================================

async def run_update(bot: Optional[Bot] = None, notify: bool = True) -> tuple[int, int]:
    if _update_lock.locked():
        logger.warning("Обновление уже выполняется, второй запуск пропущен")
        return 0, 0

    async with _update_lock:
        logger.info("=== Обновление: %s ===", datetime.now().strftime("%d.%m.%Y %H:%M"))

        new_tenders: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            for keyword in SEARCH_TERMS:
                raw_items = await tp_search(session, keyword, page=1, count=50)

                parsed_count = 0
                duplicate_count = 0
                saved_count = 0
                filtered_count = 0

                for item in raw_items:
                    parsed = parse_tender(item, keyword)
                    if not parsed:
                        continue

                    parsed_count += 1

                    # Дубли внутри одного прохода не сохраняем.
                    if parsed["tender_id"] in seen_ids:
                        duplicate_count += 1
                        continue
                    seen_ids.add(parsed["tender_id"])

                    if not is_relevant_tender(parsed.get("title", ""), parsed.get("keyword", "")):
                        filtered_count += 1
                        logger.info(
                            "ОТФИЛЬТРОВАНО [%s]: %s",
                            keyword,
                            parsed.get("title", "")[:250],
                        )
                        continue

                    is_new = await upsert_tender(parsed)
                    if is_new:
                        saved_count += 1
                        new_tenders.append(parsed)
                    else:
                        duplicate_count += 1

                logger.info(
                    "Tenderplan '%s': raw=%d, parsed=%d, duplicates=%d, saved_new=%d, filtered/skipped=%d",
                    keyword,
                    len(raw_items),
                    parsed_count,
                    duplicate_count,
                    saved_count,
                    filtered_count,
                )

                await asyncio.sleep(REQUEST_DELAY)

        all_tenders = await get_tenders(limit=1000, only_active=False)
        active = [t for t in all_tenders if (t.get("status_code") or 1) in (1, 2)]

        logger.info(
            "Итого в базе: %d, активных/на рассмотрении: %d, новых за проход: %d",
            len(all_tenders),
            len(active),
            len(new_tenders),
        )

        if bot and notify and new_tenders:
            await notify_new(bot, new_tenders)

        return len(new_tenders), len(all_tenders)


async def notify_new(bot: Bot, new_tenders: list[dict[str, Any]]) -> None:
    subscribers = await get_subscribers()

    for chat_id in subscribers:
        try:
            await bot.send_message(
                chat_id,
                f"🔔 <b>Новых тендеров: {len(new_tenders)}</b>\nПоказываю первые 10.",
                parse_mode="HTML",
            )
            for t in new_tenders[:10]:
                await bot.send_message(
                    chat_id,
                    format_tender_card(t),
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
        except (TelegramForbiddenError, TelegramBadRequest):
            continue
        except Exception as e:
            logger.error("Ошибка уведомления %s: %s", chat_id, e)


# =============================================================================
# TELEGRAM
# =============================================================================

router = Router()

KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🆕 Тендеры"), KeyboardButton(text="📊 Аналитика")],
        [KeyboardButton(text="📤 Excel/CSV"), KeyboardButton(text="🔄 Обновить")],
        [KeyboardButton(text="❓ Помощь")],
    ],
    resize_keyboard=True,
)


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await subscribe(message.chat.id)
    await message.answer(
        "👋 <b>Бот мониторинга тендеров по металлолому</b>\n\n"
        "Кнопки:\n"
        "🆕 Тендеры — последние найденные тендеры\n"
        "📊 Аналитика — сводка по базе\n"
        "📤 Excel/CSV — выгрузка для Excel\n"
        "🔄 Обновить — принудительно обновить базу\n\n"
        "Команды:\n"
        "/new — тендеры\n"
        "/analytics — аналитика\n"
        "/export — выгрузка CSV\n"
        "/update — обновить",
        parse_mode="HTML",
        reply_markup=KEYBOARD,
    )


@router.message(Command("new"))
@router.message(lambda m: m.text == "🆕 Тендеры")
async def cmd_new(message: Message) -> None:
    rows = await get_tenders(limit=15, only_active=False)
    if not rows:
        await message.answer(
            "📭 В базе пока 0 тендеров.\nНажми 🔄 Обновить или /update.",
            reply_markup=KEYBOARD,
        )
        return

    await message.answer(f"🆕 <b>Последние тендеры</b> ({len(rows)}):", parse_mode="HTML")
    for t in rows:
        await message.answer(
            format_tender_card(t),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


@router.message(Command("analytics"))
@router.message(lambda m: m.text == "📊 Аналитика")
async def cmd_analytics(message: Message) -> None:
    data = await get_summary()
    await message.answer(format_summary(data), parse_mode="HTML")


@router.message(Command("export"))
@router.message(lambda m: m.text == "📤 Excel/CSV")
async def cmd_export(message: Message) -> None:
    rows = await get_tenders(limit=1, only_active=False)
    if not rows:
        await message.answer("📭 Экспорт пустой: в базе пока нет тендеров.")
        return

    data = await export_csv_bytes()
    filename = f"tenders_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    await message.answer_document(
        BufferedInputFile(data, filename=filename),
        caption="📤 Выгрузка тендеров CSV. Открывается в Excel.",
    )


@router.message(Command("update"))
@router.message(lambda m: m.text == "🔄 Обновить")
async def cmd_update(message: Message) -> None:
    await message.answer("🔄 Запускаю обновление. Это может занять 1–2 минуты.")
    new_count, total_count = await run_update(message.bot, notify=False)
    await message.answer(
        f"✅ Обновление завершено.\n"
        f"Новых: <b>{new_count}</b>\n"
        f"Всего в базе: <b>{total_count}</b>",
        parse_mode="HTML",
        reply_markup=KEYBOARD,
    )


@router.message(Command("search"))
async def cmd_search(message: Message, command: CommandObject) -> None:
    q = normalize_text(command.args or "")
    if not q:
        await message.answer("Пример: <code>/search алюминия</code>", parse_mode="HTML")
        return

    rows = await get_tenders(limit=1000, only_active=False)
    filtered = [r for r in rows if q in normalize_text(r.get("title", ""))][:15]

    if not filtered:
        await message.answer(f"📭 По запросу <b>{escape(q)}</b> ничего не найдено.", parse_mode="HTML")
        return

    await message.answer(f"🔍 Найдено: {len(filtered)}")
    for t in filtered:
        await message.answer(
            format_tender_card(t),
            parse_mode="HTML",
            disable_web_page_preview=True,
        )


@router.message(Command("help"))
@router.message(lambda m: m.text == "❓ Помощь")
async def cmd_help(message: Message) -> None:
    await cmd_start(message)


# =============================================================================
# START
# =============================================================================

async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN в Environment Variables")
    if not TENDERPLAN_TOKEN:
        raise RuntimeError("Не задан TENDERPLAN_TOKEN в Environment Variables")

    await init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler(
        timezone="Europe/Moscow",
        job_defaults={"misfire_grace_time": 600, "max_instances": 1},
    )

    scheduler.add_job(
        run_update,
        "interval",
        minutes=UPDATE_INTERVAL_MINUTES,
        args=[bot, True],
        next_run_time=datetime.now(),
        id="run_update",
        replace_existing=True,
    )

    scheduler.start()

    logger.info("Бот запущен ✅. Интервал обновления: %d мин.", UPDATE_INTERVAL_MINUTES)

    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message"],
            drop_pending_updates=True,
        )
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
