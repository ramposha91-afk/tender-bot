"""
Telegram-бот мониторинга тендеров по металлолому
Источник данных: Tenderplan API
Запуск: python3 main.py
Переменные окружения: BOT_TOKEN, TENDERPLAN_TOKEN
"""

import asyncio
import html
import logging
import os
import re
from datetime import datetime
from typing import Optional

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ═══════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "ВСТАВЬТЕ_ТОКЕН_СЮДА")
TENDERPLAN_TOKEN: str = os.getenv("TENDERPLAN_TOKEN", "ВСТАВЬТЕ_TENDERPLAN_TOKEN")

DB_PATH: str = "tenders.db"
UPDATE_INTERVAL_MINUTES: int = 30

# Ключевые слова для поиска — редактируйте здесь
SEARCH_KEYWORDS: list[str] = [
    "металлолом",
    "лом черных металлов",
    "лом цветных металлов",
    "лом алюминия",
    "лом меди",
    "лом латуни",
    "прием лома",
    "реализация лома",
    "стружка металлическая",
    "отходы черных металлов",
    "отходы цветных металлов",
    "демонтаж металлоконструкций",
]

# Обязательные слова — хотя бы одно должно быть в названии
REQUIRED_WORDS: list[str] = [
    "лом", "металлолом", "металлолом", "чермет", "цветмет",
    "стружка металл", "отходы металл", "металлоконструкц",
]

# Запрещённые слова — тендер отклоняется
FORBIDDEN_WORDS: list[str] = [
    "молоко", "молочн", "мясо", "рыба", "продукт питани",
    "медицинск", "лекарств", "фармацевт", "посуда",
    "квартир", "жилое", "недвижим", "земельный участок",
    "автомобил", "транспортн", "канцелярск",
]

# HTTP
REQUEST_TIMEOUT: int = 30
REQUEST_DELAY: float = 1.0
MAX_RETRIES: int = 3
TENDERPLAN_API: str = "https://tenderplan.ru/api"
_update_lock = asyncio.Lock()

# ═══════════════════════════════════════════════════════════
#  ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  ФИЛЬТРАЦИЯ
# ═══════════════════════════════════════════════════════════

def clean_html(text: str) -> str:
    """Убрать HTML теги из названия."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return " ".join(text.split()).strip()


def is_metal_tender(title: str) -> bool:
    """Проверить что тендер по металлолому."""
    if not title:
        return False
    t = title.lower()
    if any(w in t for w in FORBIDDEN_WORDS):
        return False
    # Хотя бы одно слово из списка должно быть в названии
    metal_words = [
        "лом", "металлолом", "металл", "чермет", "цветмет",
        "стружка", "металлоконструкц", "чугун", "нержавей",
        "алюмин", "медн", "латун", "цинк", "свинец",
    ]
    return any(w in t for w in metal_words)


# ═══════════════════════════════════════════════════════════
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS active_tenders (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_id     TEXT UNIQUE,
                title         TEXT,
                region        TEXT,
                keyword       TEXT,
                price         REAL,
                deadline      TEXT,
                status        TEXT,
                url           TEXT,
                published_at  TEXT,
                updated_at    TEXT
            );

            CREATE TABLE IF NOT EXISTS finished_tenders (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_id     TEXT UNIQUE,
                title         TEXT,
                region        TEXT,
                start_price   REAL,
                final_price   REAL,
                winner        TEXT,
                finished_at   TEXT,
                url           TEXT
            );

            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id   INTEGER PRIMARY KEY,
                alerts_on INTEGER DEFAULT 1,
                joined_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_active_updated ON active_tenders(updated_at);
            CREATE INDEX IF NOT EXISTS idx_active_region  ON active_tenders(region);
            CREATE INDEX IF NOT EXISTS idx_active_keyword ON active_tenders(keyword);
        """)
        await db.commit()
    logger.info("БД готова: %s", DB_PATH)


async def upsert_active(t: dict) -> bool:
    """Сохранить активный тендер. True = новый."""
    if not is_metal_tender(t.get("title", "")):
        return False
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id FROM active_tenders WHERE tender_id=?", (t["tender_id"],)
        )
        if await cur.fetchone():
            await db.execute(
                "UPDATE active_tenders SET title=?,region=?,price=?,deadline=?,status=?,url=?,updated_at=? WHERE tender_id=?",
                (t.get("title"), t.get("region"), t.get("price"),
                 t.get("deadline"), t.get("status"), t.get("url"), now, t["tender_id"])
            )
            await db.commit()
            return False
        await db.execute(
            """INSERT INTO active_tenders
               (tender_id,title,region,keyword,price,deadline,status,url,published_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (t["tender_id"], t.get("title"), t.get("region"), t.get("keyword"),
             t.get("price"), t.get("deadline"), t.get("status"),
             t.get("url"), now, now)
        )
        await db.commit()
        return True


async def move_to_finished(tender_id: str, final_price: Optional[float],
                            winner: Optional[str], finished_at: Optional[str]):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT * FROM active_tenders WHERE tender_id=?", (tender_id,)
        )
        row = await cur.fetchone()
        if not row:
            return
        cols = [d[0] for d in cur.description]
        t = dict(zip(cols, row))
        await db.execute(
            """INSERT OR REPLACE INTO finished_tenders
               (tender_id,title,region,start_price,final_price,winner,finished_at,url)
               VALUES (?,?,?,?,?,?,?,?)""",
            (tender_id, t.get("title"), t.get("region"), t.get("price"),
             final_price, winner, finished_at or datetime.utcnow().isoformat(), t.get("url"))
        )
        await db.execute("DELETE FROM active_tenders WHERE tender_id=?", (tender_id,))
        await db.commit()
        logger.info("Тендер %s → завершённые", tender_id)


async def get_active(limit: int = 15, region: Optional[str] = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if region:
            cur = await db.execute(
                "SELECT * FROM active_tenders WHERE region LIKE ? ORDER BY updated_at DESC LIMIT ?",
                (f"%{region}%", limit)
            )
        else:
            cur = await db.execute(
                "SELECT * FROM active_tenders ORDER BY updated_at DESC LIMIT ?", (limit,)
            )
        return [dict(r) for r in await cur.fetchall()]


async def get_finished(limit: int = 15, region: Optional[str] = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if region:
            cur = await db.execute(
                "SELECT * FROM finished_tenders WHERE region LIKE ? ORDER BY finished_at DESC LIMIT ?",
                (f"%{region}%", limit)
            )
        else:
            cur = await db.execute(
                "SELECT * FROM finished_tenders ORDER BY finished_at DESC LIMIT ?", (limit,)
            )
        return [dict(r) for r in await cur.fetchall()]


async def get_active_summary() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        c1 = await db.execute("SELECT COUNT(*) as cnt, SUM(price) as total FROM active_tenders")
        r1 = dict(await c1.fetchone())
        c2 = await db.execute(
            """SELECT region, COUNT(*) as cnt, SUM(price) as total
               FROM active_tenders WHERE region IS NOT NULL AND length(region) > 3
               GROUP BY region ORDER BY total DESC LIMIT 10"""
        )
        regions = [dict(r) for r in await c2.fetchall()]
        c3 = await db.execute(
            """SELECT keyword, COUNT(*) as cnt, SUM(price) as total
               FROM active_tenders WHERE keyword IS NOT NULL
               GROUP BY keyword ORDER BY cnt DESC LIMIT 10"""
        )
        keywords = [dict(r) for r in await c3.fetchall()]
        return {"count": r1["cnt"] or 0, "total": r1["total"] or 0,
                "regions": regions, "keywords": keywords}


async def get_finished_summary() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        c1 = await db.execute(
            "SELECT COUNT(*) as cnt, SUM(start_price) as total, AVG(final_price) as avg FROM finished_tenders"
        )
        r1 = dict(await c1.fetchone())
        c2 = await db.execute(
            """SELECT region, COUNT(*) as cnt, SUM(start_price) as total
               FROM finished_tenders WHERE region IS NOT NULL
               GROUP BY region ORDER BY cnt DESC LIMIT 10"""
        )
        regions = [dict(r) for r in await c2.fetchall()]
        return {"count": r1["cnt"] or 0, "total": r1["total"] or 0,
                "avg": r1["avg"] or 0, "regions": regions}


async def subscribe(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
        await db.commit()


async def get_subscribers() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id FROM subscribers WHERE alerts_on=1")
        return [r[0] for r in await cur.fetchall()]


# ═══════════════════════════════════════════════════════════
#  ФОРМАТИРОВАНИЕ
# ═══════════════════════════════════════════════════════════

def fmt_price(price: Optional[float]) -> str:
    if price is None:
        return "не указана"
    if price >= 1_000_000_000:
        return f"{price/1_000_000_000:.2f} млрд ₽"
    if price >= 1_000_000:
        return f"{price/1_000_000:.2f} млн ₽"
    if price >= 1_000:
        return f"{price/1_000:.1f} тыс. ₽"
    return f"{price:.2f} ₽"


def fmt_ts(ts_ms: Optional[int]) -> Optional[str]:
    """Timestamp в миллисекундах → строка даты."""
    if not ts_ms:
        return None
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return None


def format_active_card(t: dict) -> str:
    return (
        f"🏭 <b>Название:</b> {t.get('title') or '—'}\n"
        f"📍 <b>Регион:</b> {t.get('region') or 'не указан'}\n"
        f"💰 <b>Начальная цена:</b> {fmt_price(t.get('price'))}\n"
        f"⏰ <b>Срок подачи:</b> {t.get('deadline') or 'не указан'}\n"
        f"🏛 <b>Площадка:</b> Tenderplan\n"
        f"📌 <b>Статус:</b> {t.get('status') or 'Активен'}\n"
        f"🔗 <a href=\"{t.get('url', '')}\">Открыть тендер</a>"
    )


def format_finished_card(t: dict) -> str:
    savings = ""
    sp = t.get("start_price")
    fp = t.get("final_price")
    if sp and fp and sp > 0:
        pct = (1 - fp / sp) * 100
        savings = f"\n📉 <b>Экономия:</b> {pct:.1f}%"
    return (
        f"🏁 <b>Тендер завершён</b>\n\n"
        f"🏭 <b>Название:</b> {t.get('title') or '—'}\n"
        f"📍 <b>Регион:</b> {t.get('region') or 'не указан'}\n"
        f"💰 <b>Начальная цена:</b> {fmt_price(sp)}\n"
        f"🏁 <b>Итоговая цена:</b> {fmt_price(fp)}"
        f"{savings}\n"
        f"🥇 <b>Победитель:</b> {t.get('winner') or 'не указан'}\n"
        f"📌 <b>Статус:</b> Завершён\n"
        f"🔗 <a href=\"{t.get('url', '')}\">Открыть тендер</a>"
    )


def format_active_summary(data: dict) -> str:
    lines = [
        "📊 <b>Сводка — Активные тендеры</b>\n",
        f"Всего: <b>{data['count']}</b>",
        f"Общая сумма: <b>{fmt_price(data['total'])}</b>",
    ]
    if data.get("regions"):
        lines += ["", "📍 <b>По регионам:</b>"]
        for r in data["regions"]:
            lines.append(f"  {r['region']} — {r['cnt']} тенд. / {fmt_price(r['total'])}")
    if data.get("keywords"):
        lines += ["", "🔑 <b>По ключевым словам:</b>"]
        for k in data["keywords"]:
            lines.append(f"  {k['keyword']} — {k['cnt']} тенд. / {fmt_price(k['total'])}")
    return "\n".join(lines)


def format_finished_summary(data: dict) -> str:
    lines = [
        "📊 <b>Сводка — Завершённые тендеры</b>\n",
        f"Всего: <b>{data['count']}</b>",
        f"Общая сумма НМЦ: <b>{fmt_price(data['total'])}</b>",
        f"Средняя итоговая цена: <b>{fmt_price(data['avg'])}</b>",
    ]
    if data.get("regions"):
        lines += ["", "📍 <b>По регионам:</b>"]
        for r in data["regions"]:
            lines.append(f"  {r['region']} — {r['cnt']} тенд. / {fmt_price(r['total'])}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
#  TENDERPLAN API
# ═══════════════════════════════════════════════════════════

def get_headers() -> dict:
    # В документации Tenderplan указано, что ключ передаётся как access_token.
    # Header оставляем запасным вариантом, если у аккаунта включена Bearer-авторизация.
    return {
        "Authorization": f"Bearer {TENDERPLAN_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


async def tp_search(session: aiohttp.ClientSession,
                    keyword: str, page: int = 1, count: int = 50) -> list[dict]:
    """Поиск тендеров через Tenderplan API."""
    payload = {"text": keyword, "page": page, "count": count}
    params = {"access_token": TENDERPLAN_TOKEN}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.post(
                f"{TENDERPLAN_API}/search/list",
                params=params,
                json=payload,
                headers=get_headers(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                text = await resp.text()
                if resp.status == 200:
                    try:
                        data = await resp.json()
                    except Exception:
                        logger.warning("Tenderplan вернул не JSON по '%s': %.300s", keyword, text)
                        return []

                    if isinstance(data, list):
                        items = data
                    elif isinstance(data, dict):
                        items = (
                            data.get("tenders")
                            or data.get("items")
                            or data.get("data")
                            or data.get("result")
                            or []
                        )
                    else:
                        items = []

                    logger.info("Tenderplan '%s': %d результатов", keyword, len(items))
                    return items if isinstance(items, list) else []

                if resp.status == 429:
                    logger.warning("Tenderplan rate limit по '%s', ждём 10 сек", keyword)
                    await asyncio.sleep(10)
                    continue

                logger.warning("Tenderplan search '%s': HTTP %s, body: %.300s", keyword, resp.status, text)
                return []
        except asyncio.TimeoutError:
            logger.warning("Таймаут Tenderplan по '%s' (попытка %d)", keyword, attempt)
        except Exception as e:
            logger.warning("Ошибка Tenderplan по '%s': %s", keyword, e)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)
    return []


async def tp_get_tender(session: aiohttp.ClientSession, tender_id: str) -> Optional[dict]:
    """Получить полную информацию о тендере включая контракты."""
    try:
        async with session.get(
            f"{TENDERPLAN_API}/tenders/v2/fullinfo",
            params={"id": tender_id, "access_token": TENDERPLAN_TOKEN},
            headers=get_headers(),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        logger.debug("tp_get_tender error: %s", e)
    return None


async def tp_get_contracts(session: aiohttp.ClientSession, tender_id: str) -> list[dict]:
    """Получить контракты тендера (победитель и итоговая цена)."""
    try:
        async with session.get(
            f"{TENDERPLAN_API}/tenders/contracts",
            params={"id": tender_id, "access_token": TENDERPLAN_TOKEN},
            headers=get_headers(),
            timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data if isinstance(data, list) else data.get("contracts", [])
    except Exception as e:
        logger.debug("tp_get_contracts error: %s", e)
    return []


def parse_tender_from_api(item: dict, keyword: str) -> Optional[dict]:
    """Преобразовать ответ API в наш формат."""
    tid = item.get("_id") or item.get("id") or item.get("tenderId") or item.get("purchaseNumber")
    if not tid:
        logger.debug("Пропуск: нет id в item=%s", item)
        return None

    title = clean_html(
        item.get("orderName")
        or item.get("name")
        or item.get("title")
        or item.get("subject")
        or ""
    )
    if not title:
        return None

    price = item.get("maxPrice") or item.get("startPrice") or item.get("price") or item.get("amount")
    deadline_ts = item.get("submissionCloseDateTime") or item.get("deadline") or item.get("endDate")
    deadline = fmt_ts(deadline_ts)
    pub_ts = item.get("publicationDateTime")

    # Статус
    status_map = {1: "Активен", 2: "На рассмотрении", 3: "Завершён",
                  4: "Отменён", 5: "Не состоялся"}
    status_code = item.get("status", 1)
    status = status_map.get(status_code, "Активен")

    # URL на tenderplan
    url = f"https://tenderplan.ru/app/analytics/tender/{tid}"

    return {
        "tender_id": tid,
        "title": title,
        "region": item.get("region") or item.get("regionName") or item.get("customerRegion"),
        "keyword": keyword,
        "price": price,
        "deadline": deadline,
        "status": status,
        "url": url,
        "published_at": fmt_ts(pub_ts),
        "status_code": status_code,
    }


# ═══════════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК
# ═══════════════════════════════════════════════════════════

async def run_update(bot: Bot):
    """Основной цикл обновления."""
    if _update_lock.locked():
        logger.info("Обновление уже выполняется, второй запуск пропущен")
        return
    async with _update_lock:
        await _run_update_locked(bot)


async def _run_update_locked(bot: Bot):
    logger.info("=== Обновление: %s ===", datetime.now().strftime("%d.%m.%Y %H:%M"))

    new_tenders = []
    completed_tenders = []

    async with aiohttp.ClientSession() as session:
        # 1. Поиск новых тендеров
        seen_ids: set[str] = set()
        for kw in SEARCH_KEYWORDS:
            items = await tp_search(session, kw, page=1, count=50)
            if not items:
                logger.info("Tenderplan '%s': 0 результатов", kw)
            for item in items:
                t = parse_tender_from_api(item, kw)
                if not t or t["tender_id"] in seen_ids:
                    continue
                seen_ids.add(t["tender_id"])

                # Завершённые переносим в finished
                if t.get("status_code") in (3, 4, 5):
                    # Пробуем получить победителя
                    contracts = await tp_get_contracts(session, t["tender_id"])
                    winner = None
                    final_price = None
                    if contracts:
                        c = contracts[0]
                        winner = c.get("supplierName") or c.get("winner")
                        final_price = c.get("price") or c.get("finalPrice")
                    await move_to_finished(
                        t["tender_id"], final_price, winner,
                        datetime.utcnow().isoformat()
                    )
                    continue

                is_new = await upsert_active(t)
                if is_new:
                    new_tenders.append(t)
            await asyncio.sleep(REQUEST_DELAY)

        # 2. Проверяем статусы активных тендеров
        active = await get_active(limit=100)
        logger.info("Проверяем %d активных тендеров", len(active))
        for t in active:
            full = await tp_get_tender(session, t["tender_id"])
            if not full:
                await asyncio.sleep(0.5)
                continue
            status_code = full.get("status")
            if status_code in (3, 4, 5):
                contracts = await tp_get_contracts(session, t["tender_id"])
                winner = None
                final_price = None
                if contracts:
                    c = contracts[0]
                    winner = c.get("supplierName") or c.get("winner")
                    final_price = c.get("price") or c.get("finalPrice")
                await move_to_finished(
                    t["tender_id"], final_price, winner,
                    datetime.utcnow().isoformat()
                )
                # Получаем для уведомления
                finished = await get_finished(limit=1)
                if finished and finished[0]["tender_id"] == t["tender_id"]:
                    completed_tenders.append(finished[0])
            await asyncio.sleep(0.5)

    logger.info("Новых: %d, Завершённых: %d", len(new_tenders), len(completed_tenders))
    if new_tenders or completed_tenders:
        await notify_all(bot, new_tenders, completed_tenders)


async def notify_all(bot: Bot, new_list: list, done_list: list):
    from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
    subs = await get_subscribers()
    for chat_id in subs:
        if new_list:
            try:
                await bot.send_message(
                    chat_id,
                    f"🔔 <b>Новых тендеров: {len(new_list)}</b>",
                    parse_mode="HTML"
                )
                for t in new_list[:5]:
                    await bot.send_message(
                        chat_id, format_active_card(t),
                        parse_mode="HTML", disable_web_page_preview=True
                    )
            except (TelegramForbiddenError, TelegramBadRequest):
                pass
            except Exception as e:
                logger.error("notify error %s: %s", chat_id, e)
        for t in done_list[:3]:
            try:
                await bot.send_message(
                    chat_id,
                    "🏁 <b>Тендер завершён!</b>\n\n" + format_finished_card(t),
                    parse_mode="HTML", disable_web_page_preview=True
                )
            except Exception as e:
                logger.error("notify done error %s: %s", chat_id, e)


# ═══════════════════════════════════════════════════════════
#  КОМАНДЫ БОТА
# ═══════════════════════════════════════════════════════════

router = Router()

KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🆕 Новые тендеры"), KeyboardButton(text="🏁 Завершённые")],
        [KeyboardButton(text="📊 Сводка новых"), KeyboardButton(text="📊 Сводка завершённых")],
        [KeyboardButton(text="🔄 Обновить"), KeyboardButton(text="❓ Помощь")],
    ],
    resize_keyboard=True,
)


@router.message(Command("start"))
async def cmd_start(message: Message):
    await subscribe(message.chat.id)
    await message.answer(
        "👋 <b>Бот мониторинга тендеров по металлолому</b>\n\n"
        "Источник: <b>Tenderplan</b> — все площадки России\n\n"
        "<b>Команды:</b>\n"
        "/new — активные тендеры\n"
        "/finished — завершённые с победителями\n"
        "/new_summary — сводка по активным\n"
        "/finished_summary — сводка по завершённым\n"
        "/search [регион] — поиск по региону\n"
        "/update — обновить сейчас",
        parse_mode="HTML",
        reply_markup=KEYBOARD,
    )


@router.message(Command("new"))
@router.message(lambda m: m.text == "🆕 Новые тендеры")
async def cmd_new(message: Message):
    tenders = await get_active(limit=10)
    if not tenders:
        await message.answer(
            "📭 Активных тендеров пока нет.\nНажмите 🔄 Обновить.",
            parse_mode="HTML"
        )
        return
    await message.answer(f"🆕 <b>Активные тендеры</b> ({len(tenders)} шт.):", parse_mode="HTML")
    for t in tenders:
        await message.answer(
            format_active_card(t), parse_mode="HTML", disable_web_page_preview=True
        )


@router.message(Command("finished"))
@router.message(lambda m: m.text == "🏁 Завершённые")
async def cmd_finished(message: Message):
    tenders = await get_finished(limit=10)
    if not tenders:
        await message.answer("📭 Завершённых тендеров пока нет.", parse_mode="HTML")
        return
    await message.answer(f"🏁 <b>Завершённые тендеры</b> ({len(tenders)} шт.):", parse_mode="HTML")
    for t in tenders:
        await message.answer(
            format_finished_card(t), parse_mode="HTML", disable_web_page_preview=True
        )


@router.message(Command("new_summary"))
@router.message(lambda m: m.text == "📊 Сводка новых")
async def cmd_new_summary(message: Message):
    data = await get_active_summary()
    await message.answer(format_active_summary(data), parse_mode="HTML")


@router.message(Command("finished_summary"))
@router.message(lambda m: m.text == "📊 Сводка завершённых")
async def cmd_finished_summary(message: Message):
    data = await get_finished_summary()
    await message.answer(format_finished_summary(data), parse_mode="HTML")


@router.message(Command("search"))
async def cmd_search(message: Message, command: CommandObject):
    region = command.args
    if not region:
        await message.answer("ℹ️ Пример: <code>/search Москва</code>", parse_mode="HTML")
        return
    tenders = await get_active(limit=10, region=region)
    if not tenders:
        await message.answer(f"📭 Тендеры по <b>{region}</b> не найдены.", parse_mode="HTML")
        return
    await message.answer(f"🔍 <b>{region}</b> ({len(tenders)} шт.):", parse_mode="HTML")
    for t in tenders:
        await message.answer(
            format_active_card(t), parse_mode="HTML", disable_web_page_preview=True
        )


@router.message(Command("update"))
@router.message(lambda m: m.text == "🔄 Обновить")
async def cmd_update(message: Message):
    await message.answer("🔄 Запускаю обновление... 5-10 минут.", parse_mode="HTML")
    asyncio.create_task(run_update(message.bot))


@router.message(Command("help"))
@router.message(lambda m: m.text == "❓ Помощь")
async def cmd_help(message: Message):
    await cmd_start(message)


# ═══════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════

async def main():
    if BOT_TOKEN == "ВСТАВЬТЕ_ТОКЕН_СЮДА":
        logger.error("Укажите BOT_TOKEN!")
        return
    if TENDERPLAN_TOKEN == "ВСТАВЬТЕ_TENDERPLAN_TOKEN":
        logger.error("Укажите TENDERPLAN_TOKEN!")
        return

    await init_db()

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler(
        timezone="Europe/Moscow",
        job_defaults={"misfire_grace_time": 600}
    )
    scheduler.add_job(
        run_update, "interval",
        minutes=UPDATE_INTERVAL_MINUTES,
        args=[bot],
        next_run_time=datetime.now(),
        id="update",
    )
    scheduler.start()
    logger.info("Бот запущен ✅ (обновление каждые %d мин)", UPDATE_INTERVAL_MINUTES)

    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
