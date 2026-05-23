"""
Telegram-бот мониторинга тендеров ЕИС (zakupki.gov.ru)
Один файл — полный MVP.

Установка:
    pip install aiogram aiohttp aiosqlite apscheduler beautifulsoup4 --break-system-packages

Запуск:
    export BOT_TOKEN="8808326457:AAEWD7QiXn2SUfH0EfZMTL4NoJz00keNcO4"
    python3 main.py
"""

import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════
#  НАСТРОЙКИ — редактируйте здесь
# ═══════════════════════════════════════════════════════════

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "8808326457:AAEWD7QiXn2SUfH0EfZMTL4NoJz00keNcO4")
DB_PATH: str = "tenders.db"
UPDATE_INTERVAL_MINUTES: int = 30  # интервал обновления

# Ключевые слова поиска — легко редактировать
SEARCH_KEYWORDS: list[str] = [
    "лом",
    "металлолом",
    "алюминий",
    "лом алюминия",
    "стружка алюминиевая",
    "медь",
    "латунь",
    "лом цветных металлов",
    "отходы металлов",
    "демонтаж металлоконструкций",
]

# Слова для фильтрации — тендер должен содержать хотя бы одно
FILTER_KEYWORDS: list[str] = [
    "лом", "металлолом", "металл", "чермет", "цветмет",
    "алюминий", "медь", "латунь", "цинк", "никель", "свинец",
    "стружка", "отходы металл", "вторсырье", "вторичное сырье",
    "демонтаж металлоконструкц", "металлоконструкц",
    "прием лома", "сдача лома", "реализация лома",
    "лом черных", "лом цветных",
]


def is_metal_tender(title: str) -> bool:
    """Проверить что тендер связан с металлом/ломом."""
    if not title:
        return False
    title_lower = title.lower()
    return any(kw.lower() in title_lower for kw in FILTER_KEYWORDS)


# HTTP настройки
REQUEST_TIMEOUT: int = 30
REQUEST_DELAY: float = 3.0
MAX_RETRIES: int = 3
MAX_PAGES: int = 5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
    "Referer": "https://zakupki.gov.ru/",
}

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
#  БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════

async def init_db():
    """Инициализация базы данных и создание таблиц."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS active_tenders (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_number TEXT UNIQUE,
                title        TEXT,
                region       TEXT,
                keyword      TEXT,
                price        REAL,
                deadline     TEXT,
                status       TEXT,
                url          TEXT,
                published_at TEXT,
                updated_at   TEXT
            );

            CREATE TABLE IF NOT EXISTS finished_tenders (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_number TEXT UNIQUE,
                title        TEXT,
                region       TEXT,
                start_price  REAL,
                final_price  REAL,
                winner       TEXT,
                finished_at  TEXT,
                status       TEXT,
                url          TEXT
            );

            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id   INTEGER PRIMARY KEY,
                alerts_on INTEGER DEFAULT 1,
                joined_at TEXT DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_active_region   ON active_tenders(region);
            CREATE INDEX IF NOT EXISTS idx_active_price    ON active_tenders(price);
            CREATE INDEX IF NOT EXISTS idx_active_updated  ON active_tenders(updated_at);
            CREATE INDEX IF NOT EXISTS idx_finished_region ON finished_tenders(region);
        """)
        await db.commit()
    logger.info("БД инициализирована: %s", DB_PATH)


async def upsert_active_tender(t: dict) -> bool:
    """Добавить/обновить активный тендер. Возвращает True если новый."""
    # Фильтруем нерелевантные тендеры
    if not is_metal_tender(t.get("title", "")):
        return False
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, status FROM active_tenders WHERE tender_number=?",
            (t["tender_number"],)
        )
        row = await cur.fetchone()
        if row:
            await db.execute(
                """UPDATE active_tenders
                   SET title=?, region=?, price=?, deadline=?, status=?,
                       url=?, updated_at=?
                   WHERE id=?""",
                (t.get("title"), t.get("region"), t.get("price"),
                 t.get("deadline"), t.get("status"), t.get("url"), now, row[0])
            )
            await db.commit()
            return False
        else:
            await db.execute(
                """INSERT INTO active_tenders
                   (tender_number, title, region, keyword, price, deadline, status, url, published_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (t["tender_number"], t.get("title"), t.get("region"),
                 t.get("keyword"), t.get("price"), t.get("deadline"),
                 t.get("status"), t.get("url"), now, now)
            )
            await db.commit()
            return True


async def move_to_finished(tender_number: str, final_price: Optional[float],
                           winner: Optional[str], finished_at: Optional[str]):
    """Перенести тендер из активных в завершённые."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT * FROM active_tenders WHERE tender_number=?", (tender_number,)
        )
        row = await cur.fetchone()
        if not row:
            return
        cols = [d[0] for d in cur.description]
        t = dict(zip(cols, row))

        # Проверяем нет ли уже в finished
        cur2 = await db.execute(
            "SELECT 1 FROM finished_tenders WHERE tender_number=?", (tender_number,)
        )
        if await cur2.fetchone():
            await db.execute("DELETE FROM active_tenders WHERE tender_number=?", (tender_number,))
            await db.commit()
            return

        await db.execute(
            """INSERT OR REPLACE INTO finished_tenders
               (tender_number, title, region, start_price, final_price,
                winner, finished_at, status, url)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (tender_number, t.get("title"), t.get("region"),
             t.get("price"), final_price, winner,
             finished_at or datetime.utcnow().isoformat(),
             "Завершён", t.get("url"))
        )
        await db.execute("DELETE FROM active_tenders WHERE tender_number=?", (tender_number,))
        await db.commit()
        logger.info("Тендер %s перенесён в завершённые", tender_number)


async def get_active_tenders(limit: int = 20, region: Optional[str] = None) -> list[dict]:
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


async def get_finished_tenders(limit: int = 20, region: Optional[str] = None) -> list[dict]:
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
    """Сводка по активным тендерам."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        c1 = await db.execute("SELECT COUNT(*) as cnt, SUM(price) as total FROM active_tenders")
        r1 = dict(await c1.fetchone())
        # По регионам
        c2 = await db.execute(
            """SELECT region, COUNT(*) as cnt, SUM(price) as total
               FROM active_tenders
               WHERE region IS NOT NULL AND region != '' AND length(region) > 5
               GROUP BY region ORDER BY total DESC LIMIT 10"""
        )
        regions = [dict(r) for r in await c2.fetchall()]
        # По ключевым словам
        c3 = await db.execute(
            """SELECT keyword, COUNT(*) as cnt, SUM(price) as total
               FROM active_tenders
               WHERE keyword IS NOT NULL AND keyword != ''
               GROUP BY keyword ORDER BY cnt DESC LIMIT 10"""
        )
        keywords = [dict(r) for r in await c3.fetchall()]
        return {"count": r1["cnt"] or 0, "total": r1["total"] or 0,
                "regions": regions, "keywords": keywords}


async def get_finished_summary() -> dict:
    """Сводка по завершённым тендерам."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        c1 = await db.execute(
            "SELECT COUNT(*) as cnt, SUM(start_price) as total, AVG(final_price) as avg_final FROM finished_tenders"
        )
        r1 = dict(await c1.fetchone())
        c2 = await db.execute(
            """SELECT region, COUNT(*) as cnt, SUM(start_price) as total
               FROM finished_tenders
               WHERE region IS NOT NULL AND region != ''
               GROUP BY region ORDER BY cnt DESC LIMIT 10"""
        )
        regions = [dict(r) for r in await c2.fetchall()]
        return {"count": r1["cnt"] or 0, "total": r1["total"] or 0,
                "avg_final": r1["avg_final"] or 0, "regions": regions}


async def subscribe(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,)
        )
        await db.commit()


async def get_subscribers() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id FROM subscribers WHERE alerts_on=1")
        return [r[0] for r in await cur.fetchall()]


# ═══════════════════════════════════════════════════════════
#  ФОРМАТИРОВАНИЕ
# ═══════════════════════════════════════════════════════════

def fmt_price(price: Optional[float]) -> str:
    """Форматировать цену."""
    if price is None:
        return "не указана"
    if price >= 1_000_000_000:
        return f"{price/1_000_000_000:.2f} млрд ₽"
    if price >= 1_000_000:
        return f"{price/1_000_000:.2f} млн ₽"
    if price >= 1_000:
        return f"{price/1_000:.1f} тыс. ₽"
    return f"{price:.2f} ₽"


def format_active_card(t: dict) -> str:
    return (
        f"🏭 <b>Название:</b> {t.get('title') or '—'}\n"
        f"📍 <b>Регион:</b> {t.get('region') or 'не указан'}\n"
        f"💰 <b>Начальная цена:</b> {fmt_price(t.get('price'))}\n"
        f"⏰ <b>Срок подачи:</b> {t.get('deadline') or 'не указан'}\n"
        f"🏛 <b>Площадка:</b> ЕИС\n"
        f"📌 <b>Статус:</b> {t.get('status') or 'Активен'}\n"
        f"🔗 <a href=\"{t.get('url', '')}\">Открыть тендер</a>"
    )


def format_finished_card(t: dict) -> str:
    savings = ""
    if t.get("start_price") and t.get("final_price") and t["start_price"] > 0:
        pct = (1 - t["final_price"] / t["start_price"]) * 100
        savings = f"\n📉 <b>Экономия:</b> {pct:.1f}%"
    return (
        f"🏁 <b>Тендер завершён</b>\n\n"
        f"🏭 <b>Название:</b> {t.get('title') or '—'}\n"
        f"📍 <b>Регион:</b> {t.get('region') or 'не указан'}\n"
        f"💰 <b>Начальная цена:</b> {fmt_price(t.get('start_price'))}\n"
        f"🏁 <b>Итоговая цена:</b> {fmt_price(t.get('final_price'))}"
        f"{savings}\n"
        f"🥇 <b>Победитель:</b> {t.get('winner') or 'не указан'}\n"
        f"📌 <b>Статус:</b> Завершён\n"
        f"🔗 <a href=\"{t.get('url', '')}\">Открыть тендер</a>"
    )


def format_active_summary(data: dict) -> str:
    lines = [
        "📊 <b>Новые тендеры</b>\n",
        f"Всего: <b>{data['count']}</b>",
        f"Общая сумма: <b>{fmt_price(data['total'])}</b>",
    ]
    if data.get("regions"):
        lines.append("")
        lines.append("📍 <b>По регионам:</b>")
        for r in data["regions"]:
            name = r["region"] or "Не указан"
            lines.append(f"  {name} — {r['cnt']} тендер(а) / {fmt_price(r['total'])}")
    if data.get("keywords"):
        lines.append("")
        lines.append("🔑 <b>По ключевым словам:</b>")
        for k in data["keywords"]:
            name = k["keyword"] or "—"
            lines.append(f"  {name} — {k['cnt']} тендер(а) / {fmt_price(k['total'])}")
    return "\n".join(lines)


def format_finished_summary(data: dict) -> str:
    lines = [
        "📊 <b>Завершённые тендеры</b>\n",
        f"Всего: <b>{data['count']}</b>",
        f"Общая сумма НМЦ: <b>{fmt_price(data['total'])}</b>",
        f"Средняя итоговая цена: <b>{fmt_price(data['avg_final'])}</b>",
    ]
    if data["regions"]:
        lines.append("")
        for r in data["regions"]:
            name = r["region"] or "Не указан"
            lines.append(f"📍 {name} — {r['cnt']} тендер(а) / {fmt_price(r['total'])}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
#  ПАРСЕР ЕИС
# ═══════════════════════════════════════════════════════════

def _parse_price(text: Optional[str]) -> Optional[float]:
    """Извлечь число из строки с ценой."""
    if not text:
        return None
    cleaned = re.sub(r"[^\d,.]", "", text.replace("\u00a0", "")).replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


async def fetch_with_retry(session: aiohttp.ClientSession, url: str,
                            params: dict = None) -> Optional[str]:
    """HTTP GET с повторными попытками."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(
                url, params=params, headers=HEADERS,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                allow_redirects=True,
            ) as resp:
                if resp.status == 200:
                    return await resp.text()
                logger.warning("HTTP %s для %s (попытка %d)", resp.status, url, attempt)
        except asyncio.TimeoutError:
            logger.warning("Таймаут %s (попытка %d)", url, attempt)
        except Exception as e:
            logger.warning("Ошибка %s (попытка %d): %s", url, attempt, e)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)
    return None


def parse_tender_card(card) -> Optional[dict]:
    """Разобрать карточку тендера из HTML ЕИС."""
    try:
        # Номер закупки
        num_tag = card.select_one(
            "div.registry-entry__header-mid__number a, "
            "a[href*='regNumber='], span.tender-number"
        )
        if not num_tag:
            return None

        href = num_tag.get("href", "")

        # Извлекаем номер из ссылки или текста
        number = ""
        if "regNumber=" in href:
            number = href.split("regNumber=")[-1].split("&")[0]
        if not number:
            number = re.sub(r"[^\d]", "", num_tag.get_text(strip=True))
        if not number:
            return None

        # Формируем правильную ссылку на тендер
        url = f"https://zakupki.gov.ru/epz/order/notice/ea44/view/common-info.html?regNumber={number}"

        # Название тендера
        title_tag = card.select_one(
            "div.registry-entry__body-value, "
            "a.registry-entry__body-title, "
            "span.tender-title"
        )
        title = title_tag.get_text(strip=True) if title_tag else ""

        # Цена (НМЦК)
        price_tag = card.select_one(
            "div.price-block__value, "
            "span.price, "
            "div.registry-entry__body-value.price"
        )
        price_text = price_tag.get_text(strip=True) if price_tag else ""
        price = _parse_price(price_text)

        # Регион заказчика
        region = None
        for block in card.select("div.registry-entry__body-href, span.region"):
            text = block.get_text(" ", strip=True)
            if "Регион" in text or "субъект" in text.lower():
                region = text.split(":")[-1].strip()
                break
        if not region:
            # Пробуем найти в адресе заказчика
            addr = card.select_one("div.registry-entry__body-value span")
            if addr:
                region = addr.get_text(strip=True)

        # Сроки подачи
        dates = card.select("div.data-block__value, span.date-value")
        deadline = dates[1].get_text(strip=True) if len(dates) > 1 else (
            dates[0].get_text(strip=True) if dates else None
        )

        # Статус
        status_tag = card.select_one(
            "div.registry-entry__header-top__title, "
            "span.label-primary, span.status"
        )
        status = status_tag.get_text(strip=True) if status_tag else "Активен"

        return {
            "tender_number": number,
            "title": title[:500] if title else "Без названия",
            "region": region[:200] if region else None,
            "price": price,
            "deadline": deadline,
            "status": status,
            "url": url,
        }
    except Exception as e:
        logger.debug("Ошибка разбора карточки: %s", e)
        return None


async def parse_eis_search(session: aiohttp.ClientSession,
                            keyword: str, page: int = 1) -> list[dict]:
    """Парсить страницу результатов ЕИС по ключевому слову."""
    params = {
        "searchString": keyword,
        "morphology": "on",
        "search-filter": "Дата+размещения",
        "pageNumber": page,
        "sortDirection": "false",
        "recordsPerPage": "_10",
        "showLotsInfoHidden": "false",
        "fz44": "on",
        "fz223": "on",
        "af": "on",
    }
    html = await fetch_with_retry(
        session,
        "https://zakupki.gov.ru/epz/order/extendedsearch/results.html",
        params=params
    )
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(
        "div.search-registry-entry-block, "
        "div.registry-entry__body, "
        "article.registry-entry"
    )

    results = []
    for card in cards:
        t = parse_tender_card(card)
        if t:
            results.append(t)

    return results


async def check_tender_status(session: aiohttp.ClientSession,
                               tender: dict) -> Optional[dict]:
    """
    Проверить статус тендера на странице ЕИС.
    Возвращает dict с итоговыми данными если тендер завершён.
    """
    if not tender.get("url"):
        return None

    html = await fetch_with_retry(session, tender["url"])
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Проверяем статус
    status_tag = soup.select_one(
        "span.navBreadcrumb, div.procurement-stage, "
        "span.label-default, div.col-6 span"
    )
    status_text = status_tag.get_text(strip=True).lower() if status_tag else ""

    completed_keywords = [
        "завершена", "завершён", "исполнение", "контракт заключён",
        "отменена", "несостоявшаяся", "итоги подведены"
    ]
    is_completed = any(kw in status_text for kw in completed_keywords)

    if not is_completed:
        return None

    # Ищем победителя и итоговую цену
    winner = None
    final_price = None

    # Итоговая цена из контракта
    for tag in soup.select("div.col, span.price, div.price-block"):
        text = tag.get_text(" ", strip=True)
        if any(w in text.lower() for w in ["цена контракта", "итоговая цена", "сумма контракта"]):
            price_match = re.search(r"[\d\s]+[,.]?\d*", text.replace("\u00a0", " "))
            if price_match:
                final_price = _parse_price(price_match.group())
                break

    # Победитель
    for tag in soup.select("div.col, span, div.winner"):
        text = tag.get_text(" ", strip=True)
        if any(w in text.lower() for w in ["победитель", "поставщик", "исполнитель"]):
            lines = [l.strip() for l in text.split("\n") if l.strip()]
            if len(lines) > 1:
                winner = lines[1][:300]
                break

    return {
        "final_price": final_price,
        "winner": winner,
        "finished_at": datetime.utcnow().isoformat(),
    }


# ═══════════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК
# ═══════════════════════════════════════════════════════════

async def run_update(bot: Bot):
    """Основной цикл: парсинг новых + обновление статусов."""
    logger.info("=== Обновление: %s ===", datetime.now().strftime("%d.%m.%Y %H:%M"))

    new_tenders: list[dict] = []
    seen_numbers: set[str] = set()

    async with aiohttp.ClientSession() as session:
        # 1. Поиск новых тендеров
        for keyword in SEARCH_KEYWORDS:
            for page in range(1, MAX_PAGES + 1):
                logger.info("ЕИС поиск: '%s', стр. %d", keyword, page)
                batch = await parse_eis_search(session, keyword, page)
                if not batch:
                    break
                for t in batch:
                    if t["tender_number"] not in seen_numbers:
                        seen_numbers.add(t["tender_number"])
                        is_new = await upsert_active_tender(t)
                        if is_new:
                            new_tenders.append(t)
                await asyncio.sleep(REQUEST_DELAY)

        # 2. Проверка статусов активных тендеров
        active = await get_active_tenders(limit=50)
        logger.info("Проверяем статусы %d активных тендеров", len(active))

        completed_tenders = []
        for t in active:
            result = await check_tender_status(session, t)
            if result:
                await move_to_finished(
                    t["tender_number"],
                    result.get("final_price"),
                    result.get("winner"),
                    result.get("finished_at"),
                )
                # Получаем перенесённый тендер для уведомления
                finished = await get_finished_tenders(limit=1)
                if finished and finished[0]["tender_number"] == t["tender_number"]:
                    completed_tenders.append(finished[0])
            await asyncio.sleep(REQUEST_DELAY)

    logger.info("Новых: %d, Завершённых: %d", len(new_tenders), len(completed_tenders))

    # 3. Уведомления подписчикам
    if new_tenders or completed_tenders:
        await notify_subscribers(bot, new_tenders, completed_tenders)


async def notify_subscribers(bot: Bot, new_tenders: list, completed_tenders: list):
    """Рассылка уведомлений."""
    from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

    subscribers = await get_subscribers()
    for chat_id in subscribers:
        # Новые тендеры
        if new_tenders:
            try:
                await bot.send_message(
                    chat_id,
                    f"🔔 <b>Новых тендеров: {len(new_tenders)}</b>",
                    parse_mode="HTML"
                )
                for t in new_tenders[:5]:  # максимум 5 в одном уведомлении
                    await bot.send_message(
                        chat_id,
                        format_active_card(t),
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
            except (TelegramForbiddenError, TelegramBadRequest):
                pass
            except Exception as e:
                logger.error("Ошибка уведомления %s: %s", chat_id, e)

        # Завершённые тендеры
        for t in completed_tenders[:3]:
            try:
                await bot.send_message(
                    chat_id,
                    "🏁 <b>Тендер завершён!</b>\n\n" + format_finished_card(t),
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.error("Ошибка уведомления завершённого %s: %s", chat_id, e)


# ═══════════════════════════════════════════════════════════
#  КОМАНДЫ БОТА
# ═══════════════════════════════════════════════════════════

router = Router()

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🆕 Новые тендеры"), KeyboardButton(text="🏁 Завершённые")],
        [KeyboardButton(text="📊 Сводка новых"), KeyboardButton(text="📊 Сводка завершённых")],
        [KeyboardButton(text="🔄 Обновить сейчас"), KeyboardButton(text="❓ Помощь")],
    ],
    resize_keyboard=True,
)


@router.message(Command("start"))
async def cmd_start(message: Message):
    await subscribe(message.chat.id)
    await message.answer(
        "👋 <b>Бот мониторинга тендеров ЕИС</b>\n\n"
        "Слежу за тендерами по металлолому и ломам на zakupki.gov.ru.\n\n"
        "<b>Команды:</b>\n"
        "/new — активные тендеры\n"
        "/finished — завершённые тендеры\n"
        "/new_summary — сводка по активным\n"
        "/finished_summary — сводка по завершённым\n"
        "/update — запустить обновление сейчас",
        parse_mode="HTML",
        reply_markup=MAIN_KEYBOARD,
    )


@router.message(Command("new"))
@router.message(lambda m: m.text == "🆕 Новые тендеры")
async def cmd_new(message: Message):
    tenders = await get_active_tenders(limit=10)
    if not tenders:
        await message.answer(
            "📭 Активных тендеров пока нет.\n"
            "Нажмите 🔄 Обновить сейчас или подождите — бот проверяет каждые 30 минут.",
            parse_mode="HTML"
        )
        return
    await message.answer(
        f"🆕 <b>Активные тендеры</b> ({len(tenders)} из базы):",
        parse_mode="HTML"
    )
    for t in tenders:
        await message.answer(
            format_active_card(t),
            parse_mode="HTML",
            disable_web_page_preview=True
        )


@router.message(Command("finished"))
@router.message(lambda m: m.text == "🏁 Завершённые")
async def cmd_finished(message: Message):
    tenders = await get_finished_tenders(limit=10)
    if not tenders:
        await message.answer(
            "📭 Завершённых тендеров пока нет.",
            parse_mode="HTML"
        )
        return
    await message.answer(
        f"🏁 <b>Завершённые тендеры</b> ({len(tenders)} из базы):",
        parse_mode="HTML"
    )
    for t in tenders:
        await message.answer(
            format_finished_card(t),
            parse_mode="HTML",
            disable_web_page_preview=True
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


@router.message(Command("update"))
@router.message(lambda m: m.text == "🔄 Обновить сейчас")
async def cmd_update(message: Message):
    await message.answer("🔄 Запускаю обновление... Займёт 5-15 минут.", parse_mode="HTML")
    asyncio.create_task(run_update(message.bot))


@router.message(Command("help"))
@router.message(lambda m: m.text == "❓ Помощь")
async def cmd_help(message: Message):
    await cmd_start(message)


@router.message(Command("search"))
async def cmd_search(message: Message, command: CommandObject):
    region = command.args
    if not region:
        await message.answer("ℹ️ Пример: <code>/search Москва</code>", parse_mode="HTML")
        return
    tenders = await get_active_tenders(limit=10, region=region)
    if not tenders:
        await message.answer(f"📭 Тендеры по региону <b>{region}</b> не найдены.", parse_mode="HTML")
        return
    await message.answer(f"🔍 <b>Тендеры: {region}</b> ({len(tenders)} шт.):", parse_mode="HTML")
    for t in tenders:
        await message.answer(format_active_card(t), parse_mode="HTML", disable_web_page_preview=True)


# ═══════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════

async def main():
    if BOT_TOKEN == "ВСТАВЬТЕ_ТОКЕН_СЮДА":
        logger.error("Укажите BOT_TOKEN в переменной окружения!")
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
        id="update_tenders",
    )
    scheduler.start()
    logger.info("Планировщик запущен (каждые %d мин)", UPDATE_INTERVAL_MINUTES)
    logger.info("Бот запущен ✅")

    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
