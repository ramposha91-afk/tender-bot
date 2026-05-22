"""
Telegram-бот мониторинга тендеров по металлолому — ОДИН ФАЙЛ
Установка: pip install aiogram aiohttp aiosqlite apscheduler beautifulsoup4 lxml
Запуск:    python bot_single.py
"""

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urljoin

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════
#  НАСТРОЙКИ — вставьте сюда ваш токен
# ═══════════════════════════════════════════════════════════

BOT_TOKEN = os.getenv("BOT_TOKEN", "ВСТАВЬТЕ_ТОКЕН_СЮДА")

DB_PATH = "tenders.db"
HISTORY_MONTHS = 6
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 1.5
MAX_PAGES = 3

SEARCH_KEYWORDS = [
    "металлолом",
    "лом черных металлов",
    "лом цветных металлов",
    "лом чермет",
    "лом алюминий",
    "отходы металла",
]

SOURCE_LABELS = {
    "rostender": "РосТендер",
    "synapse":   "Синапс",
    "kontur":    "Контур.Закупки",
}

# Synapse credentials (set via environment variables)
SYNAPSE_LOGIN = os.getenv("SYNAPSE_LOGIN", "")
SYNAPSE_PASS = os.getenv("SYNAPSE_PASS", "")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9",
}

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
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS tenders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT NOT NULL,
                source      TEXT NOT NULL,
                title       TEXT NOT NULL,
                region      TEXT,
                start_price REAL,
                published   TEXT,
                deadline    TEXT,
                url         TEXT,
                status      TEXT DEFAULT 'active',
                created_at  TEXT DEFAULT (datetime('now')),
                UNIQUE(external_id, source)
            );
            CREATE TABLE IF NOT EXISTS results (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_id   INTEGER NOT NULL,
                winner_name TEXT,
                winner_inn  TEXT,
                final_price REAL,
                start_price REAL,
                savings_pct REAL,
                completed   TEXT,
                created_at  TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS winners (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL,
                inn         TEXT,
                wins        INTEGER DEFAULT 1,
                total_value REAL DEFAULT 0,
                last_win    TEXT,
                UNIQUE(inn)
            );
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id   INTEGER PRIMARY KEY,
                alerts_on INTEGER DEFAULT 1,
                region    TEXT,
                joined_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS seen_notifications (
                chat_id   INTEGER NOT NULL,
                tender_id INTEGER NOT NULL,
                kind      TEXT NOT NULL,
                sent_at   TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (chat_id, tender_id, kind)
            );
            CREATE INDEX IF NOT EXISTS idx_tenders_created ON tenders(created_at);
            CREATE INDEX IF NOT EXISTS idx_tenders_region  ON tenders(region);
        """)
        await db.commit()


async def upsert_tender(t: dict) -> tuple[int, bool]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id FROM tenders WHERE external_id=? AND source=?",
            (t["external_id"], t["source"]),
        )
        row = await cur.fetchone()
        if row:
            await db.execute(
                "UPDATE tenders SET title=?,region=?,start_price=?,published=?,deadline=?,url=?,status=? WHERE id=?",
                (t.get("title"), t.get("region"), t.get("start_price"),
                 t.get("published"), t.get("deadline"), t.get("url"),
                 t.get("status", "active"), row[0]),
            )
            await db.commit()
            return row[0], False
        cur2 = await db.execute(
            "INSERT INTO tenders (external_id,source,title,region,start_price,published,deadline,url,status) VALUES (?,?,?,?,?,?,?,?,?)",
            (t["external_id"], t["source"], t.get("title"), t.get("region"),
             t.get("start_price"), t.get("published"), t.get("deadline"),
             t.get("url"), t.get("status", "active")),
        )
        await db.commit()
        return cur2.lastrowid, True


async def get_new_tenders(hours=24, region=None) -> list[dict]:
    since = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if region:
            cur = await db.execute(
                "SELECT * FROM tenders WHERE created_at>=? AND region LIKE ? ORDER BY created_at DESC LIMIT 50",
                (since, f"%{region}%"),
            )
        else:
            cur = await db.execute(
                "SELECT * FROM tenders WHERE created_at>=? ORDER BY created_at DESC LIMIT 50",
                (since,),
            )
        return [dict(r) for r in await cur.fetchall()]


async def get_completed_results(limit=10, region=None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if region:
            cur = await db.execute(
                "SELECT r.*,t.title,t.region,t.url FROM results r JOIN tenders t ON r.tender_id=t.id WHERE t.region LIKE ? ORDER BY r.completed DESC LIMIT ?",
                (f"%{region}%", limit),
            )
        else:
            cur = await db.execute(
                "SELECT r.*,t.title,t.region,t.url FROM results r JOIN tenders t ON r.tender_id=t.id ORDER BY r.completed DESC LIMIT ?",
                (limit,),
            )
        return [dict(r) for r in await cur.fetchall()]


async def get_weekly_summary() -> dict:
    since = (datetime.utcnow() - timedelta(days=7)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        c1 = await db.execute(
            "SELECT COUNT(*) as cnt, SUM(start_price) as total FROM tenders WHERE created_at>=?", (since,)
        )
        r1 = dict(await c1.fetchone())
        c2 = await db.execute(
            "SELECT AVG(final_price) as avg FROM results r JOIN tenders t ON r.tender_id=t.id WHERE t.created_at>=?", (since,)
        )
        r2 = dict(await c2.fetchone())
        c3 = await db.execute("SELECT name,wins FROM winners ORDER BY wins DESC LIMIT 5")
        top = [dict(r) for r in await c3.fetchall()]
        return {"count": r1["cnt"] or 0, "total_volume": r1["total"] or 0,
                "avg_price": r2["avg"] or 0, "top_winners": top}


async def subscribe(chat_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
        await db.commit()


async def set_alerts(chat_id: int, on: bool):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subscribers (chat_id,alerts_on) VALUES (?,?) ON CONFLICT(chat_id) DO UPDATE SET alerts_on=excluded.alerts_on",
            (chat_id, 1 if on else 0),
        )
        await db.commit()


async def get_alert_subscribers() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT chat_id,region FROM subscribers WHERE alerts_on=1")
        return [dict(r) for r in await cur.fetchall()]


async def mark_notified(chat_id, tender_id, kind):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO seen_notifications (chat_id,tender_id,kind) VALUES (?,?,?)",
            (chat_id, tender_id, kind),
        )
        await db.commit()


async def is_notified(chat_id, tender_id, kind) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT 1 FROM seen_notifications WHERE chat_id=? AND tender_id=? AND kind=?",
            (chat_id, tender_id, kind),
        )
        return await cur.fetchone() is not None


# ═══════════════════════════════════════════════════════════
#  ФОРМАТИРОВАНИЕ
# ═══════════════════════════════════════════════════════════

def fmt_price(price) -> str:
    if price is None:
        return "не указана"
    if price >= 1_000_000:
        return f"{price/1_000_000:.2f} млн руб."
    if price >= 1_000:
        return f"{price/1_000:.1f} тыс. руб."
    return f"{price:.2f} руб."


def format_tender_card(t: dict) -> str:
    source = SOURCE_LABELS.get(t.get("source", ""), t.get("source", "—"))
    status_map = {"active": "🟢 Активен", "completed": "🏁 Завершён", "cancelled": "❌ Отменён"}
    lines = [
        f"🏭 <b>Название:</b> {t.get('title') or '—'}",
        f"📍 <b>Регион:</b> {t.get('region') or 'не указан'}",
        f"💰 <b>Начальная цена:</b> {fmt_price(t.get('start_price'))}",
        f"⏰ <b>Срок подачи:</b> {t.get('deadline') or 'не указан'}",
        f"🏛 <b>Площадка:</b> {source}",
        f"📌 <b>Статус:</b> {status_map.get(t.get('status',''), t.get('status',''))}",
    ]
    if t.get("url"):
        lines.append(f'🔗 <a href="{t["url"]}">Открыть тендер</a>')
    return "\n".join(lines)


def format_result_card(r: dict) -> str:
    lines = [
        "🏁 <b>Тендер завершён</b>",
        "",
        f"🏭 <b>Название:</b> {r.get('title') or '—'}",
        f"📍 <b>Регион:</b> {r.get('region') or 'не указан'}",
        f"🏆 <b>Победитель:</b> {r.get('winner_name') or 'не известен'}",
        f"💰 <b>Начальная цена:</b> {fmt_price(r.get('start_price'))}",
        f"✅ <b>Итоговая цена:</b> {fmt_price(r.get('final_price'))}",
        f"📉 <b>Экономия:</b> {str(round(r['savings_pct'],1))+'%' if r.get('savings_pct') else '—'}",
        f"📅 <b>Завершён:</b> {r.get('completed') or '—'}",
    ]
    if r.get("url"):
        url = r["url"]
        lines.append(f'🔗 <a href="{url}">Открыть тендер</a>')
    return "\n".join(lines)


def format_weekly_summary(data: dict) -> str:
    lines = [
        "📊 <b>Сводка за последнюю неделю</b>", "",
        f"📋 Всего тендеров: <b>{data['count']}</b>",
        f"💼 Общий объём: <b>{fmt_price(data['total_volume'])}</b>",
        f"⚖️ Средняя итоговая цена: <b>{fmt_price(data['avg_price'])}</b>",
    ]
    if data["top_winners"]:
        lines += ["", "🏆 <b>Топ-5 победителей:</b>"]
        for i, w in enumerate(data["top_winners"], 1):
            lines.append(f"  {i}. {w['name']} — {w['wins']} побед")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
#  ПАРСЕРЫ
# ═══════════════════════════════════════════════════════════

def _clean_price(text) -> Optional[float]:
    if not text:
        return None
    try:
        return float(
            str(text).replace("\u00a0","").replace(" ","")
            .replace(",",".").replace("руб.","").replace("₽","").strip()
        )
    except ValueError:
        return None



# ═══════════════════════════════════════════════════════════
#  ПАРСЕРЫ
# ═══════════════════════════════════════════════════════════

def _clean_price(text) -> float | None:
    if not text:
        return None
    try:
        return float(
            str(text).replace("\u00a0","").replace(" ","")
            .replace(",",".").replace("руб.","").replace("₽","").strip()
        )
    except ValueError:
        return None


# ── rostender.info ──────────────────────────────────────────

async def parse_rostender() -> list[dict]:
    results, seen = [], set()
    urls = [
        "https://rostender.info/tendery-metallicheskie-othody-i-lom",
        "https://rostender.info/category/tendery-lom-chernyh-metallov",
        "https://rostender.info/category/tendery-vyvoz-metalloloma",
    ]
    async with aiohttp.ClientSession() as session:
        for url in urls:
            for page in range(1, MAX_PAGES + 1):
                try:
                    page_url = url if page == 1 else f"{url}?page={page}"
                    async with session.get(
                        page_url, headers=HEADERS,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                    ) as resp:
                        if resp.status != 200:
                            break
                        html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    # Ищем все ссылки на тендеры
                    links = soup.select("a[href*='/tender/']")
                    if not links:
                        break
                    for link in links:
                        try:
                            href = link.get("href", "")
                            if not href or "login" in href:
                                continue
                            full_url = href if href.startswith("http") else "https://rostender.info" + href
                            number = href.rstrip("/").split("/")[-1]
                            eid = f"rostender_{number}"
                            if eid in seen or not number.isdigit():
                                continue
                            seen.add(eid)
                            title = link.get_text(strip=True) or "Тендер на металлолом"
                            if len(title) < 5:
                                continue
                            results.append({
                                "external_id": eid, "source": "rostender",
                                "title": title, "region": None,
                                "start_price": None, "published": None,
                                "deadline": None, "url": full_url, "status": "active",
                            })
                        except Exception:
                            pass
                    await asyncio.sleep(REQUEST_DELAY)
                except Exception as e:
                    logger.error("rostender error: %s", e)
                    break
    logger.info("rostender: %d тендеров", len(results))
    return results


# ── synapsenet.ru (с авторизацией) ─────────────────────────

async def parse_synapse() -> list[dict]:
    results, seen = [], set()
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar()) as session:
        # Авторизация
        if SYNAPSE_LOGIN and SYNAPSE_PASS:
            try:
                login_data = {"email": SYNAPSE_LOGIN, "password": SYNAPSE_PASS}
                await session.post(
                    "https://synapsenet.ru/login",
                    data=login_data, headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                )
                logger.info("Synapse: авторизация выполнена")
            except Exception as e:
                logger.error("Synapse login error: %s", e)

        # Парсинг активных тендеров
        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"page": page}
                async with session.get(
                    "https://synapsenet.ru/search/category/metallolom",
                    params=params, headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status != 200:
                        break
                    html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                # Ищем карточки тендеров
                cards = soup.select("div.tender-card, div.search-result-item, tr.tender")
                links = soup.select("a[href*='/zakupka/'], a[href*='/procedure/']")
                items = cards if cards else links
                if not items:
                    break
                for item in items:
                    try:
                        link = item if item.name == "a" else item.select_one("a[href]")
                        if not link:
                            continue
                        href = link.get("href", "")
                        if not href:
                            continue
                        full_url = href if href.startswith("http") else "https://synapsenet.ru" + href
                        number = href.rstrip("/").split("/")[-1]
                        eid = f"synapse_{number}"
                        if eid in seen:
                            continue
                        seen.add(eid)
                        title = link.get_text(strip=True) or "Тендер на металлолом"
                        if len(title) < 5:
                            continue
                        # Цена
                        price_tag = item.select_one("span.price, .nmck, .tender-price") if item.name != "a" else None
                        price = _clean_price(price_tag.get_text(strip=True) if price_tag else None)
                        # Регион
                        region_tag = item.select_one("span.region, .location") if item.name != "a" else None
                        region = region_tag.get_text(strip=True) if region_tag else None
                        # Статус
                        status_tag = item.select_one("span.status, .tender-status") if item.name != "a" else None
                        status_text = status_tag.get_text(strip=True).lower() if status_tag else ""
                        status = "completed" if any(w in status_text for w in ["завершён", "закрыт", "итоги"]) else "active"
                        results.append({
                            "external_id": eid, "source": "synapse",
                            "title": title, "region": region,
                            "start_price": price, "published": None,
                            "deadline": None, "url": full_url, "status": status,
                        })
                    except Exception:
                        pass
                await asyncio.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.error("synapse error: %s", e)
                break

        # Парсинг завершённых тендеров
        if SYNAPSE_LOGIN and SYNAPSE_PASS:
            for page in range(1, 3):
                try:
                    params = {"page": page, "phase": "finished"}
                    async with session.get(
                        "https://synapsenet.ru/search/category/metallolom",
                        params=params, headers=HEADERS,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                    ) as resp:
                        if resp.status != 200:
                            break
                        html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    links = soup.select("a[href*='/zakupka/'], a[href*='/procedure/']")
                    for link in links:
                        try:
                            href = link.get("href", "")
                            if not href:
                                continue
                            full_url = href if href.startswith("http") else "https://synapsenet.ru" + href
                            number = href.rstrip("/").split("/")[-1]
                            eid = f"synapse_done_{number}"
                            if eid in seen:
                                continue
                            seen.add(eid)
                            title = link.get_text(strip=True) or "Завершённый тендер"
                            if len(title) < 5:
                                continue
                            results.append({
                                "external_id": eid, "source": "synapse",
                                "title": title, "region": None,
                                "start_price": None, "published": None,
                                "deadline": None, "url": full_url, "status": "completed",
                            })
                        except Exception:
                            pass
                    await asyncio.sleep(REQUEST_DELAY)
                except Exception as e:
                    logger.error("synapse completed error: %s", e)
                    break

    logger.info("synapse: %d тендеров", len(results))
    return results


# ── komtender.ru ─────────────────────────────────────────────

async def parse_kontur() -> list[dict]:
    results, seen = [], set()
    base = "https://www.komtender.ru/tendery-metallicheskie-othody-i-lom"
    async with aiohttp.ClientSession() as session:
        for page in range(1, MAX_PAGES + 1):
            try:
                page_url = base if page == 1 else f"{base}?page={page}"
                async with session.get(
                    page_url, headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                ) as resp:
                    if resp.status != 200:
                        break
                    html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                links = soup.select("a[href*='/tender/']")
                if not links:
                    break
                for link in links:
                    try:
                        href = link.get("href", "")
                        if not href or "login" in href or "registration" in href:
                            continue
                        full_url = href if href.startswith("http") else "https://www.komtender.ru" + href
                        number = href.rstrip("/").split("/")[-1]
                        eid = f"komtender_{number}"
                        if eid in seen:
                            continue
                        seen.add(eid)
                        title = link.get_text(strip=True) or "Тендер на металлолом"
                        if len(title) < 5:
                            continue
                        results.append({
                            "external_id": eid, "source": "kontur",
                            "title": title, "region": None,
                            "start_price": None, "published": None,
                            "deadline": None, "url": full_url, "status": "active",
                        })
                    except Exception:
                        pass
                await asyncio.sleep(REQUEST_DELAY)
            except Exception as e:
                logger.error("kontur error: %s", e)
                break
    logger.info("komtender: %d тендеров", len(results))
    return results

# ═══════════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК + УВЕДОМЛЕНИЯ
# ═══════════════════════════════════════════════════════════

async def run_all_parsers(bot: Bot):
    logger.info("=== Запуск парсеров: %s ===", datetime.now().strftime("%d.%m.%Y %H:%M"))
    all_tenders = []
    for parser in [parse_rostender, parse_synapse, parse_kontur]:
        try:
            all_tenders.extend(await parser())
        except Exception as e:
            logger.error("Парсер упал: %s", e)

    new_ones = []
    for t in all_tenders:
        tid, is_new = await upsert_tender(t)
        if is_new:
            t["_db_id"] = tid
            new_ones.append(t)

    logger.info("Новых тендеров: %d", len(new_ones))
    if new_ones:
        await notify_subscribers(bot, new_ones)


async def notify_subscribers(bot: Bot, new_tenders: list[dict]):
    from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
    subscribers = await get_alert_subscribers()
    for sub in subscribers:
        chat_id = sub["chat_id"]
        region_filter = sub.get("region")
        for t in new_tenders:
            if region_filter and t.get("region"):
                if region_filter.lower() not in t["region"].lower():
                    continue
            tid = t.get("_db_id")
            if not tid or await is_notified(chat_id, tid, "new"):
                continue
            try:
                await bot.send_message(
                    chat_id,
                    "🔔 <b>Новый тендер!</b>\n\n" + format_tender_card(t),
                    parse_mode="HTML", disable_web_page_preview=True,
                )
                await mark_notified(chat_id, tid, "new")
            except (TelegramForbiddenError, TelegramBadRequest):
                pass
            except Exception as e:
                logger.error("Ошибка уведомления: %s", e)


# ═══════════════════════════════════════════════════════════
#  КОМАНДЫ БОТА
# ═══════════════════════════════════════════════════════════

router = Router()

MAIN_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🆕 Новые тендеры"), KeyboardButton(text="🏁 Результаты")],
        [KeyboardButton(text="📊 Сводка за неделю"), KeyboardButton(text="🔔 Уведомления вкл")],
        [KeyboardButton(text="🔕 Уведомления выкл"), KeyboardButton(text="❓ Помощь")],
    ],
    resize_keyboard=True,
)


@router.message(Command("start"))
async def cmd_start(message: Message):
    await subscribe(message.chat.id)
    await message.answer(
        "👋 <b>Бот мониторинга тендеров по металлолому</b>\n\n"
        "Отслеживаю площадки:\n"
        "• 🏛 Госзакупки (zakupki.gov.ru)\n"
        "• 🔵 РТС-тендер\n"
        "• 🟡 ЭТП ГПБ\n\n"
        "<b>Команды:</b>\n"
        "/new — новые тендеры за 24 часа\n"
        "/results — завершённые тендеры\n"
        "/summary — сводка за неделю\n"
        "/search [регион] — поиск по региону\n"
        "/alerts on | off — уведомления",
        parse_mode="HTML", reply_markup=MAIN_KEYBOARD,
    )


@router.message(Command("help"))
async def cmd_help(message: Message):
    await cmd_start(message)


@router.message(Command("new"))
async def cmd_new(message: Message):
    tenders = await get_new_tenders(hours=24)
    if not tenders:
        await message.answer("📭 За последние 24 часа новых тендеров нет.\nПроверка идёт каждые 2 часа.", parse_mode="HTML")
        return
    await message.answer(f"🆕 <b>Новые тендеры за 24 часа</b> ({len(tenders)} шт.):", parse_mode="HTML")
    for t in tenders[:15]:
        await message.answer(format_tender_card(t), parse_mode="HTML", disable_web_page_preview=True)
    if len(tenders) > 15:
        await message.answer(f"ℹ️ Показано 15 из {len(tenders)}. Используйте /search для фильтрации.")


@router.message(Command("results"))
async def cmd_results(message: Message):
    results = await get_completed_results(limit=10)
    if not results:
        await message.answer("📭 Завершённых тендеров пока нет.", parse_mode="HTML")
        return
    await message.answer(f"🏁 <b>Последние завершённые тендеры</b>:", parse_mode="HTML")
    for r in results:
        await message.answer(format_result_card(r), parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("summary"))
async def cmd_summary(message: Message):
    data = await get_weekly_summary()
    await message.answer(format_weekly_summary(data), parse_mode="HTML")


@router.message(Command("search"))
async def cmd_search(message: Message, command: CommandObject):
    region = command.args
    if not region:
        await message.answer("ℹ️ Пример: <code>/search Москва</code>", parse_mode="HTML")
        return
    tenders = await get_new_tenders(hours=24*7, region=region)
    if not tenders:
        await message.answer(f"📭 Тендеры по региону <b>{region}</b> не найдены.", parse_mode="HTML")
        return
    await message.answer(f"🔍 <b>Тендеры: {region}</b> ({len(tenders)} шт.):", parse_mode="HTML")
    for t in tenders[:10]:
        await message.answer(format_tender_card(t), parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("alerts"))
async def cmd_alerts(message: Message, command: CommandObject):
    arg = (command.args or "").lower()
    if arg == "on":
        await set_alerts(message.chat.id, True)
        await message.answer("🔔 <b>Уведомления включены!</b>", parse_mode="HTML")
    elif arg == "off":
        await set_alerts(message.chat.id, False)
        await message.answer("🔕 <b>Уведомления выключены.</b>", parse_mode="HTML")
    else:
        await message.answer("ℹ️ Используйте:\n<code>/alerts on</code>\n<code>/alerts off</code>", parse_mode="HTML")


# Кнопки клавиатуры
@router.message(lambda m: m.text == "🆕 Новые тендеры")
async def btn_new(message: Message): await cmd_new(message)

@router.message(lambda m: m.text == "🏁 Результаты")
async def btn_results(message: Message): await cmd_results(message)

@router.message(lambda m: m.text == "📊 Сводка за неделю")
async def btn_summary(message: Message): await cmd_summary(message)

@router.message(lambda m: m.text == "❓ Помощь")
async def btn_help(message: Message): await cmd_start(message)

@router.message(Command("parse"))
async def cmd_parse(message: Message):
    await message.answer("🔄 Запускаю парсинг всех площадок... Займёт 10-15 минут.", parse_mode="HTML")
    asyncio.create_task(run_all_parsers(message.bot))

@router.message(lambda m: m.text == "🔔 Уведомления вкл")
async def btn_alerts_on(message: Message):
    await set_alerts(message.chat.id, True)
    await message.answer("🔔 <b>Уведомления включены!</b>", parse_mode="HTML")

@router.message(lambda m: m.text == "🔕 Уведомления выкл")
async def btn_alerts_off(message: Message):
    await set_alerts(message.chat.id, False)
    await message.answer("🔕 <b>Уведомления выключены.</b>", parse_mode="HTML")


# ═══════════════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════════════

async def main():
    if BOT_TOKEN == "ВСТАВЬТЕ_ТОКЕН_СЮДА":
        print("❌ Укажите BOT_TOKEN в переменной окружения или в файле!")
        return

    await init_db()
    logger.info("БД инициализирована")

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow", job_defaults={"misfire_grace_time": 3600})
    scheduler.add_job(
        run_all_parsers, "interval", hours=2,
        args=[bot], next_run_time=datetime.now(),
        id="parse_tenders",
    )
    scheduler.start()
    logger.info("Планировщик запущен")
    # Запускаем первый парсинг сразу в фоне
    asyncio.create_task(run_all_parsers(bot))

    logger.info("Бот запущен ✅")
    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
