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
import csv
import io
from datetime import datetime, timedelta
from typing import Optional

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ═══════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "ВСТАВЬТЕ_ТОКЕН_СЮДА")
TENDERPLAN_TOKEN: str = os.getenv("TENDERPLAN_TOKEN", "ВСТАВЬТЕ_TENDERPLAN_TOKEN")

DB_PATH: str = "tenders.db"
UPDATE_INTERVAL_MINUTES: int = 30

# Ключевые слова для поиска
SEARCH_KEYWORDS: list[str] = [
    "лом алюминия",
    "лом меди",
    "лом латуни",
    "лом черных металлов",
    "стружка алюминиевая",
    "стружка медная",
    "стружка латунная",
    "отходы цветных металлов",
    "отходы черных металлов"
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

# Защита от одновременных ручных обновлений /update
update_lock: asyncio.Lock = asyncio.Lock()

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
    """Строгий фильтр: оставляем именно лом/металлолом/отходы металлов/стружку.

    Tenderplan по широким ключам возвращает много чужого: поставку металлопроката,
    ремонт, строительные работы, инвентарь и т.п. Поэтому здесь проверяем само
    название тендера, а не только факт, что API нашёл его по слову.
    """
    if not title:
        return False

    t = title.lower().replace("ё", "е")
    t = re.sub(r"\s+", " ", t)

    # Явно чужие тематики
    hard_forbidden = [
        "молоко", "молочн", "мясо", "рыба", "продукт", "питани",
        "медицин", "лекарств", "фармацевт", "посуда", "канцеляр",
        "квартир", "жилое", "недвижим", "земельн", "автомобил",
        "шиномонтаж", "шлифмашин", "сверл", "шуруповерт", "инвентар",
        "лакокрас", "краска", "пожарн", "ремонт", "строительно-монтаж",
        "строительн", "поставка труб", "труба", "металлопрокат",
    ]
    if any(w in t for w in hard_forbidden):
        return False

    # Самые надежные признаки металлолома
    strong_patterns = [
        r"\bметаллолом\w*",
        r"\bлом\s+(черн|цветн|алюмин|мед|латун|бронз|нержав|свинц|цинк|металл)",
        r"\bлома\s+(черн|цветн|алюмин|мед|латун|бронз|нержав|свинц|цинк|металл)",
        r"\bчермет\w*",
        r"\bцветмет\w*",
        r"стружк\w*.*(металл|алюмин|мед|латун|чугун|сталь)",
        r"(металл|алюмин|мед|латун|чугун|сталь).*стружк\w*",
        r"отход\w*\s+(черн|цветн|алюмин|мед|латун|бронз|нержав|свинц|цинк|металл)",
        r"(черн|цветн|алюмин|мед|латун|бронз|нержав|свинц|цинк|металл)\w*\s+отход\w*",
        r"реализац\w*\s+(лом|металлолом|отход\w*\s+металл|стружк)",
        r"прием\w*\s+(лом|металлолом)",
        r"вывоз\w*\s+(лом|металлолом|отход\w*\s+металл|стружк)",
    ]
    if any(re.search(p, t) for p in strong_patterns):
        return True

    # Демонтаж металлоконструкций оставляем только когда он похож на получение/вывоз лома
    if "демонтаж" in t and "металлоконструкц" in t:
        return any(w in t for w in ["лом", "металлолом", "вывоз", "утилизац", "реализац", "отход"])

    return False


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



async def get_market_analytics() -> dict:
    """Аналитика по локальной базе: активные + завершённые."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        active_row = dict(await (await db.execute(
            "SELECT COUNT(*) cnt, COALESCE(SUM(price),0) total, COALESCE(AVG(price),0) avg_price FROM active_tenders"
        )).fetchone())
        finished_row = dict(await (await db.execute(
            "SELECT COUNT(*) cnt, COALESCE(SUM(start_price),0) total, COALESCE(AVG(final_price),0) avg_final FROM finished_tenders"
        )).fetchone())

        by_keyword = [dict(r) for r in await (await db.execute(
            """SELECT COALESCE(keyword,'не указано') keyword, COUNT(*) cnt, COALESCE(SUM(price),0) total
               FROM active_tenders
               GROUP BY COALESCE(keyword,'не указано')
               ORDER BY cnt DESC, total DESC
               LIMIT 12"""
        )).fetchall()]

        by_region = [dict(r) for r in await (await db.execute(
            """SELECT COALESCE(NULLIF(region,''),'не указан') region, COUNT(*) cnt, COALESCE(SUM(price),0) total
               FROM active_tenders
               GROUP BY COALESCE(NULLIF(region,''),'не указан')
               ORDER BY cnt DESC, total DESC
               LIMIT 12"""
        )).fetchall()]

        by_status_active = [dict(r) for r in await (await db.execute(
            """SELECT COALESCE(status,'не указан') status, COUNT(*) cnt, COALESCE(SUM(price),0) total
               FROM active_tenders
               GROUP BY COALESCE(status,'не указан')
               ORDER BY cnt DESC"""
        )).fetchall()]

        # Месяц берём из published_at, если его нет — из deadline/updated_at.
        rows = [dict(r) for r in await (await db.execute(
            "SELECT published_at, deadline, updated_at, price FROM active_tenders"
        )).fetchall()]

        month_map: dict[str, dict] = {}
        for r in rows:
            raw = r.get('published_at') or r.get('deadline') or r.get('updated_at') or ''
            month = extract_month_label(str(raw))
            if not month:
                month = 'дата не указана'
            item = month_map.setdefault(month, {'month': month, 'cnt': 0, 'total': 0.0})
            item['cnt'] += 1
            item['total'] += float(r.get('price') or 0)

        by_month = sorted(month_map.values(), key=lambda x: x['month'])[-12:]

        price_buckets = [
            ('0–100 тыс', 0, 100_000),
            ('100 тыс–1 млн', 100_000, 1_000_000),
            ('1–10 млн', 1_000_000, 10_000_000),
            ('10–100 млн', 10_000_000, 100_000_000),
            ('100 млн+', 100_000_000, None),
            ('без цены', None, None),
        ]
        bucket_counts = {b[0]: {'bucket': b[0], 'cnt': 0, 'total': 0.0} for b in price_buckets}
        for r in await (await db.execute("SELECT price FROM active_tenders")).fetchall():
            price = r['price']
            if price is None:
                bucket_counts['без цены']['cnt'] += 1
                continue
            price = float(price)
            placed = False
            for name, lo, hi in price_buckets[:-1]:
                if price >= lo and (hi is None or price < hi):
                    bucket_counts[name]['cnt'] += 1
                    bucket_counts[name]['total'] += price
                    placed = True
                    break
            if not placed:
                bucket_counts['без цены']['cnt'] += 1

        return {
            'active': active_row,
            'finished': finished_row,
            'by_keyword': by_keyword,
            'by_region': by_region,
            'by_status_active': by_status_active,
            'by_month': by_month,
            'price_buckets': list(bucket_counts.values()),
        }


def extract_month_label(raw: str) -> Optional[str]:
    """Пытается привести разные строки дат к YYYY-MM."""
    if not raw:
        return None
    raw = raw.strip()
    # ISO: 2026-05-24...
    m = re.search(r"(20\d{2})[-.](\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # RU: 24.05.2026
    m = re.search(r"\b\d{2}\.(\d{2})\.(20\d{2})", raw)
    if m:
        return f"{m.group(2)}-{m.group(1)}"
    return None


def bar(cnt: int, max_cnt: int, width: int = 12) -> str:
    if max_cnt <= 0:
        return ""
    n = max(1, round(cnt / max_cnt * width)) if cnt else 0
    return "█" * n


def format_market_analytics(data: dict) -> str:
    active = data['active']
    finished = data['finished']
    total_cnt = int(active['cnt'] or 0) + int(finished['cnt'] or 0)
    total_sum = float(active['total'] or 0) + float(finished['total'] or 0)

    lines = [
        "📊 <b>Аналитика по тендерам</b>",
        "",
        f"Всего в базе: <b>{total_cnt}</b>",
        f"Актуальных: <b>{int(active['cnt'] or 0)}</b> на сумму <b>{fmt_price(active['total'])}</b>",
        f"Завершённых: <b>{int(finished['cnt'] or 0)}</b> на сумму <b>{fmt_price(finished['total'])}</b>",
        f"Средняя НМЦ актуальных: <b>{fmt_price(active['avg_price'])}</b>",
        "",
    ]

    if data.get('by_keyword'):
        max_cnt = max(int(x['cnt']) for x in data['by_keyword']) or 1
        lines += ["🔑 <b>Топ по направлениям:</b>"]
        for x in data['by_keyword'][:8]:
            lines.append(f"{bar(int(x['cnt']), max_cnt)} {x['keyword']} — {x['cnt']} / {fmt_price(x['total'])}")
        lines.append("")

    if data.get('by_region'):
        max_cnt = max(int(x['cnt']) for x in data['by_region']) or 1
        lines += ["📍 <b>Топ регионов:</b>"]
        for x in data['by_region'][:8]:
            lines.append(f"{bar(int(x['cnt']), max_cnt)} {x['region']} — {x['cnt']} / {fmt_price(x['total'])}")
        lines.append("")

    if data.get('price_buckets'):
        max_cnt = max(int(x['cnt']) for x in data['price_buckets']) or 1
        lines += ["💰 <b>Диапазоны НМЦ:</b>"]
        for x in data['price_buckets']:
            if int(x['cnt']) > 0:
                lines.append(f"{bar(int(x['cnt']), max_cnt)} {x['bucket']} — {x['cnt']}")
        lines.append("")

    if data.get('by_month'):
        max_cnt = max(int(x['cnt']) for x in data['by_month']) or 1
        lines += ["📅 <b>Динамика по месяцам:</b>"]
        for x in data['by_month'][-8:]:
            lines.append(f"{bar(int(x['cnt']), max_cnt)} {x['month']} — {x['cnt']} / {fmt_price(x['total'])}")

    return "\n".join(lines)


async def build_tenders_csv() -> bytes:
    """CSV-выгрузка активных тендеров, открывается в Excel."""
    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['id', 'Название', 'Регион', 'Направление', 'Цена', 'Срок подачи', 'Статус', 'Ссылка'])
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            "SELECT tender_id,title,region,keyword,price,deadline,status,url FROM active_tenders ORDER BY updated_at DESC"
        )).fetchall()
        for r in rows:
            writer.writerow([
                r['tender_id'], r['title'], r['region'] or '', r['keyword'] or '',
                r['price'] if r['price'] is not None else '', r['deadline'] or '',
                r['status'] or '', r['url'] or ''
            ])
    return ('\ufeff' + output.getvalue()).encode('utf-8')

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


def fmt_ts(value) -> Optional[str]:
    """Timestamp/date string → строка даты."""
    if not value:
        return None
    try:
        if isinstance(value, str):
            raw = value.strip()
            if raw.isdigit():
                value = int(raw)
            else:
                # Если API уже дал дату строкой — оставляем её читаемо.
                return raw.replace("T", " ")[:16]
        if isinstance(value, (int, float)):
            # 13 цифр = миллисекунды, 10 цифр = секунды
            ts = value / 1000 if value > 10_000_000_000 else value
            return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return None
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




def first_value(data: dict, keys: list[str]):
    """Вернуть первое непустое значение из списка возможных ключей API."""
    for key in keys:
        if key in data and data.get(key) not in (None, "", []):
            return data.get(key)
    return None


def normalize_tender_items(data):
    """Tenderplan может возвращать список или dict с разными ключами результатов."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key in ("tenders", "items", "results", "data", "list", "rows", "documents"):
        value = data.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            for inner_key in ("tenders", "items", "results", "data", "list", "rows"):
                inner_value = value.get(inner_key)
                if isinstance(inner_value, list):
                    return inner_value
    return []

# ═══════════════════════════════════════════════════════════
#  TENDERPLAN API
# ═══════════════════════════════════════════════════════════

def get_headers() -> dict:
    # Tenderplan API требует access_token в параметрах запроса/теле запроса.
    # Bearer Authorization здесь не используем, иначе API возвращает HTTP 401.
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


async def tp_search(session: aiohttp.ClientSession,
                    keyword: str, page: int = 1, count: int = 50) -> list[dict]:
    """Поиск тендеров через Tenderplan API с диагностикой формата ответа."""
    payload = {
        "access_token": TENDERPLAN_TOKEN,
        "text": keyword,
        "page": page,
        "count": count,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.post(
                f"{TENDERPLAN_API}/search/list",
                json=payload,
                headers=get_headers(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                body = await resp.text()
                if resp.status == 200:
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        logger.warning("Tenderplan '%s': HTTP 200, но ответ не JSON: %s", keyword, body[:500])
                        return []

                    items = normalize_tender_items(data)
                    if isinstance(data, dict):
                        logger.info("Tenderplan '%s': HTTP 200, ключи ответа=%s, найдено raw=%d",
                                    keyword, list(data.keys())[:12], len(items))
                    else:
                        logger.info("Tenderplan '%s': HTTP 200, ответ list, найдено raw=%d", keyword, len(items))

                    if items:
                        logger.info("Tenderplan '%s': пример полей 1-го тендера=%s",
                                    keyword, list(items[0].keys())[:20] if isinstance(items[0], dict) else type(items[0]))
                    return items
                elif resp.status == 429:
                    logger.warning("Rate limit Tenderplan, ждём 10 сек")
                    await asyncio.sleep(10)
                else:
                    logger.warning("Tenderplan search '%s': HTTP %s, body: %s", keyword, resp.status, body[:700])
                    return []
        except asyncio.TimeoutError:
            logger.warning("Таймаут Tenderplan (попытка %d)", attempt)
        except Exception as e:
            logger.warning("Ошибка Tenderplan: %s", e)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)
    return []


async def tp_get_tender(session: aiohttp.ClientSession, tender_id: str) -> Optional[dict]:
    """Получить полную информацию о тендере включая контракты."""
    try:
        async with session.get(
            f"{TENDERPLAN_API}/tenders/v2/fullinfo",
            params={"access_token": TENDERPLAN_TOKEN, "id": tender_id},
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
            params={"access_token": TENDERPLAN_TOKEN, "id": tender_id},
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
    """Преобразовать ответ API в наш формат. Поддерживает несколько вариантов названий полей."""
    if not isinstance(item, dict):
        return None

    tid = first_value(item, [
        "_id", "id", "tenderId", "tender_id", "noticeId", "purchaseId",
        "number", "registryNumber", "notificationNumber", "purchaseNumber"
    ])
    if not tid:
        logger.info("Пропуск: не нашёл ID. Поля=%s", list(item.keys())[:20])
        return None
    tid = str(tid)

    title = clean_html(str(first_value(item, [
        "orderName", "name", "title", "purchaseName", "objectName", "subject",
        "lotName", "tenderName", "description", "placingWayName"
    ]) or ""))
    if not title:
        logger.info("Пропуск %s: не нашёл название. Поля=%s", tid, list(item.keys())[:20])
        return None

    price = first_value(item, [
        "maxPrice", "initialPrice", "initialContractPrice", "nmck", "price",
        "sum", "amount", "startPrice", "lotPrice"
    ])
    try:
        price = float(str(price).replace(" ", "").replace(",", ".")) if price not in (None, "") else None
    except Exception:
        price = None

    deadline = fmt_ts(first_value(item, [
        "submissionCloseDateTime", "submissionCloseDate", "endDate", "deadline",
        "biddingDateEnd", "requestReceivingEndDate", "finishDate", "dateEnd"
    ]))
    published_at = fmt_ts(first_value(item, [
        "publicationDateTime", "publicationDate", "publishDate", "createDate", "datePublished"
    ]))

    status_code = first_value(item, ["status", "statusCode", "state", "tenderStatus"])
    status_map = {1: "Активен", 2: "На рассмотрении", 3: "Завершён", 4: "Отменён", 5: "Не состоялся"}
    try:
        status_code_int = int(status_code)
    except Exception:
        status_code_int = 1
    status = status_map.get(status_code_int, str(status_code) if status_code else "Активен")

    region = first_value(item, [
        "regionName", "region", "customerRegion", "deliveryRegion", "subjectRF", "placeName"
    ])
    if isinstance(region, dict):
        region = first_value(region, ["name", "title", "regionName"])

    url = first_value(item, ["url", "href", "tenderUrl", "link"])
    if not url:
        url = f"https://tenderplan.ru/app/analytics/tender/{tid}"

    return {
        "tender_id": tid,
        "title": title,
        "region": region,
        "keyword": keyword,
        "price": price,
        "deadline": deadline,
        "status": status,
        "url": url,
        "published_at": published_at,
        "status_code": status_code_int,
    }


# ═══════════════════════════════════════════════════════════
#  ПЛАНИРОВЩИК
# ═══════════════════════════════════════════════════════════

async def run_update(bot: Bot):
    """Основной цикл обновления."""
    if update_lock.locked():
        logger.warning("Обновление уже выполняется, второй запуск пропущен")
        return

    async with update_lock:
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

                parsed_count = 0
                saved_count = 0
                duplicate_count = 0
                filtered_count = 0

                for item in items:
                    t = parse_tender_from_api(item, kw)
                    if not t:
                        filtered_count += 1
                        continue
                    parsed_count += 1
                    if t["tender_id"] in seen_ids:
                        duplicate_count += 1
                        continue
                    seen_ids.add(t["tender_id"])

                    # Завершённые переносим в finished
                    if t.get("status_code") in (3, 4, 5):
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

                    before_new_count = len(new_tenders)
                    is_new = await upsert_active(t)
                    if is_new:
                        new_tenders.append(t)
                        saved_count += 1
                    elif len(new_tenders) == before_new_count and not is_metal_tender(t.get("title", "")):
                        filtered_count += 1

                logger.info("Tenderplan '%s': raw=%d, parsed=%d, duplicates=%d, saved_new=%d, filtered/skipped=%d",
                            kw, len(items), parsed_count, duplicate_count, saved_count, filtered_count)

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
        [KeyboardButton(text="📊 Аналитика"), KeyboardButton(text="📤 Excel/CSV")],
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



@router.message(Command("analytics"))
@router.message(lambda m: m.text == "📊 Аналитика")
async def cmd_analytics(message: Message):
    data = await get_market_analytics()
    await message.answer(format_market_analytics(data), parse_mode="HTML")


@router.message(Command("export"))
@router.message(lambda m: m.text == "📤 Excel/CSV")
async def cmd_export(message: Message):
    csv_bytes = await build_tenders_csv()
    file = BufferedInputFile(csv_bytes, filename="tenders_export.csv")
    await message.answer_document(file, caption="📤 Выгрузка активных тендеров для Excel")


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
    await message.answer("🔄 Запускаю обновление. Когда найду новые тендеры — пришлю.", parse_mode="HTML")
    asyncio.create_task(run_update(message.bot))



@router.message(Command("menu"))
async def cmd_menu(message: Message):
    await message.answer("Меню обновлено", reply_markup=KEYBOARD)

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

    # На всякий случай выключаем webhook перед polling.
    # Это не заменяет выключение второго запущенного экземпляра, но убирает конфликт webhook/polling.
    await bot.delete_webhook(drop_pending_updates=True)

    try:
        await dp.start_polling(bot, allowed_updates=["message"])
    finally:
        scheduler.shutdown()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
