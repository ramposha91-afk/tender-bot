"""
Telegram-бот мониторинга тендеров по металлолому
Источник: Tenderplan API

ENV:
- BOT_TOKEN
- TENDERPLAN_TOKEN
"""

import asyncio
import csv
import html
import io
import json
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
from aiogram.types import BufferedInputFile, KeyboardButton, Message, ReplyKeyboardMarkup
from apscheduler.schedulers.asyncio import AsyncIOScheduler


# =============================================================================
# НАСТРОЙКИ
# =============================================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TENDERPLAN_TOKEN = os.getenv("TENDERPLAN_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "tenders.db")
UPDATE_INTERVAL_MINUTES = int(os.getenv("UPDATE_INTERVAL_MINUTES", "30"))

TENDERPLAN_API = "https://tenderplan.ru/api"
TENDERPLAN_KEY_ID = "6a1206f769fe7578ea07d6c1"

TENDERPLAN_WORDS = (
    'металлолом*,"лом","лома",(метал* отход*)~0,металлоотход*,'
    '(облом* метал*)~2,(обрез* метал*)~2,(на слом* идущ*)~0,'
    'вывоз* металлоконструкц*,цветмет*,чермет*,'
    '(стальн* струж*)~0,(метал* стружк*)~0,'
    'Лом черных металлов,Лом цветных металлов,Лом чермет,'
    'Лом латуни,Стружка металлическая,Отходы черных металлов,'
    'Отходы цветных металлов,Демонтаж металлоконструкций,'
    'Прием лома,Реализация лома'
)
TENDERPLAN_EXCLUDED = (
    'канцеляр*,канцтовар*,(хозяйственн* инвентар*)~1,'
    '(хозяйственн* товар*)~1,(продук* питан*)~0,'
    '(лом* пожарн*)~0,(лом* лапчат*)~0,(Лом-топор)~0,'
    '"багор","лопата",лакокрасоч*,"ремонт",'
    '(ремонт* работ*)~1,(ремонт* услуг*)~1,шлифмашин*,'
    'сверл*,кувалд*,(строительно-монтаж* работ*)~1,выставк*'
)

REQUEST_TIMEOUT = 40
REQUEST_DELAY = 1.2
MAX_RETRIES = 3

FORBIDDEN_PATTERNS = [
    r"канцеляр", r"канцтовар", r"молок", r"мяс", r"рыб",
    r"овощ", r"фрукт", r"продукт[ыа] питан", r"хлеб",
    r"медицин", r"лекарств", r"фармацевт",
    r"одежд", r"обув", r"мебел", r"посуд",
    r"хозтовар", r"картридж", r"тонер", r"швабр",
    r"моющ", r"дезинфиц", r"ремонт дорог",
    r"ремонт помещ", r"ремонт здан", r"благоустройств",
    r"асфальт", r"страхован", r"водопровод", r"канализац",
    r"автомобил.{0,20}(металлолом|утилизац)",
    r"(транспортн|грузов|легков).{0,20}(утилизац|металлолом)",
    r"капитальный ремонт", r"строительно-монтаж",
    r"дорожн.{0,10}работ", r"водоснабжен", r"теплоснабжен",
    r"земельный участок", r"жилой дом", r"квартир",
]

GOOD_PATTERNS = [
    r"металлолом",
    r"лом (черных|цветных|чёрных|черн|цветн) металл",
    r"(прием|приём|реализац|закупк|поставк|продаж|сдач).{0,20}металлолом",
    r"(прием|приём|реализац|закупк).{0,15}лом",
    r"стружка.{0,15}(металл|алюмин|медн|латун|стальн)",
    r"отходы.{0,10}(черных|цветных|черн|цветн).{0,10}металл",
    r"(демонтаж|утилизац).{0,20}металлоконструкц",
    r"чермет", r"цветмет",
    r"лом.{0,5}(3а|5а|12а|13а|15а|16а)",
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
        if ts > 10_000_000_000:
            ts = ts // 1000
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return None


def status_name(status_code: Any) -> str:
    mapping = {1: "Активен", 2: "На рассмотрении", 3: "Завершён", 4: "Отменён", 5: "Не состоялся"}
    try:
        return mapping.get(int(status_code), str(status_code or "не указан"))
    except Exception:
        return str(status_code or "не указан")


def escape(s: Any) -> str:
    return html.escape(str(s or ""))


REGION_MAP = {
    1: "Республика Адыгея", 2: "Республика Башкортостан", 3: "Республика Бурятия",
    4: "Республика Алтай", 5: "Республика Дагестан", 6: "Республика Ингушетия",
    7: "Кабардино-Балкарская Республика", 8: "Республика Калмыкия",
    9: "Карачаево-Черкесская Республика", 10: "Республика Карелия", 11: "Республика Коми",
    12: "Республика Марий Эл", 13: "Республика Мордовия", 14: "Республика Саха (Якутия)",
    15: "Республика Северная Осетия — Алания", 16: "Республика Татарстан",
    17: "Республика Тыва", 18: "Удмуртская Республика", 19: "Республика Хакасия",
    20: "Чеченская Республика", 21: "Чувашская Республика", 22: "Алтайский край",
    23: "Краснодарский край", 24: "Красноярский край", 25: "Приморский край",
    26: "Ставропольский край", 27: "Хабаровский край", 28: "Амурская область",
    29: "Архангельская область", 30: "Астраханская область", 31: "Белгородская область",
    32: "Брянская область", 33: "Владимирская область", 34: "Волгоградская область",
    35: "Вологодская область", 36: "Воронежская область", 37: "Ивановская область",
    38: "Иркутская область", 39: "Калининградская область", 40: "Калужская область",
    41: "Камчатский край", 42: "Кемеровская область", 43: "Кировская область",
    44: "Костромская область", 45: "Курганская область", 46: "Курская область",
    47: "Ленинградская область", 48: "Липецкая область", 49: "Магаданская область",
    50: "Московская область", 51: "Мурманская область", 52: "Нижегородская область",
    53: "Новгородская область", 54: "Новосибирская область", 55: "Омская область",
    56: "Оренбургская область", 57: "Орловская область", 58: "Пензенская область",
    59: "Пермский край", 60: "Псковская область", 61: "Ростовская область",
    62: "Рязанская область", 63: "Самарская область", 64: "Саратовская область",
    65: "Сахалинская область", 66: "Свердловская область", 67: "Смоленская область",
    68: "Тамбовская область", 69: "Тверская область", 70: "Томская область",
    71: "Тульская область", 72: "Тюменская область", 73: "Ульяновская область",
    74: "Челябинская область", 75: "Забайкальский край", 76: "Ярославская область",
    77: "Москва", 78: "Санкт-Петербург", 79: "Еврейская автономная область",
    83: "Ненецкий автономный округ", 86: "Ханты-Мансийский автономный округ — Югра",
    87: "Чукотский автономный округ", 89: "Ямало-Ненецкий автономный округ",
    91: "Республика Крым", 92: "Севастополь",
}


def region_name(region_code: Any) -> str:
    try:
        code = int(region_code)
        return REGION_MAP.get(code, f"Регион {code}")
    except Exception:
        return ""


def detect_metal_type(title: str) -> str:
    t = normalize_text(title)
    if "алюмин" in t:
        return "Алюминий"
    if re.search(r"\bмед[ьи]\b|медн", t):
        return "Медь"
    if "латун" in t:
        return "Латунь"
    if "цветн" in t or "цветмет" in t:
        return "Цветные металлы"
    if "черн" in t or "чермет" in t:
        return "Черные металлы"
    if "струж" in t:
        return "Стружка"
    if "металлоконструкц" in t:
        return "Металлоконструкции"
    return "Металлолом"


def _safe_json_loads(value: Any) -> dict:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def _fv(node: Any) -> Any:
    return node.get("fv") if isinstance(node, dict) else None


def _recursive_find_by_fn_or_fdn(node: Any, names: set[str]) -> Optional[Any]:
    if isinstance(node, dict):
        fn = str(node.get("fn") or "").lower()
        fdn = str(node.get("fdn") or "").lower()
        if fn in names or fdn in names:
            return node.get("fv")
        for v in node.values():
            found = _recursive_find_by_fn_or_fdn(v, names)
            if found not in (None, ""):
                return found
    elif isinstance(node, list):
        for v in node:
            found = _recursive_find_by_fn_or_fdn(v, names)
            if found not in (None, ""):
                return found
    return None


def extract_details_fields(details: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not details:
        return {}

    embedded = _safe_json_loads(details.get("json"))
    contacts_info = embedded.get("2", {}).get("fv", {}) if isinstance(embedded, dict) else {}
    contacts = contacts_info.get("3", {}).get("fv", {}) if isinstance(contacts_info, dict) else {}
    general = embedded.get("general", {}) if isinstance(embedded, dict) else {}

    fio = _fv(contacts.get("0", {})) if isinstance(contacts, dict) else None
    phone = _fv(contacts.get("1", {})) if isinstance(contacts, dict) else None
    email = _fv(contacts.get("3", {})) if isinstance(contacts, dict) else None
    fact_address = _fv(contacts_info.get("1", {})) if isinstance(contacts_info, dict) else None
    delivery_place = _fv(general.get("1", {})) if isinstance(general, dict) else None

    if not delivery_place:
        delivery_place = _recursive_find_by_fn_or_fdn(embedded, {"deliveryplace", "место поставки"})
    if not fio:
        fio = _recursive_find_by_fn_or_fdn(embedded, {"fio", "фио"})
    if not phone:
        phone = _recursive_find_by_fn_or_fdn(embedded, {"phone", "телефон"})
    if not email:
        email = _recursive_find_by_fn_or_fdn(embedded, {"email", "электронная почта"})
    if not fact_address:
        fact_address = _recursive_find_by_fn_or_fdn(embedded, {"factaddress", "фактический адрес"})

    customers = details.get("customers") or []
    customer = customers[0].get("name", "") if customers and isinstance(customers[0], dict) else ""
    region_code = details.get("region")
    if not region_code and customers and isinstance(customers[0], dict):
        region_code = customers[0].get("region")
    platform = details.get("platform") or {}

    return {
        "region_code": int(region_code) if str(region_code or "").isdigit() else None,
        "region_name": region_name(region_code),
        "delivery_place": clean_html(delivery_place or ""),
        "contact_person": clean_html(fio or ""),
        "contact_phone": clean_html(phone or ""),
        "contact_email": clean_html(email or ""),
        "customer": clean_html(customer),
        "fact_address": clean_html(fact_address or ""),
        "platform_name": clean_html(platform.get("name", "")) if isinstance(platform, dict) else "",
        "source_url": details.get("href") or "",
    }


def enrich_tender(parsed: dict[str, Any], details: Optional[dict[str, Any]]) -> dict[str, Any]:
    extra = extract_details_fields(details)
    if details:
        parsed["status_code"] = int(details.get("status")) if str(details.get("status", "")).isdigit() else parsed.get("status_code")
        parsed["status"] = status_name(parsed.get("status_code"))
        parsed["price"] = safe_float(details.get("maxPrice")) if details.get("maxPrice") is not None else parsed.get("price")
        parsed["deadline"] = fmt_ts_ms(details.get("submissionCloseDateTime")) or parsed.get("deadline")
        parsed["published_at"] = fmt_ts_ms(details.get("publicationDateTime")) or parsed.get("published_at")
    parsed.update(extra)
    details_text = details.get("tenderSearch", "") if details else ""
    parsed["metal_type"] = detect_metal_type(f"{parsed.get('title', '')} {details_text}")
    return parsed


# =============================================================================
# БД
# =============================================================================

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.executescript("""
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
                region_code INTEGER,
                region_name TEXT,
                delivery_place TEXT,
                contact_person TEXT,
                contact_phone TEXT,
                contact_email TEXT,
                customer TEXT,
                fact_address TEXT,
                platform_name TEXT,
                source_url TEXT,
                metal_type TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS finished_tenders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tender_id TEXT UNIQUE,
                title TEXT,
                customer TEXT,
                start_price REAL,
                final_price REAL,
                winner TEXT,
                participants_count INTEGER,
                finished_at TEXT,
                url TEXT
            );
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY,
                alerts_on INTEGER DEFAULT 1,
                joined_at TEXT DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_tenders_status ON tenders(status_code);
            CREATE INDEX IF NOT EXISTS idx_tenders_updated ON tenders(updated_at);
            CREATE INDEX IF NOT EXISTS idx_finished_at ON finished_tenders(finished_at);
        """)
        for col, col_type in [
            ("region_code", "INTEGER"),
            ("region_name", "TEXT"),
            ("delivery_place", "TEXT"),
            ("contact_person", "TEXT"),
            ("contact_phone", "TEXT"),
            ("contact_email", "TEXT"),
            ("customer", "TEXT"),
            ("fact_address", "TEXT"),
            ("platform_name", "TEXT"),
            ("source_url", "TEXT"),
            ("metal_type", "TEXT"),
        ]:
            try:
                await db.execute(f"ALTER TABLE tenders ADD COLUMN {col} {col_type}")
            except aiosqlite.OperationalError:
                pass
        await db.commit()
    logger.info("БД готова: %s", DB_PATH)


async def subscribe(chat_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
        await db.commit()


async def get_subscribers() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT chat_id FROM subscribers WHERE alerts_on=1")
        return [int(r[0]) for r in await cur.fetchall()]


async def upsert_tender(t: dict[str, Any]) -> bool:
    now = datetime.utcnow().isoformat()
    fields = [
        "title", "keyword", "price", "deadline", "status_code", "status", "url",
        "kind", "type", "placing_way", "currency", "published_at",
        "region_code", "region_name", "delivery_place", "contact_person",
        "contact_phone", "contact_email", "customer", "fact_address",
        "platform_name", "source_url", "metal_type",
    ]

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tender_id FROM tenders WHERE tender_id=?", (t["tender_id"],))
        exists = await cur.fetchone()

        if exists:
            set_clause = ", ".join([f"{f}=?" for f in fields] + ["updated_at=?"])
            values = [t.get(f) for f in fields] + [now, t["tender_id"]]
            await db.execute(f"UPDATE tenders SET {set_clause} WHERE tender_id=?", values)
            await db.commit()
            return False

        insert_fields = ["tender_id"] + fields + ["created_at", "updated_at"]
        placeholders = ",".join(["?"] * len(insert_fields))
        values = [t.get("tender_id")] + [t.get(f) for f in fields] + [now, now]
        await db.execute(f"INSERT INTO tenders ({','.join(insert_fields)}) VALUES ({placeholders})", values)
        await db.commit()
        return True

async def upsert_finished(t: dict[str, Any]) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tender_id FROM finished_tenders WHERE tender_id=?", (t["tender_id"],))
        if await cur.fetchone():
            return False
        await db.execute(
            """INSERT INTO finished_tenders
               (tender_id,title,customer,start_price,final_price,winner,participants_count,finished_at,url)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (t.get("tender_id"), t.get("title"), t.get("customer"), t.get("start_price"),
             t.get("final_price"), t.get("winner"), t.get("participants_count"),
             t.get("finished_at"), t.get("url")),
        )
        await db.commit()
        return True


async def get_tenders(limit: int = 15, only_active: bool = False) -> list[dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        if only_active:
            cur = await db.execute(
                "SELECT * FROM tenders WHERE COALESCE(status_code,1) IN (1,2) ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            )
        else:
            cur = await db.execute("SELECT * FROM tenders ORDER BY updated_at DESC LIMIT ?", (limit,))
        return [dict(r) for r in await cur.fetchall()]


async def get_finished(limit: int = 15) -> list[dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT * FROM finished_tenders ORDER BY finished_at DESC LIMIT ?", (limit,)
        )
        return [dict(r) for r in await cur.fetchall()]


async def get_summary() -> dict[str, Any]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        c1 = await db.execute(
            """SELECT COUNT(*) AS total_count, SUM(price) AS total_price, AVG(price) AS avg_price,
               SUM(CASE WHEN COALESCE(status_code,1) IN (1,2) THEN 1 ELSE 0 END) AS active_count,
               SUM(CASE WHEN COALESCE(status_code,1) IN (3,4,5) THEN 1 ELSE 0 END) AS done_count
               FROM tenders"""
        )
        totals = dict(await c1.fetchone())
        c2 = await db.execute(
            "SELECT status, COUNT(*) AS cnt, SUM(price) AS total FROM tenders GROUP BY status ORDER BY cnt DESC"
        )
        by_status = [dict(r) for r in await c2.fetchall()]
        c3 = await db.execute(
            """SELECT COUNT(*) AS cnt, SUM(final_price) AS total, AVG(final_price) AS avg
               FROM finished_tenders"""
        )
        finished_stats = dict(await c3.fetchone())
        c4 = await db.execute(
            """SELECT SUM(CASE WHEN price IS NULL OR price=0 THEN 1 ELSE 0 END) AS no_price,
               SUM(CASE WHEN price>0 AND price<1000000 THEN 1 ELSE 0 END) AS p_0_1,
               SUM(CASE WHEN price>=1000000 AND price<10000000 THEN 1 ELSE 0 END) AS p_1_10,
               SUM(CASE WHEN price>=10000000 AND price<100000000 THEN 1 ELSE 0 END) AS p_10_100,
               SUM(CASE WHEN price>=100000000 THEN 1 ELSE 0 END) AS p_100 FROM tenders"""
        )
        price_ranges = dict(await c4.fetchone())
        c5 = await db.execute(
            """SELECT COALESCE(region_name, 'Регион не указан') AS region,
                      COALESCE(metal_type, 'Не определено') AS metal_type,
                      COUNT(*) AS cnt,
                      SUM(price) AS total
                 FROM tenders
                GROUP BY COALESCE(region_name, 'Регион не указан'),
                         COALESCE(metal_type, 'Не определено')
                ORDER BY cnt DESC, total DESC
                LIMIT 30"""
        )
        by_region_metal = [dict(r) for r in await c5.fetchall()]
        return {"totals": totals, "by_status": by_status,
                "finished_stats": finished_stats, "price_ranges": price_ranges,
                "by_region_metal": by_region_metal}

async def export_csv_bytes() -> bytes:
    rows = await get_tenders(limit=5000, only_active=False)
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")
    writer.writerow([
        "ID", "Название", "Тип металла", "Регион", "Место поставки",
        "Контактное лицо", "Телефон", "Email", "Заказчик",
        "Цена", "Срок подачи", "Статус", "Опубликовано", "Площадка", "Ссылка"
    ])
    for r in rows:
        writer.writerow([
            r.get("tender_id"), r.get("title"), r.get("metal_type"),
            r.get("region_name"), r.get("delivery_place"),
            r.get("contact_person"), r.get("contact_phone"), r.get("contact_email"),
            r.get("customer"), r.get("price"), r.get("deadline"), r.get("status"),
            r.get("published_at"), r.get("platform_name"), r.get("url")
        ])
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


async def tp_request_post(session: aiohttp.ClientSession, url: str, payload: dict) -> list:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.post(
                url, json=payload, headers=api_headers(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    tenders = data.get("tenders", []) if isinstance(data, dict) else []
                    logger.info("POST %s: найдено=%d", url.split("/")[-1], len(tenders))
                    return tenders
                elif resp.status == 429:
                    await asyncio.sleep(10)
                    continue
                else:
                    body = await resp.text()
                    logger.warning("POST %s HTTP %s: %s", url.split("/")[-1], resp.status, body[:200])
                    return []
        except Exception as e:
            logger.warning("POST error attempt %d: %s", attempt, e)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 * attempt)
    return []


async def tp_search_by_key(session: aiohttp.ClientSession, page: int = 1, count: int = 50) -> list:
    return await tp_request_post(
        session, f"{TENDERPLAN_API}/relations/v2/list",
        {"keyId": TENDERPLAN_KEY_ID, "page": page, "count": count}
    )


async def tp_search_by_words(session: aiohttp.ClientSession, page: int = 1, count: int = 50) -> list:
    return await tp_request_post(
        session, f"{TENDERPLAN_API}/search/list",
        {"words": {"value": TENDERPLAN_WORDS, "excluded": TENDERPLAN_EXCLUDED},
         "condition": "or", "page": page, "count": count}
    )


async def tp_get_tender(session: aiohttp.ClientSession, tender_id: str) -> Optional[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(
                f"{TENDERPLAN_API}/tenders/get",
                params={"id": tender_id},
                headers=api_headers(),
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                return None
        except Exception as e:
            logger.debug("tp_get_tender error: %s", e)
        if attempt < MAX_RETRIES:
            await asyncio.sleep(1)
    return None


def extract_winner(tender_data: dict) -> tuple[Optional[str], Optional[float]]:
    participants = tender_data.get("participants", [])
    for p in participants:
        if p.get("winner"):
            return p.get("name"), safe_float(p.get("price"))
    # Если winner не помечен — берём с минимальной ценой среди допущенных
    admitted = [p for p in participants if p.get("admitted")]
    if admitted:
        best = min(admitted, key=lambda x: safe_float(x.get("price")) or float("inf"))
        return best.get("name"), safe_float(best.get("price"))
    return None, None


def parse_tender(item: dict[str, Any], keyword: str = "металлолом") -> Optional[dict[str, Any]]:
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
        f"📍 <b>Регион:</b> {escape(t.get('region_name') or 'не указан')}\n"
        f"🧱 <b>Тип металла:</b> {escape(t.get('metal_type') or 'не определён')}\n"
        f"🚚 <b>Место поставки:</b> {escape(t.get('delivery_place') or 'не указано')}\n"
        f"💰 <b>Начальная цена:</b> {fmt_price(t.get('price'))}\n"
        f"⏰ <b>Срок подачи:</b> {escape(t.get('deadline') or 'не указан')}\n"
        f"📌 <b>Статус:</b> {escape(t.get('status') or 'не указан')}\n"
        f"👤 <b>Контактное лицо:</b> {escape(t.get('contact_person') or 'не указано')}\n"
        f"☎️ <b>Телефон:</b> {escape(t.get('contact_phone') or 'не указан')}\n"
        f"✉️ <b>Email:</b> {escape(t.get('contact_email') or 'не указан')}\n"
        f"🏢 <b>Заказчик:</b> {escape(t.get('customer') or 'не указан')}\n"
        f"🏛 <b>Площадка:</b> {escape(t.get('platform_name') or 'Tenderplan')}\n"
        f"🔗 <a href=\"{escape(t.get('url'))}\">Открыть тендер</a>"
    )


def format_finished_card(t: dict[str, Any]) -> str:
    sp = safe_float(t.get("start_price"))
    fp = safe_float(t.get("final_price"))
    savings = ""
    if sp and fp and sp > 0:
        pct = (1 - fp / sp) * 100
        savings = f"\n📉 <b>Снижение цены:</b> {pct:.1f}%"
    lines = [
        f"🏁 <b>Название:</b> {escape(t.get('title'))}",
        f"🏢 <b>Заказчик:</b> {escape(t.get('customer') or 'не указан')}",
        f"💰 <b>Начальная цена:</b> {fmt_price(sp)}",
        f"🏆 <b>Итоговая цена:</b> {fmt_price(fp)}{savings}",
        f"🥇 <b>Победитель:</b> {escape(t.get('winner') or 'не указан')}",
        f"👥 <b>Участников:</b> {t.get('participants_count') or 'не указано'}",
        f"📅 <b>Дата итогов:</b> {escape(t.get('finished_at') or 'не указана')}",
        f"🔗 <a href=\"{escape(t.get('url'))}\">Открыть тендер</a>",
    ]
    return "\n".join(lines)


def format_summary(data: dict[str, Any]) -> str:
    totals = data["totals"]
    pr = data["price_ranges"]
    fs = data["finished_stats"]
    lines = [
        "📊 <b>Аналитика по тендерам</b>", "",
        f"Активных в базе: <b>{totals.get('active_count') or 0}</b>",
        f"Завершённых: <b>{totals.get('done_count') or 0}</b>",
        f"Сумма НМЦ активных: <b>{fmt_price(totals.get('total_price'))}</b>",
        f"Средняя НМЦ: <b>{fmt_price(totals.get('avg_price'))}</b>",
        "",
        f"Завершённых с итогами: <b>{fs.get('cnt') or 0}</b>",
        f"Средняя итоговая цена: <b>{fmt_price(fs.get('avg'))}</b>",
        "",
        "💰 <b>Диапазоны цен:</b>",
        f"  без цены: {pr.get('no_price') or 0}",
        f"  до 1 млн ₽: {pr.get('p_0_1') or 0}",
        f"  1–10 млн ₽: {pr.get('p_1_10') or 0}",
        f"  10–100 млн ₽: {pr.get('p_10_100') or 0}",
        f"  100+ млн ₽: {pr.get('p_100') or 0}",
    ]
    if data["by_status"]:
        lines += ["", "📌 <b>По статусам:</b>"]
        for r in data["by_status"]:
            lines.append(f"  {escape(r.get('status'))}: {r.get('cnt') or 0} / {fmt_price(r.get('total'))}")

    if data.get("by_region_metal"):
        lines += ["", "📍 <b>Регион / тип металла / количество:</b>"]
        for r in data["by_region_metal"][:20]:
            lines.append(
                f"  {escape(r.get('region'))} — {escape(r.get('metal_type'))}: "
                f"{r.get('cnt') or 0} / {fmt_price(r.get('total'))}"
            )
    return "\n".join(lines)


# =============================================================================
# ОБНОВЛЕНИЕ
# =============================================================================

async def run_update(bot: Optional[Bot] = None, notify: bool = True) -> tuple[int, int]:
    if _update_lock.locked():
        logger.warning("Обновление уже выполняется")
        return 0, 0

    async with _update_lock:
        logger.info("=== Обновление: %s ===", datetime.now().strftime("%d.%m.%Y %H:%M"))
        new_tenders: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        connector = aiohttp.TCPConnector(limit=5)
        async with aiohttp.ClientSession(connector=connector) as session:

            # Шаг 1: поиск по ключу
            all_raw: list[dict] = []
            for page_num in range(1, 6):
                raw = await tp_search_by_key(session, page=page_num, count=50)
                if not raw:
                    break
                all_raw.extend(raw)
                await asyncio.sleep(REQUEST_DELAY)

            # Шаг 2: запасной поиск если ключ пуст
            if not all_raw:
                logger.info("Ключ вернул 0, запасной поиск...")
                raw = await tp_search_by_words(session, page=1, count=50)
                all_raw.extend(raw)

            logger.info("Получено из API: %d", len(all_raw))

            # Сохраняем новые тендеры
            parsed_count = saved_count = filtered_count = duplicate_count = 0
            for item in all_raw:
                parsed = parse_tender(item)
                if not parsed:
                    continue
                parsed_count += 1
                if parsed["tender_id"] in seen_ids:
                    duplicate_count += 1
                    continue
                seen_ids.add(parsed["tender_id"])

                details = await tp_get_tender(session, parsed["tender_id"])
                parsed = enrich_tender(parsed, details)

                if not is_relevant_tender(parsed.get("title", "")):
                    filtered_count += 1
                    logger.info("ОТФИЛЬТРОВАНО: %s", parsed.get("title", "")[:100])
                    continue
                is_new = await upsert_tender(parsed)
                if is_new:
                    saved_count += 1
                    new_tenders.append(parsed)
                else:
                    duplicate_count += 1

            logger.info("Новых: %d, отфильтровано: %d, дублей: %d", saved_count, filtered_count, duplicate_count)

            # Шаг 3: проверяем статусы активных → переносим завершённые
            active_tenders = await get_tenders(limit=100, only_active=True)
            finished_count = 0
            for t in active_tenders:
                details = await tp_get_tender(session, t["tender_id"])
                if not details:
                    await asyncio.sleep(0.3)
                    continue
                status_code = details.get("status")
                if status_code in (3, 4, 5):
                    winner, final_price = extract_winner(details)
                    customers = details.get("customers", [])
                    customer = customers[0].get("name", "") if customers else ""
                    participants = details.get("participants", [])
                    saved = await upsert_finished({
                        "tender_id": t["tender_id"],
                        "title": t.get("title"),
                        "customer": customer,
                        "start_price": t.get("price"),
                        "final_price": final_price,
                        "winner": winner,
                        "participants_count": len(participants),
                        "finished_at": fmt_ts_ms(
                            details.get("summingUpDateTime") or details.get("updateDateTime")
                        ),
                        "url": t.get("url"),
                    })
                    if saved:
                        finished_count += 1
                        async with aiosqlite.connect(DB_PATH) as db:
                            await db.execute("DELETE FROM tenders WHERE tender_id=?", (t["tender_id"],))
                            await db.commit()
                await asyncio.sleep(0.3)

            if finished_count:
                logger.info("Перенесено в завершённые: %d", finished_count)

        all_tenders = await get_tenders(limit=1000, only_active=False)
        logger.info("Всего в базе: %d", len(all_tenders))

        if bot and notify and new_tenders:
            await notify_new(bot, new_tenders)

        return len(new_tenders), len(all_tenders)


async def notify_new(bot: Bot, new_tenders: list[dict[str, Any]]) -> None:
    subscribers = await get_subscribers()
    for chat_id in subscribers:
        try:
            await bot.send_message(
                chat_id,
                f"🔔 <b>Новых тендеров: {len(new_tenders)}</b>",
                parse_mode="HTML",
            )
            for t in new_tenders[:10]:
                await bot.send_message(
                    chat_id, format_tender_card(t),
                    parse_mode="HTML", disable_web_page_preview=True,
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
        [KeyboardButton(text="🆕 Тендеры"), KeyboardButton(text="🏁 Завершённые")],
        [KeyboardButton(text="📊 Аналитика"), KeyboardButton(text="📤 Excel/CSV")],
        [KeyboardButton(text="🔄 Обновить"), KeyboardButton(text="❓ Помощь")],
    ],
    resize_keyboard=True,
)


@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    await subscribe(message.chat.id)
    await message.answer(
        "👋 <b>Бот мониторинга тендеров по металлолому</b>\n\n"
        "🆕 Тендеры — активные тендеры\n"
        "🏁 Завершённые — итоги с победителями\n"
        "📊 Аналитика — сводка по базе\n"
        "📤 Excel/CSV — выгрузка\n"
        "🔄 Обновить — обновить базу\n\n"
        "Команды: /new /finished /analytics /export /update /search",
        parse_mode="HTML",
        reply_markup=KEYBOARD,
    )


@router.message(Command("new"))
@router.message(lambda m: m.text == "🆕 Тендеры")
async def cmd_new(message: Message) -> None:
    rows = await get_tenders(limit=15, only_active=False)
    if not rows:
        await message.answer("📭 В базе пока 0 тендеров.\nНажми 🔄 Обновить.", reply_markup=KEYBOARD)
        return
    await message.answer(f"🆕 <b>Последние тендеры</b> ({len(rows)}):", parse_mode="HTML")
    for t in rows:
        await message.answer(format_tender_card(t), parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("finished"))
@router.message(lambda m: m.text == "🏁 Завершённые")
async def cmd_finished(message: Message) -> None:
    rows = await get_finished(limit=10)
    if not rows:
        await message.answer(
            "📭 Завершённых тендеров пока нет.\nБот проверяет статусы каждые 30 минут.",
            reply_markup=KEYBOARD,
        )
        return
    await message.answer(f"🏁 <b>Завершённые тендеры</b> ({len(rows)}):", parse_mode="HTML")
    for t in rows:
        await message.answer(format_finished_card(t), parse_mode="HTML", disable_web_page_preview=True)


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
        await message.answer("📭 Экспорт пустой: в базе нет тендеров.")
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
    await message.answer("🔄 Запускаю обновление. 1–2 минуты.")
    new_count, total_count = await run_update(message.bot, notify=False)
    await message.answer(
        f"✅ Готово.\nНовых: <b>{new_count}</b>\nВсего в базе: <b>{total_count}</b>",
        parse_mode="HTML",
        reply_markup=KEYBOARD,
    )


@router.message(Command("search"))
async def cmd_search(message: Message, command: CommandObject) -> None:
    q = normalize_text(command.args or "")
    if not q:
        await message.answer("Пример: <code>/search алюминий</code>", parse_mode="HTML")
        return
    rows = await get_tenders(limit=1000, only_active=False)
    filtered = [r for r in rows if q in normalize_text(r.get("title", ""))][:15]
    if not filtered:
        await message.answer(f"📭 По запросу <b>{escape(q)}</b> ничего не найдено.", parse_mode="HTML")
        return
    await message.answer(f"🔍 Найдено: {len(filtered)}")
    for t in filtered:
        await message.answer(format_tender_card(t), parse_mode="HTML", disable_web_page_preview=True)


@router.message(Command("help"))
@router.message(lambda m: m.text == "❓ Помощь")
async def cmd_help(message: Message) -> None:
    await cmd_start(message)


# =============================================================================
# ЗАПУСК
# =============================================================================

async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не задан BOT_TOKEN")
    if not TENDERPLAN_TOKEN:
        raise RuntimeError("Не задан TENDERPLAN_TOKEN")

    await init_db()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    scheduler = AsyncIOScheduler(
        timezone="Europe/Moscow",
        job_defaults={"misfire_grace_time": 600, "max_instances": 1},
    )
    scheduler.add_job(
        run_update, "interval",
        minutes=UPDATE_INTERVAL_MINUTES,
        args=[bot, True],
        next_run_time=datetime.now(),
        id="run_update",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Бот запущен. Интервал: %d мин.", UPDATE_INTERVAL_MINUTES)

    try:
        await dp.start_polling(bot, allowed_updates=["message"], drop_pending_updates=True)
    finally:
        scheduler.shutdown(wait=False)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
