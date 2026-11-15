"""
Catalog service — каталог товаров продуктового магазина.

Что делает:
  * хранит товары (название, категория, цена, остаток на складе) в PostgreSQL;
  * отдаёт список товаров, кэшируя его в Redis на 60 секунд;
  * резервирует (списывает) товары, когда оформляется заказ.

Здесь видны два обязательных компонента задания: БАЗА ДАННЫХ и КЭШ.
"""
import json
import logging
import os
import time
from contextlib import asynccontextmanager

import psycopg
import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [catalog] %(message)s")
log = logging.getLogger("catalog")

# Адреса берём из переменных окружения (их задаёт docker-compose.yml).
# "postgres" и "redis" — это имена контейнеров: внутри Docker-сети они работают как адреса.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://shop:shop@localhost:5432/shop")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

CACHE_KEY = "catalog:products"   # ключ, под которым список товаров лежит в Redis
CACHE_TTL = 60                   # сколько секунд кэш считается свежим

# Учебная задержка: имитирует "тяжёлый" запрос к БД, чтобы на демо была видна
# разница во времени ответа с кэшем и без. В реальном проекте её бы не было.
SLOW_DB_SECONDS = float(os.getenv("SLOW_DB_SECONDS", "0.5"))


class UTF8JSONResponse(JSONResponse):
    # Явно указываем кодировку, чтобы русские буквы везде отображались правильно
    media_type = "application/json; charset=utf-8"


cache = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# Стартовый набор товаров: (название, категория, цена, остаток)
SEED_PRODUCTS = [
    ("Молоко 3,2%, 1 л", "Молочные продукты", 89.90, 20),
    ("Кефир 1%, 900 мл", "Молочные продукты", 79.50, 15),
    ("Сыр российский, 200 г", "Молочные продукты", 189.00, 10),
    ("Хлеб бородинский", "Хлеб", 55.00, 15),
    ("Батон нарезной", "Хлеб", 49.90, 25),
    ("Яблоки, 1 кг", "Фрукты и овощи", 129.00, 40),
    ("Бананы, 1 кг", "Фрукты и овощи", 119.00, 30),
    ("Картофель, 1 кг", "Фрукты и овощи", 45.00, 50),
    ("Яйца С1, 10 шт", "Яйца", 109.00, 12),
    ("Гречка, 900 г", "Крупы", 99.00, 3),
]


class ProductIn(BaseModel):
    name: str
    category: str
    price: float = Field(gt=0)
    stock: int = Field(ge=0)


class ReserveItem(BaseModel):
    product_id: int
    quantity: int = Field(gt=0)


class ReserveRequest(BaseModel):
    items: list[ReserveItem]


def db():
    """Открывает соединение с PostgreSQL. Строки возвращаются как словари."""
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    """При запуске создаём таблицу (если её нет) и заполняем стартовыми товарами."""
    for attempt in range(30):  # БД может стартовать чуть позже сервиса — пробуем несколько раз
        try:
            with db() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS products (
                        id       SERIAL PRIMARY KEY,
                        name     TEXT NOT NULL,
                        category TEXT NOT NULL,
                        price    NUMERIC(10, 2) NOT NULL,
                        stock    INTEGER NOT NULL CHECK (stock >= 0)
                    )
                """)
                count = conn.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"]
                if count == 0:
                    conn.cursor().executemany(
                        "INSERT INTO products (name, category, price, stock) VALUES (%s, %s, %s, %s)",
                        SEED_PRODUCTS,
                    )
                    log.info("Таблица products заполнена стартовыми товарами")
            cache.delete(CACHE_KEY)
            log.info("Подключение к PostgreSQL и Redis установлено")
            return
        except psycopg.OperationalError:
            log.info("PostgreSQL ещё не готов, ждём... (попытка %s)", attempt + 1)
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к PostgreSQL")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()  # выполняется один раз при старте сервиса
    yield


app = FastAPI(title="Catalog service", description="Каталог товаров",
              default_response_class=UTF8JSONResponse, lifespan=lifespan)

SELECT_PRODUCTS = "SELECT id, name, category, price::float AS price, stock FROM products"


@app.get("/products", summary="Список товаров (с кэшем Redis)")
def list_products():
    started = time.perf_counter()

    # 1. Сначала смотрим в кэш
    cached = cache.get(CACHE_KEY)
    if cached:
        ms = round((time.perf_counter() - started) * 1000, 1)
        log.info("GET /products -> ИЗ КЭША Redis (%s мс)", ms)
        return {"source": "cache (Redis)", "response_time_ms": ms, "items": json.loads(cached)}

    # 2. Кэш пуст — идём в базу данных
    time.sleep(SLOW_DB_SECONDS)
    with db() as conn:
        items = conn.execute(SELECT_PRODUCTS + " ORDER BY id").fetchall()

    # 3. Кладём результат в кэш на CACHE_TTL секунд
    cache.set(CACHE_KEY, json.dumps(items, ensure_ascii=False), ex=CACHE_TTL)
    ms = round((time.perf_counter() - started) * 1000, 1)
    log.info("GET /products -> кэш пуст, ИЗ БД PostgreSQL (%s мс), сохранено в кэш на %s с", ms, CACHE_TTL)
    return {"source": "database (PostgreSQL)", "response_time_ms": ms, "items": items}


@app.get("/products/{product_id}", summary="Один товар")
def get_product(product_id: int):
    with db() as conn:
        product = conn.execute(SELECT_PRODUCTS + " WHERE id = %s", (product_id,)).fetchone()
    if product is None:
        raise HTTPException(status_code=404, detail=f"Товар {product_id} не найден")
    return product


@app.post("/products", status_code=201, summary="Добавить товар")
def create_product(product: ProductIn):
    with db() as conn:
        created = conn.execute(
            "INSERT INTO products (name, category, price, stock) VALUES (%s, %s, %s, %s) "
            "RETURNING id, name, category, price::float AS price, stock",
            (product.name, product.category, product.price, product.stock),
        ).fetchone()
    cache.delete(CACHE_KEY)  # данные изменились — старый кэш больше не верен
    log.info("Добавлен товар %s, кэш сброшен", created["id"])
    return created


@app.post("/products/reserve", summary="Зарезервировать товары под заказ (вызывает order-service)")
def reserve_products(request: ReserveRequest):
    """
    Списывает остатки сразу для всех товаров заказа.
    Всё происходит в одной транзакции: если хотя бы одного товара не хватает,
    ничего не списывается (откат транзакции).
    """
    reserved = []
    with db() as conn:  # при ошибке внутри блока транзакция откатится автоматически
        for item in request.items:
            # FOR UPDATE блокирует строку, чтобы два заказа одновременно не списали один товар
            product = conn.execute(
                "SELECT id, name, price::float AS price, stock FROM products WHERE id = %s FOR UPDATE",
                (item.product_id,),
            ).fetchone()
            if product is None:
                raise HTTPException(status_code=404, detail=f"Товар {item.product_id} не найден")
            if product["stock"] < item.quantity:
                log.info("Резерв отклонён: '%s' — нужно %s, в наличии %s",
                         product["name"], item.quantity, product["stock"])
                raise HTTPException(
                    status_code=409,
                    detail=f"Недостаточно товара '{product['name']}': нужно {item.quantity}, в наличии {product['stock']}",
                )
            conn.execute("UPDATE products SET stock = stock - %s WHERE id = %s", (item.quantity, item.product_id))
            reserved.append({"product_id": product["id"], "name": product["name"],
                             "price": product["price"], "quantity": item.quantity})
    cache.delete(CACHE_KEY)  # остатки изменились — сбрасываем кэш
    log.info("Зарезервировано позиций: %s, кэш сброшен", len(reserved))
    return {"items": reserved}


@app.get("/health", summary="Проверка, что сервис жив")
def health():
    return {"status": "ok"}
