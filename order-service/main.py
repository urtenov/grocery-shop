"""
Order service — оформление заказов.

Как проходит оформление заказа (POST /orders):
  1. Берём корзину пользователя у cart-service            (синхронно, HTTP)
  2. Просим catalog-service списать товары со склада      (синхронно, HTTP)
     - если чего-то не хватает, заказ не создаётся (ошибка 409)
  3. Сохраняем заказ в PostgreSQL
  4. Публикуем событие "order.created" в RabbitMQ          (АСИНХРОННО)
     - order-service не ждёт, пока письмо отправится, а сразу отвечает клиенту;
       событие заберёт notification-service, когда будет готов
  5. Очищаем корзину

Здесь виден третий обязательный компонент задания: БРОКЕР СООБЩЕНИЙ.
"""
import json
import logging
import os
import time
from contextlib import asynccontextmanager

import httpx
import pika
import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [order] %(message)s")
log = logging.getLogger("order")
logging.getLogger("pika").setLevel(logging.WARNING)  # убираем служебный шум библиотеки pika

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://shop:shop@localhost:5432/shop")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8001")
CART_URL = os.getenv("CART_URL", "http://localhost:8002")
QUEUE = "order.created"  # имя очереди в RabbitMQ


class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"


class OrderRequest(BaseModel):
    user_id: int


def db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def init_db():
    for attempt in range(30):
        try:
            with db() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        id         SERIAL PRIMARY KEY,
                        user_id    INTEGER NOT NULL,
                        total      NUMERIC(10, 2) NOT NULL,
                        status     TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS order_items (
                        id         SERIAL PRIMARY KEY,
                        order_id   INTEGER NOT NULL REFERENCES orders(id),
                        product_id INTEGER NOT NULL,
                        name       TEXT NOT NULL,
                        price      NUMERIC(10, 2) NOT NULL,
                        quantity   INTEGER NOT NULL
                    )
                """)
            log.info("Подключение к PostgreSQL установлено")
            return
        except psycopg.OperationalError:
            log.info("PostgreSQL ещё не готов, ждём... (попытка %s)", attempt + 1)
            time.sleep(2)
    raise RuntimeError("Не удалось подключиться к PostgreSQL")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Order service", description="Оформление заказов",
              default_response_class=UTF8JSONResponse, lifespan=lifespan)


def publish_event(event: dict):
    """Отправляет событие в очередь RabbitMQ."""
    connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
    try:
        channel = connection.channel()
        # durable=True — очередь переживёт перезапуск RabbitMQ
        channel.queue_declare(queue=QUEUE, durable=True)
        channel.basic_publish(
            exchange="",          # стандартный обменник: доставляет сообщение в очередь с именем routing_key
            routing_key=QUEUE,
            body=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # сообщение сохраняется на диск и не потеряется при перезапуске
            ),
        )
    finally:
        connection.close()


@app.post("/orders", status_code=201, summary="Оформить заказ из корзины")
def create_order(request: OrderRequest):
    user_id = request.user_id

    # 1. Корзина
    try:
        cart = httpx.get(f"{CART_URL}/cart/{user_id}", timeout=5).json()
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="Сервис корзины недоступен")
    if not cart["items"]:
        raise HTTPException(status_code=400, detail="Корзина пуста — нечего заказывать")

    # 2. Резерв товаров на складе
    try:
        response = httpx.post(f"{CATALOG_URL}/products/reserve", json={"items": cart["items"]}, timeout=5)
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="Каталог недоступен")
    if response.status_code in (404, 409):
        log.info("Заказ пользователя %s отклонён: %s", user_id, response.json()["detail"])
        raise HTTPException(status_code=response.status_code, detail=response.json()["detail"])
    response.raise_for_status()
    items = response.json()["items"]
    total = round(sum(item["price"] * item["quantity"] for item in items), 2)

    # 3. Сохраняем заказ в БД
    with db() as conn:
        order = conn.execute(
            "INSERT INTO orders (user_id, total, status) VALUES (%s, %s, 'created') "
            "RETURNING id, user_id, total::float AS total, status, created_at",
            (user_id, total),
        ).fetchone()
        for item in items:
            conn.execute(
                "INSERT INTO order_items (order_id, product_id, name, price, quantity) VALUES (%s, %s, %s, %s, %s)",
                (order["id"], item["product_id"], item["name"], item["price"], item["quantity"]),
            )
    order["created_at"] = order["created_at"].isoformat()
    order["items"] = items
    log.info("Создан заказ #%s пользователя %s на сумму %.2f руб.", order["id"], user_id, total)

    # 4. Асинхронное событие в брокер
    try:
        publish_event({"event": "order.created", "order_id": order["id"], "user_id": user_id,
                       "total": total, "items": items})
        log.info("Событие order.created для заказа #%s отправлено в RabbitMQ", order["id"])
    except pika.exceptions.AMQPError as error:
        # Заказ уже сохранён, поэтому не роняем запрос — только пишем ошибку в лог
        log.error("Не удалось отправить событие в RabbitMQ: %r", error)

    # 5. Очищаем корзину
    try:
        httpx.delete(f"{CART_URL}/cart/{user_id}", timeout=5)
    except httpx.HTTPError:
        log.error("Не удалось очистить корзину пользователя %s", user_id)

    return order


@app.get("/orders", summary="Заказы пользователя")
def list_orders(user_id: int):
    with db() as conn:
        orders = conn.execute(
            "SELECT id, user_id, total::float AS total, status, created_at FROM orders "
            "WHERE user_id = %s ORDER BY id DESC",
            (user_id,),
        ).fetchall()
    for order in orders:
        order["created_at"] = order["created_at"].isoformat()
    return orders


@app.get("/orders/{order_id}", summary="Один заказ с позициями")
def get_order(order_id: int):
    with db() as conn:
        order = conn.execute(
            "SELECT id, user_id, total::float AS total, status, created_at FROM orders WHERE id = %s",
            (order_id,),
        ).fetchone()
        if order is None:
            raise HTTPException(status_code=404, detail=f"Заказ {order_id} не найден")
        order["items"] = conn.execute(
            "SELECT product_id, name, price::float AS price, quantity FROM order_items WHERE order_id = %s",
            (order_id,),
        ).fetchall()
    order["created_at"] = order["created_at"].isoformat()
    return order


@app.get("/health", summary="Проверка, что сервис жив")
def health():
    return {"status": "ok"}
