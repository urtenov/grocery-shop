"""
Cart service — корзина покупателя.

Корзина хранится прямо в Redis (без PostgreSQL), потому что:
  * это временные данные — брошенная корзина сама удалится через 24 часа (TTL);
  * Redis очень быстрый, а корзину дёргают постоянно.

Устройство в Redis: ключ "cart:<user_id>" — это хэш (словарь),
где поле = id товара, значение = количество. Например:
    cart:1  ->  {"3": "2", "5": "1"}   (2 шт. товара №3 и 1 шт. товара №5)
"""
import logging
import os

import httpx
import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [cart] %(message)s")
log = logging.getLogger("cart")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CATALOG_URL = os.getenv("CATALOG_URL", "http://localhost:8001")  # адрес соседнего сервиса
CART_TTL = 24 * 60 * 60  # корзина живёт 24 часа


class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"


app = FastAPI(title="Cart service", description="Корзина покупателя", default_response_class=UTF8JSONResponse)
store = redis.Redis.from_url(REDIS_URL, decode_responses=True)


class CartItem(BaseModel):
    product_id: int
    quantity: int = Field(gt=0, description="Сколько штук добавить")


def cart_key(user_id: int) -> str:
    return f"cart:{user_id}"


@app.get("/cart/{user_id}", summary="Содержимое корзины")
def get_cart(user_id: int):
    raw = store.hgetall(cart_key(user_id))
    items = [{"product_id": int(pid), "quantity": int(qty)} for pid, qty in raw.items()]
    return {"user_id": user_id, "items": items, "expires_in_seconds": store.ttl(cart_key(user_id))}


@app.post("/cart/{user_id}/items", summary="Добавить товар в корзину")
def add_item(user_id: int, item: CartItem):
    # Синхронный вызов другого микросервиса по HTTP: проверяем, что такой товар существует
    try:
        response = httpx.get(f"{CATALOG_URL}/products/{item.product_id}", timeout=5)
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="Каталог недоступен, попробуйте позже")
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail=f"Товар {item.product_id} не найден в каталоге")
    product = response.json()

    key = cart_key(user_id)
    store.hincrby(key, str(item.product_id), item.quantity)  # увеличиваем количество
    store.expire(key, CART_TTL)                               # продлеваем жизнь корзины
    log.info("Пользователь %s добавил '%s' x%s", user_id, product["name"], item.quantity)
    return get_cart(user_id)


@app.delete("/cart/{user_id}/items/{product_id}", summary="Убрать товар из корзины")
def remove_item(user_id: int, product_id: int):
    store.hdel(cart_key(user_id), str(product_id))
    return get_cart(user_id)


@app.delete("/cart/{user_id}", summary="Очистить корзину (вызывает order-service после заказа)")
def clear_cart(user_id: int):
    store.delete(cart_key(user_id))
    log.info("Корзина пользователя %s очищена", user_id)
    return {"user_id": user_id, "items": []}


@app.get("/health", summary="Проверка, что сервис жив")
def health():
    return {"status": "ok"}
