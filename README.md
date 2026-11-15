# Продуктовый интернет-магазин на микросервисах

Учебный проект: несколько микросервисов на Python (FastAPI), база данных PostgreSQL,
кэш Redis, брокер сообщений RabbitMQ и API gateway на nginx. Всё запускается одной командой
через Docker Compose.

## Компоненты

| Компонент | Роль | Адрес |
|---|---|---|
| `catalog-service` | Каталог товаров и остатки. Хранит в PostgreSQL, список кэширует в Redis на 60 с | http://localhost:8001/docs |
| `cart-service` | Корзина покупателя. Хранится в Redis, живёт 24 часа | http://localhost:8002/docs |
| `order-service` | Оформление заказа: резерв товаров, сохранение в PostgreSQL, событие в RabbitMQ | http://localhost:8003/docs |
| `notification-service` | Слушает RabbitMQ и «отправляет письмо» о заказе (пишет в лог) | — |
| `gateway` (nginx) | Единая точка входа: `/products`, `/cart`, `/orders` | http://localhost:8080 |
| PostgreSQL | База данных: товары, заказы | внутри Docker |
| Redis | Кэш каталога и хранилище корзин | внутри Docker |
| RabbitMQ | Брокер сообщений, очередь `order.created` | http://localhost:15672 (guest / guest) |

## Как сервисы общаются

* **Синхронно (HTTP):** cart → catalog (проверка товара); order → cart (взять корзину),
  order → catalog (списать остатки), order → cart (очистить корзину).
* **Асинхронно (RabbitMQ):** order публикует событие `order.created`, notification его читает.
  Order не ждёт отправки письма и сразу отвечает клиенту.

## Запуск

Нужен установленный и запущенный Docker Desktop. В папке проекта:

```
docker compose up --build
```

Первый запуск скачивает образы и занимает несколько минут. Остановка: `Ctrl+C`,
затем `docker compose down`. Сбросить базу данных полностью: `docker compose down -v`.

## Минимальные сценарии

**1. Просмотр каталога и кэш.** В http://localhost:8001/docs выполнить `GET /products` два раза подряд.
Первый ответ: `"source": "database (PostgreSQL)"`, около 500 мс. Второй: `"source": "cache (Redis)"`, около 1 мс.
Ключ в Redis: `docker compose exec redis redis-cli keys "*"`.

**2. Корзина.** В http://localhost:8002/docs выполнить `POST /cart/{user_id}/items` с `user_id = 1`
и телом `{"product_id": 1, "quantity": 2}`, затем ещё раз с `{"product_id": 6, "quantity": 3}`.
`GET /cart/1` покажет корзину. В Redis: `docker compose exec redis redis-cli hgetall cart:1`.
Несуществующий товар (`product_id: 99`) вернёт ошибку 404.

**3. Оформление заказа и брокер.** В http://localhost:8003/docs выполнить `POST /orders`
с телом `{"user_id": 1}`. Ответ: заказ с суммой. Затем:
* `GET /products/1` в каталоге: остаток уменьшился на 2;
* `GET /cart/1`: корзина пуста;
* `docker compose logs notification-service`: «письмо» о заказе.

Наглядная проверка асинхронности: `docker compose stop notification-service`, оформить заказ,
в панели RabbitMQ (вкладка Queues) видно сообщение, ждущее в очереди `order.created`.
Потом `docker compose start notification-service`: сообщение обработается и очередь опустеет.

**4. Нехватка товара.** Добавить в корзину пользователя 2 товар 10 (гречка, на складе 3 шт.)
в количестве 5 и оформить заказ: ответ 409 «Недостаточно товара», остатки не изменились.

**5. Gateway.** Те же запросы через единую точку входа: http://localhost:8080/products,
http://localhost:8080/cart/1, http://localhost:8080/orders?user_id=1.

## Полезные команды

```
docker compose ps                         # какие контейнеры запущены
docker compose logs -f order-service      # логи одного сервиса в реальном времени
docker compose exec postgres psql -U shop -d shop -c "SELECT * FROM orders;"
```
