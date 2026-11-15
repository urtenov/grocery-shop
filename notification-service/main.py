"""
Notification service — уведомления покупателям.

Это не веб-сервер, а "слушатель" (consumer): он подключается к RabbitMQ,
подписывается на очередь "order.created" и обрабатывает каждое событие о новом заказе.

Настоящую почту в учебном проекте не отправляем — вместо этого печатаем
"письмо" в лог. Посмотреть его можно командой:
    docker compose logs -f notification-service
"""
import json
import logging
import os
import time

import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [notification] %(message)s")
log = logging.getLogger("notification")
logging.getLogger("pika").setLevel(logging.WARNING)  # убираем служебный шум библиотеки pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
QUEUE = "order.created"


def connect():
    """Подключаемся к RabbitMQ, повторяя попытки, пока он не запустится."""
    while True:
        try:
            return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        except pika.exceptions.AMQPConnectionError:
            log.info("RabbitMQ ещё не готов, ждём...")
            time.sleep(3)


def on_order_created(channel, method, properties, body):
    """Вызывается автоматически на каждое новое сообщение в очереди."""
    event = json.loads(body)
    lines = "\n".join(
        f"      - {item['name']} x{item['quantity']} = {item['price'] * item['quantity']:.2f} руб."
        for item in event["items"]
    )
    log.info(
        "Получено событие %s из RabbitMQ. Отправляем письмо:\n"
        "    Кому: покупатель #%s\n"
        "    Тема: Ваш заказ #%s оформлен\n"
        "    Состав заказа:\n%s\n"
        "    Итого: %.2f руб.",
        event["event"], event["user_id"], event["order_id"], lines, event["total"],
    )
    # Подтверждаем (ack), что сообщение обработано, — только после этого RabbitMQ удалит его из очереди.
    # Если сервис упадёт до ack, сообщение не потеряется и будет доставлено повторно.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    while True:
        connection = connect()
        try:
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE, durable=True)
            channel.basic_qos(prefetch_count=1)  # брать по одному сообщению за раз
            channel.basic_consume(queue=QUEUE, on_message_callback=on_order_created)
            log.info("Подключились к RabbitMQ, слушаем очередь '%s'", QUEUE)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            log.info("Соединение с RabbitMQ потеряно, переподключаемся...")
            time.sleep(3)


if __name__ == "__main__":
    main()
