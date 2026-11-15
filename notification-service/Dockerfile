# Берём готовый образ с Python 3.12 (slim — облегчённая версия)
FROM python:3.12-slim

# Логи сразу выводятся в консоль, без задержки
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Сначала ставим библиотеки (этот слой кэшируется и не пересобирается без нужды)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Потом копируем код сервиса
COPY . .

# Команда запуска контейнера
CMD ["python", "main.py"]
