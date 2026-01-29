FROM python:3.12-slim

# Устанавливаем git и зависимости для dbt
RUN apt-get update && apt-get install -y git curl && \
    pip install --no-cache-dir dbt-core==1.11.2 dbt-postgres==1.10.0

# Рабочая директория
WORKDIR /dbt

# По умолчанию держим контейнер живым
CMD ["tail", "-f", "/dev/null"]
