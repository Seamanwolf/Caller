FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY broker_call_bot.py .
COPY employee_data_provider.py .
COPY employees_export.py .
COPY export/ ./export/
COPY employees.xlsx .

RUN useradd -m -u 1000 botuser && chown -R botuser:botuser /app
USER botuser

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "broker_call_bot.py"]
