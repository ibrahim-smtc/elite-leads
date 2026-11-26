# Google Sheets -> Perfox CRM connector
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY sheets_connector ./sheets_connector

RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000
# API + scheduler in one process. To scale out: SCHEDULER_ENABLED=false here, and run
# workers with:  python -m sheets_connector.server worker
CMD ["sh", "-c", "uvicorn sheets_connector.server:app --host 0.0.0.0 --port ${PORT:-8000} --proxy-headers"]
