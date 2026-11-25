FROM python:3.12.10-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /code
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip pip install --timeout=120 --retries=6 -r requirements.txt
COPY app app
COPY schemas schemas
COPY sql sql
COPY tests tests
COPY infra/grafana/dashboards infra/grafana/dashboards
CMD ["uvicorn", "app.producer.main:app", "--host", "0.0.0.0", "--port", "8000"]
