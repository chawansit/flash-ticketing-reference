FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml ./
COPY requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY src ./src
RUN pip install --no-cache-dir --no-deps . && useradd --uid 10001 --create-home ticketing
COPY migrations ./migrations
COPY tests ./tests
USER ticketing
EXPOSE 8000
CMD ["uvicorn", "ticketing.api:app", "--host", "0.0.0.0", "--port", "8000", "--limit-concurrency", "256", "--timeout-keep-alive", "5"]
