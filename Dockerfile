FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir -e .
COPY src ./src
CMD ["python", "-m", "indicators_engine.app"]
