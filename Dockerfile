FROM python:3.12.13-slim-bookworm@sha256:4766d8b510c428e595d74b9cc5bbb2fae8e26316fffb4adc89908d79aacd58a2
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src DATA_DIR=/app/data
WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r requirements.lock \
    && groupadd --gid 10001 alert \
    && useradd --uid 10001 --gid alert --no-create-home alert \
    && install -d -m 750 -o alert -g alert /app/data
COPY src/live_alert/*.py /app/src/live_alert/
USER 10001:10001
ENTRYPOINT ["python", "-m", "live_alert"]
CMD ["run"]
