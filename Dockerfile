FROM python:3.10-slim-bookworm

WORKDIR /app

ENV PYTHONPATH=/app/src
ENV PYTHON_BIN=python

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["sh", "scripts/start.sh", "pipeline"]
