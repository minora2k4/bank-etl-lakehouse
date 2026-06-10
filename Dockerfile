FROM python:3.10-slim-bookworm

WORKDIR /app

ENV PYTHONPATH=/app/src
ENV PYTHON_BIN=python

COPY . /app

CMD ["sh", "scripts/run_pipeline.sh"]
