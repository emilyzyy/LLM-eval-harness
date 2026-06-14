FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml ./
COPY designops_eval/ ./designops_eval/
COPY evals/ ./evals/

RUN pip install --no-cache-dir -e .

ENTRYPOINT ["python", "-m", "designops_eval.cli"]
CMD ["--help"]
