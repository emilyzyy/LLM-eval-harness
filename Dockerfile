# Keep in step with .python-version and .github/workflows/tests.yml.
FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml ./
COPY designops_eval/ ./designops_eval/
COPY evals/ ./evals/

RUN pip install --no-cache-dir -e .

ENTRYPOINT ["python", "-m", "designops_eval.cli"]
CMD ["--help"]
