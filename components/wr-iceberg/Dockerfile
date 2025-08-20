FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code/
COPY components/common/pyproject.toml .
COPY components/common/uv.lock .
COPY components/common/scripts/build_n_test.sh scripts/build_n_test.sh
ENV UV_PROJECT_ENVIRONMENT="/usr/local/"

RUN uv sync --all-groups --frozen

COPY components/wr-iceberg/src/ src
COPY components/wr-iceberg/tests/ tests
#COPY components/common/flake8.cfg .

CMD ["python", "-u", "/code/src/component.py"]