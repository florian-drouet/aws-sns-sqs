FROM python:3.9-slim

# Set environment variables
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="${VIRTUAL_ENV}/bin:/root/.cargo/bin:$PATH"
ENV PYTHONPATH=/app/src

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv (manually to ensure it’s on the PATH)
COPY --from=ghcr.io/astral-sh/uv:0.5.22 /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy app code
COPY src/ /app/src
COPY tests/ /app/tests
COPY pytest.ini /app/
COPY pyproject.toml /app/
COPY uv.lock /app/
COPY README.md /app/

# Create and sync virtual environment using uv
RUN uv venv && uv sync

# Run the Flask app using gunicorn via uv
CMD ["uv", "run", "gunicorn", "src.main:app", "--bind", "0.0.0.0:8000", "--threads", "4"]