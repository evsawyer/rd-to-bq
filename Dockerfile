FROM python:3.12-slim-bookworm
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy only the requirements file first
COPY pyproject.toml .
COPY uv.lock .

RUN uv sync --locked

# Copy only the necessary application files
COPY BigQueryClient.py .
COPY RedditAdsClient.py .
COPY RedditToBigQueryPipeline.py .
COPY main.py .

# Set the PORT environment variable to 8080
ENV PORT=8080

# Expose the port
EXPOSE 8080

# Run the application with uvicorn
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]