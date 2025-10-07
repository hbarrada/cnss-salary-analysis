# Dockerfile
FROM python:3.11-slim

# System deps (psycopg2 needs libpq-dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev gcc curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your code
COPY . .

# Runtime env (override in compose if needed)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FLASK_ENV=production \
    PORT=5000

# Expose Flask port
EXPOSE 5000

# IMPORTANT: ensure your app binds to 0.0.0.0:5000
# Your README says entrypoint is: python -m src.app
CMD ["python", "-m", "src.app"]
