# Use Python 3.11 slim image
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN python -m playwright install --with-deps chromium

# Copy application code
COPY app/ ./app/
COPY routes/ ./routes/
COPY services/ ./services/
COPY db/ ./db/
COPY tools/ ./tools/
COPY alembic/ ./alembic/
COPY static/ ./static/
COPY templates/ ./templates/
COPY alembic.ini ./
COPY well_number_extractor.py ./
COPY background_cron.py ./
COPY start.sh ./

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONPATH=/app

# Make start script executable
RUN chmod +x start.sh

# Run the application with migrations
CMD ["./start.sh"]
