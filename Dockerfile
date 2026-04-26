# Dockerfile cho Playwright + Chromium trên Render
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

# Cài thêm Vietnamese fonts
RUN apt-get update && apt-get install -y \
    fonts-noto \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose port
EXPOSE 8080

# Single worker - tối ưu RAM cho Chromium
CMD gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 2 --timeout 180 --log-level info app:app
