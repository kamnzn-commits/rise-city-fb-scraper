# Dockerfile cho Playwright + Chromium trên Render Starter
# Sử dụng image chính thức của Microsoft Playwright (đã có sẵn Chromium + deps)
FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

# Cài thêm Vietnamese fonts (để render text VN đúng)
RUN apt-get update && apt-get install -y \
    fonts-noto \
    fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Verify Chromium is installed (Microsoft image đã có sẵn)
RUN python -c "from playwright.sync_api import sync_playwright; \
    with sync_playwright() as p: \
        print('Chromium path:', p.chromium.executable_path)"

# Copy app code
COPY . .

# Expose port (Render tự set $PORT)
EXPOSE 8080

# Single worker, single thread - tối ưu RAM cho Chromium
CMD gunicorn --bind 0.0.0.0:$PORT \
    --workers 1 \
    --threads 2 \
    --timeout 180 \
    --log-level info \
    app:app
