FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium

COPY . .

EXPOSE 8080

CMD gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 2 --timeout 180 --log-level info app:app
