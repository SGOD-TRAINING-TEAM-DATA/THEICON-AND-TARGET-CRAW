# Dockerfile cho ECOMMERCE-CRAWLER
FROM python:3.10-slim

# Thư mục làm việc trong container
WORKDIR /app

# Copy file requirements.txt trước để tận dụng cache
COPY requirements.txt /app/

# Cài Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY . /app

# Cài Chromium & ChromeDriver
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium chromium-driver libgl1 wget unzip \
    && rm -rf /var/lib/apt/lists/*

# Biến môi trường cho Selenium
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROME_DRIVER=/usr/bin/chromedriver

# Lệnh chạy chính
CMD ["python", "main1.py"]