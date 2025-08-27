FROM python:3.11-slim

WORKDIR /app

# Tizim kutubxonalarini o'rnatish
RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Skriptni ishga tushurish
CMD ["python", "bot.py"]
