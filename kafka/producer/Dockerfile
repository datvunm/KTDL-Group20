FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY booking_producer.py flight_producer.py .

CMD ["python", "booking_producer.py"]
