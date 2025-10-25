from kafka import KafkaProducer
import json
import time
from datetime import datetime

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=json_serializer
        )
        return producer
    except Exception as e:
        print(f"Error creating producer: {str(e)}")
        return None

def send_data(producer, topic, data):
    try:
        future = producer.send(topic, data)
        producer.flush()  # Đảm bảo tất cả message được gửi
        future.get(timeout=60)  # Đợi xác nhận
        print(f"Sent data to topic {topic}: {data}")
    except Exception as e:
        print(f"Error sending data: {str(e)}")

def main():
    producer = create_producer()
    if not producer:
        return

    topic = "crawl_data"  # Tên topic để lưu dữ liệu crawl

    try:
        while True:
            # Giả lập dữ liệu crawl được
            data = {
                "timestamp": datetime.now().isoformat(),
                "website": "example.com",
                "data": {
                    "title": "Test Product",
                    "price": 99.99
                }
            }
            
            send_data(producer, topic, data)
            time.sleep(5)  # Đợi 5 giây trước khi gửi data tiếp

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()