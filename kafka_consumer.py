from kafka import KafkaConsumer
import json
import os
import requests
import time
import hashlib

# ---  topics and output paths ---
topics = ["theicon-topic", "target-topic"]

# ensure output folders
os.makedirs("output", exist_ok=True)
for topic in topics:
    os.makedirs(os.path.join("output", "images", topic), exist_ok=True)

downloaded_urls = set()

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='image-consumer',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("[INFO] Consumer started, listening to topics:", topics)

for msg in consumer:
    try:
        data = msg.value
        topic = msg.topic

        if not data:
            print(f"[WARN] Nhận được message rỗng từ topic {topic}")
            continue

        img_url = data.get("image_url")
        # tên file cơ bản: lấy id hoặc title
        title = str(data.get("title") or data.get("name") or f"product_{data.get('id', '0')}").replace(" ", "_")
        image_folder = os.path.join("output", "images", topic)

        if not img_url or img_url.startswith("data:image"):
            print(f"[WARN] Bỏ qua ảnh không hợp lệ từ topic {topic}")
            # vẫn lưu message nhận được
            out_file = os.path.join("output", f"{topic}_received.json")
            with open(out_file, "a", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
                f.write("\n")
            continue

        if img_url in downloaded_urls:
            print(f"[INFO] Bỏ qua ảnh trùng lặp: {img_url}")
            # lưu message
            out_file = os.path.join("output", f"{topic}_received.json")
            with open(out_file, "a", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
                f.write("\n")
            continue

        try:
            response = requests.get(img_url, timeout=15, stream=True)
            if response.status_code == 200:
                url_hash = hashlib.md5(img_url.encode()).hexdigest()[:8]
                # lấy extension từ content-type hoặc url
                content_type = response.headers.get("content-type", "")
                ext = ".jpg"
                if "png" in content_type:
                    ext = ".png"
                elif "webp" in content_type:
                    ext = ".webp"
                else:
                    from urllib.parse import urlparse
                    p = urlparse(img_url).path
                    if "." in p:
                        ext = os.path.splitext(p)[1] or ext

                filename = f"{title}_{url_hash}{ext}"
                filepath = os.path.join(image_folder, filename)

                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(1024 * 8):
                        if chunk:
                            f.write(chunk)

                downloaded_urls.add(img_url)
                print(f"[{topic}] Saved image: {filepath}")

                # lưu message + local filename
                data.setdefault("downloaded_image", filename)
                out_file = os.path.join("output", f"{topic}_received.json")
                with open(out_file, "a", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                    f.write("\n")

            else:
                print(f"[{topic}] HTTP {response.status_code} error for {img_url}")
        except Exception as e:
            print(f"[{topic}] Error downloading image: {e}")

    except Exception as e:
        print(f"[ERROR] Unexpected error while processing message: {e}")