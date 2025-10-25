import os
import json
import requests
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse, urljoin
from kafka import KafkaProducer

# --- CHANGED: topic and output paths ---
TOPIC = "theicon-topic"
OUTPUT_JSON = os.path.join("output", f"{TOPIC}_products.json")
IMAGES_FOLDER = os.path.join("output", "images", TOPIC)
os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs("output", exist_ok=True)

# Hàm helper chọn URL từ srcset
def pick_from_srcset(srcset):
    try:
        parts = [p.strip() for p in srcset.split(',') if p.strip()]
        # chọn URL có độ phân giải cao nhất (thường là phần cuối)
        last = parts[-1]
        url = last.split()[0]
        return url
    except:
        return None

# Hàm download ảnh - đã sửa để xử lý tốt hơn và có retry
def download_image(img_url, folder_path, filename):
    try:
        if not img_url:
            print(f" URL ảnh không hợp lệ: {img_url}")
            return None

        # đảm bảo folder tồn tại
        os.makedirs(folder_path, exist_ok=True)
        
        # nếu là srcset thì lấy 1 URL
        if ',' in img_url and ' ' in img_url:
            picked = pick_from_srcset(img_url)
            if picked:
                img_url = picked

        if not img_url.startswith(('http://', 'https://')):
            print(f" URL ảnh không bắt đầu bằng http(s), bỏ qua: {img_url}")
            return None

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.theiconic.com.au/'
        }

        # retry đơn giản
        last_exc = None
        for attempt in range(3):
            try:
                print(f" Đang tải ảnh từ: {img_url} (attempt {attempt+1})")
                response = requests.get(img_url, headers=headers, timeout=30, stream=True)
                response.raise_for_status()
                content_type = response.headers.get('content-type', '')
                if 'image' not in content_type:
                    print(f"  Cảnh báo: content-type không phải image: {content_type}")
                # Lấy extension từ path
                parsed_url = urlparse(img_url)
                file_ext = os.path.splitext(parsed_url.path)[1]
                if not file_ext:
                    if 'jpeg' in content_type or 'jpg' in content_type:
                        file_ext = '.jpg'
                    elif 'png' in content_type:
                        file_ext = '.png'
                    elif 'webp' in content_type:
                        file_ext = '.webp'
                    else:
                        file_ext = '.jpg'

                safe_name = filename.replace('/', '_').replace('\\', '_').replace(':', '_').strip()
                file_path = os.path.join(folder_path, f"{safe_name}{file_ext}")

                # ghi file theo chunk
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(1024 * 8):
                        if chunk:
                            f.write(chunk)

                file_size = os.path.getsize(file_path)
                print(f" Đã tải ảnh: {os.path.basename(file_path)} ({file_size} bytes)")
                return os.path.basename(file_path)

            except Exception as e:
                last_exc = e
                print(f"  Lỗi tải ảnh (attempt {attempt+1}): {e}")
                time.sleep(1)
                continue

        print(f" Thất bại tải ảnh sau 3 lần: {last_exc}")
        return None

    except Exception as e:
        print(f" Lỗi tổng quát khi tải ảnh {filename}: {e}")
        return None

# Setup driver
driverpath = r"D:\chromedriver-win64\chromedriver.exe"
service = Service(driverpath)
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(service=service, options=options)
driver.set_page_load_timeout(30)  # Page load timeout
driver.set_script_timeout(30)     # Script timeout
driver.implicitly_wait(10)        # Implicit wait for elements
wait = WebDriverWait(driver, 20)  # Explicit wait object

# Tạo Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Truy cập trang
url = "https://www.theiconic.com.au/all?campaign=lp-gtm-w-gifts-oct25"
print(" Đang tải trang...")
driver.get(url)
time.sleep(10)

def safe_scroll(driver, retries=3, scroll_pause=2.0):
    """Safely scroll with retries and error handling"""
    for attempt in range(retries):
        try:
            # Get current scroll position
            current = driver.execute_script("return window.pageYOffset;")
            
            # Scroll in smaller increments
            driver.execute_script("""
                window.scrollTo({
                    top: window.pageYOffset + 800,
                    behavior: 'smooth'
                });
            """)
            
            # Wait for scroll
            time.sleep(scroll_pause)
            
            # Check if actually scrolled
            new_position = driver.execute_script("return window.pageYOffset;")
            if new_position > current:
                return True
            
        except Exception as e:
            print(f"[WARN] Scroll attempt {attempt + 1} failed: {str(e)}")
            time.sleep(scroll_pause)
    
    return False

# Scroll để load sản phẩm
print(" Đang tải tất cả sản phẩm...")
for i in range(8):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    print(f" Scroll lần {i+1}/8")
    time.sleep(2)

time.sleep(5)

print("Đang thu thập dữ liệu sản phẩm...")
products_data = []

try:
    # Initial wait for page load
    time.sleep(5)
    
    # Scroll with improved error handling
    scroll_count = 0
    max_scrolls = 10
    
    while scroll_count < max_scrolls:
        # Try to scroll
        if not safe_scroll(driver, retries=3, scroll_pause=2.0):
            print("[INFO] Reached bottom or scroll failed")
            break
            
        scroll_count += 1
        print(f"Scroll {scroll_count}/{max_scrolls}")
        
        # Find products after each scroll
        product_containers = driver.find_elements(By.CSS_SELECTOR, "#catalogProductsList > div")
        if len(product_containers) >= 50:  # Or your desired minimum
            print(f"Found enough products: {len(product_containers)}")
            break
    
    print(f"Tìm thấy {len(product_containers)} sản phẩm (container)")
    
    for container in product_containers:
        try:
            # Lấy tên sản phẩm trước hết
            name = ""
            try:
                name_elem = container.find_element(By.CSS_SELECTOR, "span.name")
                name = name_elem.text.strip()
            except:
                print(" Không tìm thấy tên, bỏ qua container")
                continue

            # Lấy brand
            brand = ""
            try:
                if " - " in name:
                    brand = name.split(" - ")[0].strip()
                else:
                    brand_elem = container.find_element(By.CSS_SELECTOR, "span.brand")
                    brand = brand_elem.text.strip()
            except:
                brand = ""

            # Discount (lấy sớm để dùng khi cần tính giá)
            discount = ""
            try:
                discount_elem = container.find_element(By.CSS_SELECTOR, "span.message.marketing.warp-extended-text")
                discount = discount_elem.text.strip()
            except:
                discount = ""

            # Giá
            price_original = ""
            price_final = ""
            try:
                original_elem = container.find_element(By.CSS_SELECTOR, "div.price.original, span.price.original")
                price_original = original_elem.text.strip()
            except:
                pass

            try:
                final_elem = container.find_element(By.CSS_SELECTOR, "div.price.final, span.price.final, span.price")
                price_final = final_elem.text.strip()
            except:
                # nếu không tìm thấy final thì vẫn tiếp tục (site có thể show only original)
                price_final = price_final or ""

            # Nếu giá ghép sai (final == original) nhưng có discount dạng "NN% OFF", tính lại final
            def parse_price(p):
                try:
                    import re
                    if not p:
                        return None
                    m = re.search(r'[\d\.,]+', p)
                    if not m:
                        return None
                    s = m.group(0).replace(',', '')
                    return float(s)
                except:
                    return None

            def format_price(v):
                if v is None:
                    return ""
                # giữ 2 chữ số thập phân như site
                return f"${v:,.2f}"

            # chuẩn hóa và sửa nếu cần
            try:
                orig_val = parse_price(price_original)
                final_val = parse_price(price_final)
                # tìm percent trong discount (ví dụ "25% OFF...")
                import re
                pct = None
                m_pct = re.search(r'(\d{1,3})\s*%', discount or "", re.IGNORECASE)
                if m_pct:
                    pct = float(m_pct.group(1)) / 100.0

                if pct is not None and orig_val is not None:
                    calc_final = round(orig_val * (1.0 - pct), 2)
                    # nếu final không tìm được hoặc final == original (chưa giảm) hoặc chênh nhiều, cập nhật
                    if final_val is None or abs((final_val or 0) - calc_final) > 0.01 or (final_val == orig_val):
                        price_final = format_price(calc_final)
                        # giữ price_original như ban đầu (site có thể hiển thị cả hai)
                        print(f"  Điều chỉnh price_final => {price_final} (từ {price_original}, giảm {int(pct*100)}%)")
            except Exception as e:
                print(f"  Lỗi xử lý giá: {e}")
            # Discount
            discount = ""
            try:
                discount_elem = container.find_element(By.CSS_SELECTOR, "span.message.marketing.warp-extended-text")
                discount = discount_elem.text.strip()
            except:
                discount = ""

            # Ảnh: thử nhiều attribute, xử lý srcset, relative URL
            image_url = ""
            try:
                img_elem = None
                selectors = ["a.product-image-link img", "img[src*='static.theiconic.com.au']", "img.product-image", "img[data-src*='static.theiconic.com.au']", "img"]
                for sel in selectors:
                    try:
                        img_elem = container.find_element(By.CSS_SELECTOR, sel)
                        if img_elem:
                            break
                    except:
                        img_elem = None
                if img_elem:
                    # ưu tiên src, data-src, data-lazy-src, srcset
                    image_url = img_elem.get_attribute("src") or img_elem.get_attribute("data-src") or img_elem.get_attribute("data-lazy-src") or img_elem.get_attribute("data-srcset") or img_elem.get_attribute("srcset")
                    if image_url and (image_url.startswith("//")):
                        image_url = "https:" + image_url
                    if image_url and not image_url.startswith("http"):
                        image_url = urljoin(url, image_url)
                else:
                    image_url = ""
            except Exception as e:
                print(f" Lỗi lấy ảnh: {e}")
                image_url = ""

            # Chuẩn bị filename và seq dựa trên số sản phẩm đã lưu (tránh đánh nhầm)
            seq = len(products_data) + 1
            safe_brand = "".join(c for c in brand if c.isalnum() or c in (' ', '-', '_')).rstrip() or "unknown_brand"
            safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip() or "unknown_product"
            filename_base = f"{seq:03d}_{safe_brand}_{safe_name}"
            filename_base = "".join(c for c in filename_base if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename_base = filename_base[:50]

            image_filename = None
            if image_url:
                image_filename = download_image(image_url, IMAGES_FOLDER, filename_base)
            else:
                print(" Không tìm thấy URL ảnh, bỏ qua tải ảnh")

            product_info = {
                "id": seq,
                "brand": brand,
                "name": name,
                "price_original": price_original,
                "price_final": price_final,
                "discount": discount,
                "image_url": image_url,
                "image_filename": image_filename
            }

            # Gửi vào Kafka
            producer.send(TOPIC, product_info)
            producer.flush()

            products_data.append(product_info)
            print(f"Đã thêm và gửi Kafka sản phẩm #{seq}: {brand} - {name}")

        except Exception as e:
            print(f" Lỗi xử lý 1 sản phẩm: {e}")
            continue

except Exception as e:
    print(f" Lỗi thu thập dữ liệu: {e}")

# Đóng producer khi hoàn thành
finally:
    producer.close()
    driver.quit()

# Lưu dữ liệu riêng theo topic
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(products_data, f, ensure_ascii=False, indent=2)

print(f"\n🎉 HOÀN THÀNH! Đã lưu {len(products_data)} sản phẩm vào {OUTPUT_JSON}")
# Đếm số ảnh đã tải thành công
downloaded_count = sum(1 for p in products_data if p.get('image_filename'))
print(f"🖼️ Đã tải {downloaded_count} ảnh")

# Hiển thị kết quả chi tiết
if products_data:
    print(f"\n CHI TIẾT SẢN PHẨM:")
    for product in products_data:
        print(f"\n{product['id']}. {product['brand']} - {product['name']}")
        print(f"    Giá gốc: {product['price_original'] or 'Không có'}")
        print(f"    Giá hiện tại: {product['price_final']}")
        if product['discount']:
            print(f"    Khuyến mãi: {product['discount']}")
        print(f"   URL ảnh: {product['image_url'][:100] if product['image_url'] else 'Không có'}...")
        if product['image_filename']:
            print(f"    File ảnh: {product['image_filename']}")
        else:
            print(f"    Không có file ảnh")
        print("-" * 100)
else:
    print(" Không có sản phẩm nào được thu thập")