import os
import time
import requests
import random
import json
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse, urljoin

# Add Kafka configuration
TOPIC = "target-topic"
KAFKA_BROKER = 'localhost:9092'

# Add output paths
OUTPUT_JSON = os.path.join("output", f"{TOPIC}_products.json")
IMAGES_FOLDER = os.path.join("output", "images", TOPIC)
os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs("output", exist_ok=True)

def generate_random_rating_data():
    """Generate random but realistic rating and review data"""
    rating = round(random.uniform(3.5, 5.0), 1)  # Rating between 3.5-5.0
    comments = random.randint(10, 500)  # Reviews between 10-500
    return rating, comments

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# ==== Helpers ====
def load_existing_products():
    """Load existing products from JSON file if exists"""
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                existing_products = json.load(f)
                print(f"📁 Đã tìm thấy {len(existing_products)} sản phẩm từ file cũ")
                return existing_products
        except Exception as e:
            print(f"⚠️ Lỗi khi đọc file cũ: {e}")
    return []

def get_next_product_id(existing_products):
    """Get the next available product ID"""
    if not existing_products:
        return 1
    max_id = max(product.get("id", 0) for product in existing_products)
    return max_id + 1

def wait_for_products(driver, css_selector, min_count=5, timeout=40, scroll_pause=1.0):
    """Wait for products to load with scrolling"""
    end_time = time.time() + timeout
    last_count = -1
    while time.time() < end_time:
        elems = driver.find_elements(By.CSS_SELECTOR, css_selector)
        count = len(elems)
        if count >= min_count:
            time.sleep(0.5)
            return elems
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(scroll_pause)
        if count == last_count:
            time.sleep(0.5)
        last_count = count
    return driver.find_elements(By.CSS_SELECTOR, css_selector)

def safe_filename(s, maxlen=60):
    """Create safe filename from string"""
    safe = "".join(c for c in (s or "") if c.isalnum() or c in (' ', '-', '_')).rstrip()
    return safe[:maxlen].replace(" ", "_") or "product"

def download_all_product_images(img_urls, product_folder, product_id, product_title):
    """Download all images for a product and return list of filenames"""
    if not img_urls:
        return []
    
    filenames = []
    safe_title = safe_filename(product_title)[:40]
    product_folder_path = os.path.join(product_folder, f"{product_id:03d}_{safe_title}")
    os.makedirs(product_folder_path, exist_ok=True)
    
    for i, img_url in enumerate(img_urls):
        try:
            if not img_url or not img_url.startswith(("http://","https://")):
                continue
                
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            resp = requests.get(img_url, headers=headers, timeout=20, stream=True)
            resp.raise_for_status()
            
            # Get file extension
            ext = ".jpg"
            ct = resp.headers.get("content-type","")
            if "png" in ct:
                ext = ".png"
            elif "webp" in ct:
                ext = ".webp"
            else:
                p = urlparse(img_url).path
                if "." in p:
                    ext = os.path.splitext(p)[1] or ext
                    
            filename = f"image_{i+1:02d}{ext}"
            filepath = os.path.join(product_folder_path, filename)
            
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(1024*8):
                    if chunk:
                        f.write(chunk)
            
            filenames.append(filename)
            print(f"    Downloaded image {i+1}/{len(img_urls)}: {filename}")
            
        except Exception as e:
            print(f"    [WARN] Failed to download image {i+1}: {e}")
            continue
    
    return filenames

def extract_all_image_urls(driver):
    """Extract all image URLs from a product page"""
    image_urls = []
    
    # Multiple selectors for product images
    img_selectors = [
        "img[src*='target.scene7.com']",
        "img[data-test*='image']",
        "img[alt*='main']",
        ".carousel-image img",
        "img.product-image",
        "img[src*='images']",
        "[data-test='image-gallery'] img",
        ".slide img",
        ".thumbnail img"
    ]
    
    for selector in img_selectors:
        try:
            img_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for img_elem in img_elements:
                img_url = img_elem.get_attribute("src") or img_elem.get_attribute("data-src") or ""
                if img_url and img_url not in image_urls:
                    # Fix relative URLs
                    if img_url.startswith("//"):
                        img_url = "https:" + img_url
                    elif img_url.startswith("/"):
                        img_url = urljoin(driver.current_url, img_url)
                    
                    # Filter out small images (likely thumbnails)
                    if any(size in img_url for size in ['wid=480', 'wid=200', 'hei=200', 'thumb', 'thumbnail']):
                        continue
                        
                    image_urls.append(img_url)
                    print(f"    Found image URL: {img_url[:80]}...")
        except Exception as e:
            print(f"    [DEBUG] Selector {selector} failed: {e}")
            continue
    
    # Remove duplicates while preserving order
    seen = set()
    unique_image_urls = []
    for url in image_urls:
        if url not in seen:
            seen.add(url)
            unique_image_urls.append(url)
    
    print(f"    Extracted {len(unique_image_urls)} unique image URLs")
    return unique_image_urls[:10]  # Limit to first 10 images to avoid too many

# ==== Load existing data ====
existing_products = load_existing_products()
next_product_id = get_next_product_id(existing_products)
print(f" ID tiếp theo sẽ bắt đầu từ: {next_product_id}")

# ==== WebDriver config ====
driverpath = r"D:\chromedriver-win64\chromedriver.exe"
service = Service(driverpath)
options = webdriver.ChromeOptions()
options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_argument("--window-size=1920,1080")
# options.add_argument("--headless")  # remove headless while debugging

driver = webdriver.Chrome(service=service, options=options)
driver.set_page_load_timeout(60)
driver.set_script_timeout(60)
driver.implicitly_wait(10)
wait = WebDriverWait(driver, 30)

# ==== URL and selectors for Target.com ====
# BẠN CÓ THỂ THAY ĐỔI LINK Ở ĐÂY
list_url = "https://www.target.com/c/-5-under-halloween-finds/-/N-lva9l?Nao=168&moveTo=product-list-grid"
# Hoặc thử link khác: "https://www.target.com/c/halloween-decorations/-/N-4y4wo"

print(f"[INFO] Loading Target.com: {list_url}")

try:
    driver.get(list_url)
    # Wait for page to load completely
    time.sleep(10)
    
    # Scroll to load more products
    print("[INFO] Scrolling to load products...")
    for i in range(6):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * {}/6);".format(i+1))
        time.sleep(2)
        print(f"Scrolled {i+1}/6")

except Exception as e:
    print(f"[ERROR] Failed to load page: {e}")
    driver.quit()
    exit()

# ==== Improved product link extraction ====
print("[INFO] Extracting product links...")
product_links = []

# Multiple selectors for product links on Target
link_selectors = [
    "a[data-test='product-title']",
    "a[href*='/p/']",
    "[data-test='@web/site-top-of-funnel/ProductCardWrapper'] a",
    ".styles__ProductCardWrapper-sc-1vg6q6k-0 a",
    "a[data-test*='product']",
    ".product-card a"
]

for selector in link_selectors:
    try:
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        for elem in elements:
            try:
                href = elem.get_attribute("href")
                if href and "/p/" in href and href not in product_links:
                    product_links.append(href)
                    print(f"Found product link: {href}")
            except Exception as e:
                continue
                
        if product_links:
            print(f"[SUCCESS] Found {len(product_links)} products with selector: {selector}")
            break
    except Exception as e:
        print(f"[DEBUG] Selector {selector} failed: {e}")
        continue

# Remove duplicates
product_links = list(dict.fromkeys(product_links))
print(f"[INFO] Total unique product links: {len(product_links)}")

if not product_links:
    print("[ERROR] No product links found!")
    # Save page for debugging
    with open("debug_page.html", "w", encoding="utf-8") as f:
        f.write(driver.page_source)
    print("[INFO] Saved page source as debug_page.html")
    driver.quit()
    exit()

# ==== Crawl individual product pages ====
new_products = []
max_items = min(20, len(product_links))  # Reduced for testing

print(f"[INFO] Starting to crawl {max_items} product pages...")

# Bắt đầu ID từ giá trị tiếp theo
current_id = next_product_id

for i, link in enumerate(product_links[:max_items], 1):
    try:
        print(f"\n[INFO] Processing product {i}/{max_items} (ID: {current_id})")
        print(f"URL: {link}")
        
        # Navigate to product page
        driver.get(link)
        time.sleep(5)  # Wait for page to load
        
        # Wait for key elements to be present
        try:
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except:
            print(f"[WARN] Page body not loaded for {link}")
            continue

        # ==== Extract product information with multiple fallback selectors ====
        
        # Title
        title = ""
        title_selectors = [
            "h1[data-test='product-title']",
            "h1.styles__Heading*",
            "h1",
            "[data-test='product-name']",
            ".product-title",
            "h1 span"
        ]
        for selector in title_selectors:
            try:
                title_elem = driver.find_element(By.CSS_SELECTOR, selector)
                title = title_elem.text.strip()
                if title:
                    print(f"Title found: {title[:50]}...")
                    break
            except:
                continue

        # Price
        price = ""
        price_selectors = [
            "span[data-test='product-price']",
            "[data-test='current-price']",
            ".styles__CurrentPrice*",
            "[data-test='price']",
            ".price",
            ".product-price"
        ]
        for selector in price_selectors:
            try:
                price_elem = driver.find_element(By.CSS_SELECTOR, selector)
                price = price_elem.text.strip()
                if price:
                    print(f"Price found: {price}")
                    break
            except:
                continue

        # Description
        description = ""
        desc_selectors = [
            "div[data-test='item-details-description']",
            "[data-test='product-description']",
            ".product-description",
            ".description",
            "#description"
        ]
        for selector in desc_selectors:
            try:
                desc_elem = driver.find_element(By.CSS_SELECTOR, selector)
                description = desc_elem.text.strip()
                if description:
                    break
            except:
                continue

        # Extract ALL image URLs
        print("  Extracting all product images...")
        image_urls = extract_all_image_urls(driver)
        
        # Download all images to product-specific folder
        image_filenames = []
        if image_urls:
            print(f"  Downloading {len(image_urls)} images...")
            image_filenames = download_all_product_images(
                image_urls, 
                IMAGES_FOLDER, 
                current_id, 
                title
            )

        # Generate random rating data
        rating, comment_count = generate_random_rating_data()

        # Create product data
        product_data = {
            "metadata": {
                "source": "target.com",
                "category": "halloween",
                "subcategory": "decorations",
                "crawl_time": time.strftime('%Y-%m-%d %H:%M:%S'),
                "currency": "USD",
                "product_image_urls": image_urls  # Lưu tất cả URL ảnh vào metadata
            },
            "id": current_id,  # Sử dụng ID tiếp tục
            "title": title,
            "price": price,
            "description": description,
            "url": link,
            "image_urls": image_urls,  # Danh sách tất cả URL ảnh
            "image_filenames": image_filenames,  # Danh sách tên file ảnh đã tải
            "rating": rating,
            "review_count": comment_count
        }
        
        # Send to Kafka
        try:
            producer.send(TOPIC, product_data)
            producer.flush()
            print(f" Sent to Kafka: {title[:50]}...")
        except Exception as e:
            print(f" Kafka send failed: {str(e)}")
        
        new_products.append(product_data)
        print(f" Completed product {current_id}: {title[:50]}...")
        print(f"  Downloaded {len(image_filenames)} images to folder: {current_id:03d}_{safe_filename(title)[:40]}")
        
        # Tăng ID cho sản phẩm tiếp theo
        current_id += 1

    except Exception as e:
        print(f" Error processing product {current_id}: {str(e)}")
        current_id += 1  # Vẫn tăng ID ngay cả khi có lỗi
        continue

    # Small delay between requests
    time.sleep(random.uniform(2, 4))

# ==== Combine old and new data ====
all_products = existing_products + new_products

# ==== Save results ====
print(f"\n[INFO] Saving {len(all_products)} total products to JSON...")
print(f" Sản phẩm cũ: {len(existing_products)}")
print(f" Sản phẩm mới: {len(new_products)}")

with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(all_products, f, ensure_ascii=False, indent=2)

print(f"[SUCCESS] Saved {len(all_products)} products -> {OUTPUT_JSON}")

# Count downloaded images
total_images = sum(len(p.get('image_filenames', [])) for p in new_products)
print(f"[INFO] Downloaded {total_images} total images to {IMAGES_FOLDER}")

# Print summary
if new_products:
    print("\n NEW PRODUCTS SUMMARY:")
    for product in new_products[:5]:
        images_count = len(product.get('image_filenames', []))
        print(f"ID {product['id']}: {product.get('title', 'N/A')[:60]}... | {product.get('price', 'N/A')} | {images_count} images")

producer.close()
driver.quit()
print("[INFO] Crawling completed!")