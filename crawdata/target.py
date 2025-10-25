import os
import time
import json
import requests
from urllib.parse import urlparse, urljoin
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- CHANGED: topic and output paths ---
TOPIC = "target-topic"
OUTPUT_JSON = os.path.join("output", f"{TOPIC}_products.json")
IMAGES_FOLDER = os.path.join("output", "images", TOPIC)
os.makedirs(IMAGES_FOLDER, exist_ok=True)
os.makedirs("output", exist_ok=True)

# ==== Helpers ====
def wait_for_products(driver, css_selector, min_count=5, timeout=40, scroll_pause=1.0):
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
    safe = "".join(c for c in (s or "") if c.isalnum() or c in (' ', '-', '_')).rstrip()
    return safe[:maxlen].replace(" ", "_") or "product"

def download_image(img_url, folder_path, filename_base):
    try:
        if not img_url or not img_url.startswith(("http://","https://")):
            return None
        os.makedirs(folder_path, exist_ok=True)
        headers = {'User-Agent': 'Mozilla/5.0'}
        resp = requests.get(img_url, headers=headers, timeout=20, stream=True)
        resp.raise_for_status()
        # extension
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
        filename = f"{filename_base}{ext}"
        path = os.path.join(folder_path, filename)
        with open(path, "wb") as f:
            for chunk in resp.iter_content(1024*8):
                if chunk:
                    f.write(chunk)
        return filename
    except Exception as e:
        print(f"[WARN] download_image failed: {e}")
        return None

# ==== WebDriver config ====
driverpath = r"D:\chromedriver-win64\chromedriver.exe"
service = Service(driverpath)
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--window-size=1920,1080")
# options.add_argument("--headless")  # remove headless while debugging
driver = webdriver.Chrome(service=service, options=options)
driver.set_page_load_timeout(60)
driver.implicitly_wait(5)
wait = WebDriverWait(driver, 20)

# ==== Kafka producer ====
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# ==== Start crawling (adjust list_url and selectors per site) ====
# ==== URL and selectors for Target.com ====
list_url = "https://www.target.com/c/women-s-clothing/-/N-5xtc3"
product_tile_selector = "[data-test='product-card'], .styles__StyledProductCard"

# ==== Adjust wait times for Target's dynamic loading ====
driver.set_page_load_timeout(90)  # Target can be slow to load
driver.implicitly_wait(10)

print(f"[INFO] Loading Target.com: {list_url}")
driver.get(list_url)
time.sleep(5)  # Initial wait for JS

# Wait for product grid
tiles = wait_for_products(
    driver, 
    product_tile_selector,
    min_count=10,
    timeout=60,
    scroll_pause=2.0  # Slower scroll for Target's infinite loading
)

# Product page selectors
selectors = {
    "title": "[data-test='product-title'], .Heading__StyledHeading",
    "price": "[data-test='product-price'], .style__PriceStandardStyles",
    "description": "[data-test='product-description'], .h-margin-b-default",
    "image": "[data-test='product-image'] img, .BackgroundImage"
}

product_links = []
for el in tiles:
    try:
        href = el.get_attribute("href")
        if href and href.startswith("http"):
            product_links.append(href)
    except:
        continue

product_links = list(dict.fromkeys(product_links))
print(f"[INFO] Found {len(product_links)} product links")

products = []
max_items = min(30, len(product_links))
for i, link in enumerate(product_links[:max_items], 1):
    try:
        print(f"[INFO] Processing ({i}/{max_items}): {link}")
        driver.get(link)
        # wait for a main element likely containing data
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")), timeout=15)
        except:
            pass
        time.sleep(1)  # let JS render

        # Extract title/price/description/image -- adjust selectors per real site
        try:
            title_el = driver.find_element(By.CSS_SELECTOR, "h1, .product-title, .title")
            title = title_el.text.strip()
        except:
            title = f"product_{i:03d}"

        try:
            price_el = driver.find_element(By.CSS_SELECTOR, ".price, .product-price, .price-final")
            price = price_el.text.strip()
        except:
            price = ""

        try:
            desc_el = driver.find_element(By.CSS_SELECTOR, ".description, .product-description, #description")
            description = desc_el.text.strip()
        except:
            description = ""

        # image
        img_url = ""
        try:
            img_el = driver.find_element(By.CSS_SELECTOR, "img[src], img[data-src]")
            img_url = img_el.get_attribute("src") or img_el.get_attribute("data-src") or ""
            if img_url and img_url.startswith("//"):
                img_url = "https:" + img_url
            if img_url and not img_url.startswith("http"):
                img_url = urljoin(link, img_url)
        except:
            img_url = ""

        safe_title = safe_filename(title)[:40]
        filename = None
        if img_url:
            filename = download_image(img_url, IMAGES_FOLDER, f"{i:03d}_{safe_title}")

        product_data = {
            "id": i,
            "title": title,
            "price": price,
            "description": description,
            "url": link,
            "image_url": img_url,
            "image_filename": filename
        }

        # send to Kafka
        try:
            producer.send(TOPIC, product_data)
            producer.flush()
        except Exception as e:
            print(f"[WARN] Kafka send failed: {e}")

        products.append(product_data)
        print(f"[INFO] Done {i}: {title}")

    except Exception as e:
        print(f"[ERROR] item {i} failed: {e}")
        continue

# save output per topic
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    json.dump(products, f, ensure_ascii=False, indent=2)

print(f"[INFO] Saved {len(products)} products -> {OUTPUT_JSON}")

producer.close()
driver.quit()