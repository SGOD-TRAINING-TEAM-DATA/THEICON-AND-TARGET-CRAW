import threading
import subprocess
import time
import sys
import os

def run_consumer():
    print("[INFO] Starting consumer...")
    # dùng sys.executable để đảm bảo dùng cùng Python interpreter
    subprocess.Popen([sys.executable, "kafka_consumer.py"])

def run_producers():
    scripts = ["theicon.py", "target.py"]
    threads = []
    for s in scripts:
        # tránh closure lấy sai biến s
        def runner(script=s):
            subprocess.run([sys.executable, script])
        t = threading.Thread(target=runner)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print("[INFO] All producers finished.")

if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)
    # Chạy consumer (background)
    t1 = threading.Thread(target=run_consumer, daemon=True)
    t1.start()

    # Chạy producer song song
    t2 = threading.Thread(target=run_producers)
    t2.start()

    # Đợi producers hoàn thành
    t2.join()
    print("[INFO] Producers done. Consumer continues to run (press Ctrl+C to stop).")