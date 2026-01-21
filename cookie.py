import pickle
import base64
import os

# Disable SSL verification for driver download (fixes common hang issues)
os.environ['WDM_SSL_VERIFY'] = '0'

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

COOKIES_FILE = "fb_cookies.pkl"

def main():
    # 1. Generate cookies if they don't exist
    if not os.path.exists(COOKIES_FILE):
        print(f"'{COOKIES_FILE}' not found. Launching browser for manual login...")
        
        opts = Options()
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-notifications")
        
        try:
            print("[INFO] Initializing Chrome Driver...")
            service = Service(ChromeDriverManager().install())
            print("[INFO] Launching Chrome Browser...")
            driver = webdriver.Chrome(service=service, options=opts)
            
            driver.get("https://www.facebook.com")
            
            print("\n" + "!"*60)
            print("ACTION REQUIRED: Log in to Facebook in the opened browser.")
            print("Ensure you can see your home feed.")
            print("!"*60 + "\n")
            
            input("Press ENTER here once you have logged in successfully...")
            
            cookies = driver.get_cookies()
            with open(COOKIES_FILE, "wb") as f:
                pickle.dump(cookies, f)
            
            print(f"[SUCCESS] Cookies saved to {COOKIES_FILE}")
            driver.quit()
            
        except Exception as e:
            print(f"[ERROR] Failed to launch browser: {e}")
            input("Press ENTER to exit...")
            return

    # 2. Read and encode cookies
    try:
        with open(COOKIES_FILE, "rb") as f:
            cookies_data = f.read()
            b64_str = base64.b64encode(cookies_data).decode('utf-8')
            print("\n" + "="*20 + " FB_COOKIES_BASE64 " + "="*20)
            print(b64_str)
            print("="*60)
            print("Copy the string above and paste it into Render Environment Variables.")
    except Exception as e:
        print(f"[ERROR] Failed to read cookies: {e}")

if __name__ == "__main__":
    main()
