"""Take a full-page screenshot of the running dashboard (uses the locally installed Google Chrome).
usage: python scripts/screenshot.py [url] [out.png] [wait_seconds] [window_label]"""
import sys
from playwright.sync_api import sync_playwright

url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8501"
out = sys.argv[2] if len(sys.argv) > 2 else "images/dashboard.png"
wait = float(sys.argv[3]) if len(sys.argv) > 3 else 12
label = sys.argv[4] if len(sys.argv) > 4 else None
with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome", headless=True)
    page = browser.new_page(viewport={"width": 1500, "height": 2900}, device_scale_factor=1.0)
    page.goto(url, wait_until="networkidle")
    page.wait_for_selector("text=Edits / sec", timeout=60000)
    if label:
        page.get_by_text(label, exact=True).click()
    page.wait_for_timeout(wait * 1000)
    page.screenshot(path=out, full_page=True)
    browser.close()
print("saved", out)
