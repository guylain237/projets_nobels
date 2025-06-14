from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time

# Configuration du navigateur
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 10)

def get_job_links(page_number):
    url = f"https://www.welcometothejungle.com/fr/jobs?query=data%20scientist&page={page_number}"
    driver.get(url)
    time.sleep(3)

    offers = driver.find_elements(By.XPATH, "//a[contains(@href,'/fr/companies') and contains(@href,'/jobs')]")
    links = []
    seen = set()
    for offer in offers:
        href = offer.get_attribute("href")
        if href and href not in seen:
            seen.add(href)
            links.append(href.split("?")[0])
    return links

def get_job_details(url):
    driver.get(url)
    try:
        # Attente explicite du titre visible dans la page
        title_elem = wait.until(EC.presence_of_element_located((By.XPATH, "//h1")))
        title = title_elem.text.strip()
    except:
        title = "N/A"

    try:
        company_elem = wait.until(EC.presence_of_element_located((By.XPATH, "//div[@data-testid='company-name']")))
        company = company_elem.text.strip()
    except:
        company = "N/A"

    try:
        location_elem = wait.until(EC.presence_of_element_located((By.XPATH, "//div[@data-testid='job-location']")))
        location = location_elem.text.strip()
    except:
        location = "N/A"

    return {
        "title": title,
        "company": company,
        "location": location,
        "url": url
    }

if __name__ == "__main__":
    all_jobs = []
    for page in range(1, 2):
        print(f"Scraping listing page {page}...")
        links = get_job_links(page)
        print(f" → {len(links)} liens trouvés")

        for i, link in enumerate(links):
            print(f"   [{i+1}/{len(links)}] {link}")
            job = get_job_details(link)
            all_jobs.append(job)

    driver.quit()
    df = pd.DataFrame(all_jobs)
    df.to_csv("data/raw/welcome_jungle/wttj_offres.csv", index=False, encoding="utf-8")
    print(f"\n✅ {len(df)} offres enregistrées dans 'data/raw/welcome_jungle/wttj_offres.csv'")
