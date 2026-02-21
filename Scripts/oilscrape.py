#first open the webpage
#then use send keys to send the appropriate api and then the appropriate wellnamne
#click the button
#click the link
#then extract like normal
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import random
options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=options)

#----pseudo code until pdf is ready----
#pull from the duck db
conn = duckdb.connect('Data/mydb.duckdb')
#load into a dataframe
results = conn.execute("SELECT * FROM OIL_DATA").df()

results = #select out the appropriate fields for api and well name


for index, row in df.iterrows():
        updated_info = []
        updated_info.append(row["name"])
        updated_info.append(row["api"])
        well_location = row["name"].replace(" ","+")
        api = row["api"]
        url = f"https://www.drillingedge.com/search?type=wells&operator_name=&well_name={well_location}&api_no={api}&lease_key=&state=&county=&section=&township=&range=&min_boe=&max_boe=&min_depth=&max_depth=&field_formation="

        driver.get(url)
        link = driver.find_elements(By.XPATH,f"//a[contains(@href, '{api}')]")
        link.click()
        production = driver.find_elements(By.CSS_SELECTOR, "p.block_stat")

        for p in production:
                value = p.text
                if "Oil" in value:
                        oil_count = value.find_elements(By.CSS_SELECTOR, "span.dropcap").text
                if "Gas" in value:
                        gas_count = value.find_elements(By.CSS_SELECTOR, "span.dropcap").text
        well_status = driver.find_elements(By.XPATH, "//th[text()='Well Status']/following-sibling::td").text

        well_type = driver.find_elements(By.XPATH, "//th[text()='Well Type']/following-sibling::td").text
        closest_city = driver.find_elements(By.XPATH, "//th[text()='Closest City']/following-sibling::td").text
        updated_info.extend(oil_count,gas_count,well_status,well_type,closest_city)

