#first open the webpage

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
import random
import duckdb
options = Options()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=options)


#pull from the duck db
conn = duckdb.connect('Data/mydb.duckdb')
#load into a dataframe
oil_df = conn.execute("SELECT * FROM OIL_DATA").df()
new_fields = []
for index, row in oil_df.iterrows():

    #well_location = row["name"].replace(" ","+")
    api = row["api_number"]
    #searching with api only because it is guaranteed to be unique and name could potentially be incorrect
    url = f"https://www.drillingedge.com/search?type=wells&operator_name=&well_name=&api_no={api}&lease_key=&state=&county=&section=&township=&range=&min_boe=&max_boe=&min_depth=&max_depth=&field_formation="

    driver.get(url)
    link = driver.find_elements(By.XPATH,f"//a[contains(@href, '{api}')]")
    for l in link:
          next_url = l.get_attribute("href")
    driver.get(next_url)
    production = driver.find_elements(By.CSS_SELECTOR, "p.block_stat")

    for p in production:
            value = p.text
            #print(value)
            if "Oil" in value:
                    oil_df.loc[index,"oil_bbl"] = value.split("\n")[0]
                    #print(row["oil_bbl"])
                    
            if "Gas" in value:
                    oil_df.loc[index,"gas_mcf"]= value.split("\n")[0]
                    #print(row["gas_mcf"])
                    
    oil_df.loc[index,"well_status"] = driver.find_element(By.XPATH, "//th[text()='Well Status']/following-sibling::td").text
    #print(row["well_status"])

    oil_df.loc[index,"well_type"] = driver.find_element(By.XPATH, "//th[text()='Well Type']/following-sibling::td").text
    oil_df.loc[index,"closest_city"] = driver.find_element(By.XPATH, "//th[text()='Closest City']/following-sibling::td").text
    #redundant updating for available values in case the pdf scrape got them incorrectly
    if row["latitude"] == "N/A":
          oil_df.loc[index,"latitude"] = driver.find_element(By.XPATH, "//th[text()='Latitude / Longitude']/following-sibling::td").text.split(", ")[0]
    if row["longitude"] == "N/A":
          oil_df.loc[index,"longitude"] = driver.find_element(By.XPATH, "//th[text()='Latitude / Longitude']/following-sibling::td").text.split(", ")[1]
    if row["operator"] == "N/A":
          oil_df.loc[index,"operator"] = driver.find_element(By.XPATH, "//th[text()='Operator']/following-sibling::td").text
    if row["county"] == "N/A":
          oil_df.loc[index,"county"] = driver.find_element(By.XPATH, "//th[text()='County']/following-sibling::td").text.split(", ")[0]
    if row["state"] == "N/A":
          oil_df.loc[index,"state"] = driver.find_element(By.XPATH, "//th[text()='County']/following-sibling::td").text.split(", ")[1]


#assuming table name is oild ata or something
# no longer need since fields are already established
'''
conn.execute("ALTER TABLE OIL_DATA ADD COLUMN oil_count INT")
conn.execute("ALTER TABLE OIL_DATA ADD COLUMN gas_count INT")
conn.execute("ALTER TABLE OIL_DATA ADD COLUMN well_status TEXT")
conn.execute("ALTER TABLE OIL_DATA ADD COLUMN well_type TEXT")
conn.execute("ALTER TABLE OIL_DATA ADD COLUMN closest_city TEXT")
'''
oil_df.to_csv("Data/test.csv")
try:
    conn.register("tmp_df", oil_df) 
    conn.execute("""
        INSERT INTO OIL_DATA (well_name, api_number, operator, 
                 county, state, latitude, longitude, well_status, well_type,
                 closest_city, oil_bbl, gas_mcf)
        SELECT well_name, api_number, operator, 
                 county, state, latitude, longitude, well_status, well_type,
                 closest_city, oil_bbl, gas_mcf FROM tmp_df
        ON CONFLICT (api_number)
        DO UPDATE SET
            
            api_number = EXCLUDED.api_number,
            well_name = EXCLUDED.well_name,
            operator = EXCLUDED.operator,
            county = EXCLUDED.county,
            state = EXCLUDED.state,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            well_status = EXCLUDED.well_status,
            well_type = EXCLUDED.well_type,
            closest_city = EXCLUDED.closest_city,
            oil_bbl = EXCLUDED.oil_bbl,
            gas_mcf = EXCLUDED.gas_mcf;

      
    """)
    conn.unregister("tmp_df")
except Exception as e:
    print("Warning: insertion failed, error = ", e)


