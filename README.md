# DSCI-560 Lab 6

**Team Members:** Elise Hadidi (1137648541), Jordan Davies (1857892197), Ryan Silva (6463166471) 
**Team Number:**  17

## Overview
This project collects, extracts, and stores oil well data from PDF reports and drillingedge.com into a DuckDB database for analysis. It also creates an interactive map to view this extracted data. 

## Folder Structure
```
560-Lab-6/
│
├── Data/
│   ├── DSCI560_Lab5/         
│   ├── mydb.duckdb
│   └── mydb.csv
│
├── Scripts/
│   ├── connect_DB.py
│   ├── extract_pdf.py
│   ├── oilscraper.py
│   ├── index.html
│   ├── main.js
│   ├── package-lock.json
│   ├── style.css
│   ├── vite.config.js
│   └── duckdb_testing.py
│
├── requirements.txt
└── README.md
```

## Setup
### System Dependencies:
```
sudo apt-get install tesseract-ocr poppler-utils
```

### Python Dependencies:

```
pip install -r requirements.txt
```

## How It Works
Step 1 - Extract from PDFs:
PDF extraction is done by running ```extract_pdf.py```. Text is extracted using two libraries (pdfplumber and pymupdf) with OCR fallback. The following fields are extracted from each PDF:
- source_pdf
- api_number
- well_name
- operator
- job_number
- job_type
- county
- state
- shl
- latitude
- longitude
- datum
- well_status (default until Step 2)
- well_type (default until Step 2)
- closest_city (default until Step 2)
- oil_bbl (default until Step 2)
- gas_mcf (default until Step 2)
- stimulation (list of date_stimulated, stimulated_formation, top_ft, bottom_ft, stimulation_stages, volume, volume_units, type_treatment, acid_pct, lbs_proppant, max_treatment_pressure, and max_treatment_rate, details)

Results are saved to the DuckDB database (OIL_DATA table) and a CSV file.

Step 2 - Scrape drillingedge.com for additional well fields:
Additional well data is scraped from drillingedge.com. The search URL is constructed for each well and used to query to website. 

Step 3 - Frontend map:
The relevant data is pulled from DuckDB and is served to the frontend via Flask. Coordinates are formatted at GeoJSON to assist with marker placement. Each well is displayed on the map with a red circle marker and clicking the marker opens a scrollable popup showing the well's data, including a section for stimulation records. 

## Running the Pipeline
Step 1 – Extract from PDFs: 
```
python3 Scripts/extract_pdf.py
```

Step 2 – Scrape drillingedge.com for additional well fields:
```
python3 Scripts/oilscrape.py
```

Step 3 - Serve flask
```
python3 connectDB.py
```

Step 4 - Build and run the frontend
```
sudo apachectl start
npm install
npm run build
sudo cp -r dist/* /Library/WebServer/Documents/
```

Step 5 - Visit http://localhost



