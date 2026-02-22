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

# Part 1

## Setup
### System Dependencies:
```
sudo apt-get install tesseract-ocr poppler-utils
```

### Python Dependencies:

```
pip install -r requirements.txt
```


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



