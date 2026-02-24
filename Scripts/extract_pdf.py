# imports
import re, os, json
import fitz
import pdfplumber
from PIL import Image
import pytesseract
import pandas as pd
import duckdb

# to make things quicker
from concurrent.futures import ProcessPoolExecutor, as_completed

# paths
#pdf_folder = 'Data/MOCK_DSCI560_Lab5'
pdf_folder = 'Data/DEMO_DSCI560_Lab5'
#db_file    = "Data/mydb.duckdb"
db_file    = "Data/DEMO_mydb.duckdb"
#csv_file   = "Data/mydb.csv"
csv_file   = "Data/DEMO_mydb.csv"

# regex patterns
api_pat = re.compile(r"\b\d{2}\s*-\s*\d{3}\s*-\s*\d{5}\b")
STIM_KEYWORDS = re.compile(r"\b(stimulat|frac|acid|inject|treatment|formation)\b", re.IGNORECASE)

# fields to extract
fields = [
    "source_pdf", "api_number", "well_name", "operator", "job_number", "job_type",
    "county", "state", "shl", "latitude", "longitude", "datum",
    "well_status", "well_type", "closest_city", "oil_bbl", "gas_mcf"
]


def get_text(pdf_path):
    txts = []

    # text extraction with pdfplumber
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    txts.append(t)
    except:
        pass

    # text extraction with fitz + OCR fallback
    with fitz.open(pdf_path) as doc:
        for p in doc:
            n = p.get_text() or ""

            # if less than 200 characters, do OCR
            if len(n.strip()) >= 200:
                txts.append(n)
            else:
                try:
                    pix = p.get_pixmap(matrix=fitz.Matrix(2.0, 2.0), alpha=False)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    txts.append(pytesseract.image_to_string(img))
                except:
                    txts.append(n)

    # clean and combine text
    ft = "\n".join(txts).replace("\xa0", " ").replace("\xad", "-")
    return re.sub(r"[ \t]+", " ", ft).strip()
#well parsing

def parse_well(txt):
    info = {}

    # api number
    # format too
    api_m = api_pat.findall(txt)
    info["api_number"] = re.sub(r"\s*", "", api_m[0]) if api_m else ""
    print(f"  api_number: {info['api_number']}")

    # well name
    # three different patterns 
    m = (re.search(r"Well\s+or\s+Facility\s+Name\s*:\s*(.+)", txt, re.IGNORECASE) or
         re.search(r"Well\s+Name\s+and\s+Number\s*\n\s*(.+)", txt, re.IGNORECASE) or
         re.search(r"Well\s+Name\s*:\s*(.+)", txt, re.IGNORECASE))
    info["well_name"] = m.group(1).strip() if m else ""
    print(f"  well_name: {info['well_name']}")

    # operation

    # lots of different patterns
    patterns = [
        r"(?:Company|Operator)[^\n]*Telephone Number[^\n]*\n([^\n]+)",
        r"(?:Company|Operator)\s*\n([^\n]+)\n",
        r"Operator\s*:\s*(.+?)(?:\s{2,}|\s*Telephone)",
        r"Well\s+Operator\s*:\s*(.+)",
    ]

    # search pattenrs 
    op_raw = ""
    for pat in patterns:
        m = re.search(pat, txt, re.IGNORECASE)
        if m:
            op_raw = m.group(1).strip()
            break

    # clean
    op_raw = re.sub(r"\S+@\S+|https?://\S+|\b20\d{2}\b|[●•·]", "", op_raw)
    op_raw = re.sub(r"\s+(?:Rig|Operator|Telephone|Phone|Tel|Fax)\s*:.*", "", op_raw, flags=re.IGNORECASE)
    op_raw = re.sub(r"\s+\(?\d{3}\)?[\s\-]\d{3}[\s\-]\d{4}.*", "", op_raw)
    op_raw = re.sub(r"\s+FOR\s+STATE.*", "", op_raw, flags=re.IGNORECASE)
    op_clean = re.sub(r"\s+", " ", op_raw).strip()
    info["operator"] = op_clean if len(op_clean) >= 3 and re.search(r"[A-Za-z]{2,}", op_clean) else ""
    print(f"  operator: {info['operator']}")

    # job number
    m = re.search(r"Job\s*(?:Number|No\.?|#)\s*[:\-]?\s*(\d+)", txt, re.IGNORECASE)
    info["job_number"] = m.group(1).strip() if m else ""
    print(f"  job_number: {info['job_number']}")

    # job type
    # checkboxes
    checked = re.findall(
        r"(?:☑|☒|✓|✔|\[x\]|\[X\])\s*([A-Za-z][^\n☐☑\[\]]{2,50})",
        txt, re.IGNORECASE
    )
    if checked:
        info["job_type"] = ", ".join(checked)
    else:
        # other
        m = re.search(r"\bOther\s+([A-Za-z][^\n]{2,80})", txt, re.IGNORECASE)
        info["job_type"] = m.group(1).strip() if m else "N/A"
    print(f"  job_type: {info['job_type']}")

    # county
    m = (re.search(r"County\s*:\s*([A-Za-z][A-Za-z\s\-]{1,30})(?:\s*\n|\s{2,}|$)", txt, re.IGNORECASE) or
         re.search(r"\b([A-Za-z]{3,20})\s+County\b", txt, re.IGNORECASE))
    info["county"] = m.group(1).strip().title() if m else ""
    print(f"  county: {info['county']}")

    # state
    # look up API prefix
    api_prefix = info["api_number"].split("-")[0] if info["api_number"] else ""

    # from online
    api__map = {"05": "CO", "30": "MT", "33": "ND", "38": "WY", "49": "UT"}
    info["state"] = api__map.get(api_prefix, "")
    print(f"  state: {info['state']}")

    # shl
    # looking for patterns lik: 149 F NL 257 F WL
    m = re.search(
        r"(\d{3,5}\s*F[\s\n]+[NS][\s\n]*L[\s\n]+\d{3,5}\s*F[\s\n]+[EW][\s\n]*L(?:[\s\n]+[NSEW]{2,6})?)",
        txt, re.IGNORECASE
    )
    info["shl"] = re.sub(r"\s+", " ", m.group(1)).strip() if m else ""
    print(f"  shl: {info['shl']}")

    # latitude
    lat_m = (re.search(r"Latitude\s*:\s*(\d+[°\s]+\d+['\s]+\d+\.?\d*\s*[\"']?\s*N)", txt, re.IGNORECASE) or
             
             # looks for coordinates
             re.search(r'(\d{2}[°\s]+\d+\'\s*\d+\.?\d*"?\s*[NS])', txt))
    if lat_m:
        dm = re.match(r"(\d+)[°\s]+(\d+)['\s]+(\d+\.?\d*)[\"'\s]*([NSEW])", lat_m.group(1).strip(), re.IGNORECASE)
        if dm:
            # convert to decimal degrees
            dd = int(dm.group(1)) + int(dm.group(2)) / 60 + float(dm.group(3)) / 3600
            info["latitude"] = f"{(-dd if dm.group(4).upper() in 'SW' else dd):.6f}"
        else:
            info["latitude"] = lat_m.group(1)
    else:
        fb = re.search(r"Lat(?:itude)?\s*[=:]?\s*(\d{2,3}\.\d{4,})", txt, re.IGNORECASE)
        info["latitude"] = fb.group(1).strip() if fb else ""
    print(f"  latitude: {info['latitude']}")

    # longitude
    lon_m = (re.search(r"Longitude\s*:\s*(\d+[°\s]+\d+['\s]+\d+\.?\d*\s*[\"']?\s*W)", txt, re.IGNORECASE) or
             # looks for coordinates
             re.search(r'(\d{3}[°\s]+\d+\'\s*\d+\.?\d*"?\s*[EW])', txt))
    if lon_m:
        dm = re.match(r"(\d+)[°\s]+(\d+)['\s]+(\d+\.?\d*)[\"'\s]*([NSEW])", lon_m.group(1).strip(), re.IGNORECASE)
       
        if dm:
            # convert to decimal degrees
            dd = int(dm.group(1)) + int(dm.group(2)) / 60 + float(dm.group(3)) / 3600
            info["longitude"] = f"{(-dd if dm.group(4).upper() in 'SW' else dd):.6f}"
        else:
            info["longitude"] = lon_m.group(1)
    else:

        # if already in decimal format, just grab it
        fb = re.search(r"Lon(?:gitude|g)?\s*[=:]?\s*(-?\d{2,3}\.\d{4,})", txt, re.IGNORECASE)
        info["longitude"] = fb.group(1).strip() if fb else ""
    print(f"  longitude: {info['longitude']}")

    # datum
    # three different patterns
    m = (re.search(r"Geo\s+Datum\s*:\s*([^\n\r]{2,60})", txt, re.IGNORECASE) or
         re.search(r"System\s+Datum\s*:\s*([^\n\r]{2,60})", txt, re.IGNORECASE) or
         re.search(r"Datum\s*:\s*([^\n\r]{2,60})", txt, re.IGNORECASE))
    info["datum"] = m.group(1).strip() if m else ""
    print(f"  datum: {info['datum']}")

    # placeholders — populated later
    info["well_status"]  = ""
    info["well_type"]    = ""
    info["closest_city"] = ""
    info["oil_bbl"]      = 0.0
    info["gas_mcf"]      = 0.0

    return info


#stimulation parsing
def parse_stims(txt):
    treatments = []
    txt = re.sub(r"\s+", " ", txt)

    # finding dates
    for m in re.finditer(r"\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b", txt):

        # seeing if the words are around
        context = txt[max(0, m.start()-200):m.end()+200]
        if not STIM_KEYWORDS.search(context):
            continue

        # takes a 400 character snippet after the date and looks for patterns
        snip = txt[m.end():m.end() + 400]
        form_m = re.search(r"\b([A-Za-z\-]{3,30})\b", snip)
        nums = [re.sub(r"[^\d.]", "", n) for n in re.findall(r"\d[\d_~\-]{2,6}", snip)
                if re.sub(r"[^\d.]", "", n)]
        top = bottom = stages_num = vol = ""
        if len(nums) >= 4:
            top, bottom, stages_num, vol = nums[:4]

        # looking for patterns
        units = re.search(r"\b(Gallons?|BBL|Barrels?|MCF|Lbs?)\b", snip, re.IGNORECASE)
        treat = re.search(r"\b(Acid|Fracture|Frac|Injection)\b", snip, re.IGNORECASE)
        acid = re.search(r"\b(\d{1,2})\s*%?\b", snip)
        proppant = re.search(r"Lbs\s*Proppant\s*\n?\s*(\d[\d,]+)", snip, re.IGNORECASE)
        pressure = re.search(r"Maximum\s*Treatment\s*Pressure[^\n]*\n?\s*(\d[\d,]+)", snip, re.IGNORECASE)
        rate = re.search(r"Maximum\s*Treatment\s*Rate[^\n]*\n?\s*(\d+\.?\d*)", snip, re.IGNORECASE)


        treatments.append({
            "date_stimulated": m.group(),
            "stimulated_formation": form_m.group(1) if form_m else "",
            "top_ft": float(str(top).replace(",", "").strip()) if top else None,
            "bottom_ft": float(str(bottom).replace(",", "").strip()) if bottom else None,
            "stimulation_stages": float(str(stages_num).replace(",", "").strip()) if stages_num else None,
            "volume": float(str(vol).replace(",", "").strip()) if vol else None,
            "volume_units": units.group(1) if units else "",
            "type_treatment": treat.group(1) if treat else "",
            "acid_pct": float(acid.group(1)) if acid else None,
            "lbs_proppant": float(proppant.group(1).replace(",", "")) if proppant else None,
            "max_treatment_pressure": float(pressure.group(1).replace(",", "")) if pressure else None,
            "max_treatment_rate": float(rate.group(1)) if rate else None,
            "details": "",
        })
    return treatments


# processing pdf
import html

# processing pdf
def process_pdf(pdf):
    try:
        txt = get_text(pdf)
    except:
        return None
    if not txt:
        return None

    # parsing well and stimulation data
    info = parse_well(txt)
    stims = parse_stims(txt)
    info["source_pdf"] = os.path.basename(pdf)

    # clean strings — strip whitespace, remove HTML tags and special characters
    def clean_str(v):
        if not isinstance(v, str):
            return v
        v = html.unescape(v)
        v = re.sub(r"<[^>]+>", "", v)         
        v = re.sub(r"[^\x20-\x7E]", "", v)     
        v = re.sub(r"\s+", " ", v).strip()
        return v

    info = {k: clean_str(v) for k, v in info.items()}

    # numeric fields default to 0
    for nf in ["oil_bbl", "gas_mcf"]:
        try: info[nf] = float(info.get(nf, 0))
        except: info[nf] = 0.0

    # fill missing fields with N/A
    for f in fields:
        if f not in info or info[f] in ["", None]:
            info[f] = "N/A"

    # clean stimulation records
    stims_clean = []
    for s in stims:
        s = {k: clean_str(v) for k, v in s.items()}
        for nf in ["top_ft", "bottom_ft", "stimulation_stages", "volume",
                   "acid_pct", "lbs_proppant", "max_treatment_pressure", "max_treatment_rate"]:
            try: s[nf] = float(str(s.get(nf, "")).replace(",", "").strip())
            except: s[nf] = 0.0 
        stims_clean.append(s)
    info["stimulation"] = stims_clean

    # for debugging
    print("-" * 60)
    return info


def process_pdfs(pdf_folder, db_file, csv_file):

    pdfs = sorted([
        os.path.join(pdf_folder, f)
        for f in os.listdir(pdf_folder) if f.endswith(".pdf")
    ])
    if not pdfs:
        return

    # setting up multiprocessing  
    max_workers = max(1, (os.cpu_count() or 4) - 2)

    rows = []

    #parallel processing of PDFs
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_pdf, pdf): pdf for pdf in pdfs}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                rows.append(result)

    if not rows:
        return

    rows.sort(key=lambda r: r["source_pdf"])

    # data frame creation
    df = pd.DataFrame(rows)
    df["stimulation"] = df["stimulation"].apply(json.dumps)

    # save to duckdb and csv
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    conn = duckdb.connect(db_file)
    conn.execute("DROP TABLE IF EXISTS OIL_DATA")
    conn.execute("CREATE TABLE OIL_DATA AS SELECT * FROM df")
    conn.execute("ALTER TABLE OIL_DATA ADD PRIMARY KEY (api_number)")
    conn.close()

    df.to_csv(csv_file, index=False)


if __name__ == "__main__":
    process_pdfs(pdf_folder, db_file, csv_file)