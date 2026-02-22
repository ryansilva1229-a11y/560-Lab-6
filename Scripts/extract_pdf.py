import re, os, json
import fitz
from PIL import Image
import pytesseract
import pandas as pd
import duckdb

pdf_folder = "Data/MOCK_DSCI560_Lab5"
db_file="Data/mydb.duckdb"
csv_file="Data/mydb.csv"

api_pat = re.compile(r"\b\d{2}\s*-\s*\d{3}\s*-\s*\d{5}\b") # ND api nums look like 33-025-12345

fields = ["source_pdf", "api_number", "well_name", "operator", "job_number", "job_type",
    "county", "state", "shl", "latitude", "longitude", "datum",
    "well_status", "well_type", "closest_city", "oil_bbl", "gas_mcf"]


def get_text(pdf_path):
    txts = []
    with fitz.open(pdf_path) as doc:
        for p in doc:
            n = p.get_text() or ""
            if len(n.strip()) >= 50:
                txts.append(n)
            else:
                try: # some pdfs are scanned so fallback to ocr
                    pix = p.get_pixmap(matrix=fitz.Matrix(2.0, 2.0), alpha=False)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    txts.append(pytesseract.image_to_string(img))
                except:
                    txts.append(n)
    ft = "\n".join(txts).replace("\xa0", " ").replace("\xad", "-")
    return re.sub(r"[ \t]+", " ", ft).strip()

def parse_well(txt):
    info={}
    api_m = api_pat.findall(txt)
    info["api_number"] = re.sub(r"\s*", "", api_m[0]) if api_m else ""

    m = (re.search(r"Well\s+or\s+Facility\s+Name\s*:\s*(.+)", txt, re.IGNORECASE) or
        re.search(r"Well\s+Name\s+and\s+Number\s*\n+(.+)", txt, re.IGNORECASE) or
        re.search(r"Well\s+Name\s*:\s*(.+)", txt, re.IGNORECASE))
    info["well_name"] = m.group(1).strip() if m else ""

    # operator field is messy - sometimes has email/url junk after it
    m = (re.search(r"Well\s+Operator\s*:\s*(.+)", txt, re.IGNORECASE) or
        re.search(r"^Operator\s{2,}(.+?)(?:\s{3,}|\t|\n)", txt, re.IGNORECASE | re.MULTILINE))
    op_raw = m.group(1) if m else ""
    op_raw = re.sub(r"\S+@\S+|https?://\S+|\b20\d{2}\b|[●•·]", "", op_raw)
    op_raw=re.sub(r"\s+Rig\s*:.*|\s+Operator\s*:.*", "", op_raw, flags=re.IGNORECASE)
    info["operator"] = re.sub(r"\s+", " ", op_raw).strip()

    m=re.search(r"Job\s*(?:Number|No\.?|#)\s*[:\-]?\s*(\d+)", txt, re.IGNORECASE)
    info["job_number"] = m.group(1).strip() if m else ""

    # try the labeled field first, then just scan for known job type keywords
    m = (re.search(r"Type\s+of\s+(?:Job|Work|Treatment)\s*:\s*(.+)", txt, re.IGNORECASE) or
        re.search(r"Job\s+Type\s*:\s*(.+)", txt, re.IGNORECASE))
    job_type=m.group(1).strip() if m else ""
    if not job_type:
        for kw in ["Stimulation", "Workover", "Completion", "Recompletion",
                   "Plug and Abandon", "Salt Water Disposal", "Injection"]:
            if re.search(rf"\b{re.escape(kw)}\b", txt, re.IGNORECASE):
                job_type=kw; break
    info["job_type"] = job_type or "N/A"

    m = (re.search(r"County\s*:\s*([A-Za-z][A-Za-z\s\-]{1,30})(?:\s*\n|\s{2,}|$)", txt, re.IGNORECASE) or
        re.search(r"\b([A-Za-z]{3,20})\s+County\b", txt, re.IGNORECASE))
    info["county"]=m.group(1).strip().title() if m else ""
    info["state"] = "ND" # all mock data is north dakota

    shl_m = re.search(r"(\d{3,5}\s*F[\s\n]+[NS][\s\n]*L[\s\n]+\d{3,5}\s*F[\s\n]+[EW][\s\n]*L(?:[\s\n]+[NSEW]{2,6})?)", txt, re.IGNORECASE)
    info["shl"] = re.sub(r"\s+", " ", shl_m.group(1)).strip() if shl_m else ""

    lat_m = (re.search(r"Latitude\s*:\s*(\d+[°\s]+\d+['\s]+\d+\.?\d*\s*[\"']?\s*N)", txt, re.IGNORECASE) or
             re.search(r'(\d{2}[°\s]+\d+\'\s*\d+\.?\d*"?\s*[NS])', txt))
    lon_m = (re.search(r"Longitude\s*:\s*(\d+[°\s]+\d+['\s]+\d+\.?\d*\s*[\"']?\s*W)", txt, re.IGNORECASE) or
             re.search(r'(\d{3}[°\s]+\d+\'\s*\d+\.?\d*"?\s*[EW])', txt))

    for key, match in [("latitude", lat_m), ("longitude", lon_m)]:
        fallback_pat = r"Lat(?:itude)?\s*[=:]?\s*(\d{2,3}\.\d{4,})" if key=="latitude" else r"Lon(?:gitude|g)?\s*[=:]?\s*(-?\d{2,3}\.\d{4,})"
        if match:
            # dms -> decimal e.g. 47 30 12 N -> 47.503333
            dm=re.match(r"(\d+)[degrees\s]+(\d+)['\s]+(\d+\.?\d*)[\"'\s]*([NSEW])", match.group(1).strip(), re.IGNORECASE)
            if dm:
                dd = int(dm.group(1)) + int(dm.group(2))/60 + float(dm.group(3))/3600
                info[key] = f"{(-dd if dm.group(4).upper() in 'SW' else dd):.6f}"
            else:
                info[key]=match.group(1).strip()
        else:
            fb=re.search(fallback_pat, txt, re.IGNORECASE)
            info[key] = fb.group(1).strip() if fb else ""

    m = (re.search(r"Geo\s+Datum\s*:\s*([^\n\r]{2,60})", txt, re.IGNORECASE) or
        re.search(r"Datum\s*:\s*([^\n\r]{2,60})", txt, re.IGNORECASE))
    info["datum"]=m.group(1).strip() if m else ""
    info.update({"well_status": "", "well_type": "", "closest_city": "", "oil_bbl": 0.0, "gas_mcf": 0.0})
    return info


def parse_stims(txt):
    stages = []
    txt=re.sub(r"\s+", " ", txt)
    for m in re.finditer(r"\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b", txt):
        snip = txt[m.end():m.end() + 400]
        form_m=re.search(r"\b([A-Za-z\-]{3,30})\b", snip)
        nums = [re.sub(r"[^\d.]", "", n) for n in re.findall(r"\d[\d_~\-]{2,6}", snip)
                if re.sub(r"[^\d.]", "", n)]
        top=bottom=stages_num=vol=""
        if len(nums) >= 4:
            top, bottom, stages_num, vol = nums[:4]
        units = re.search(r"\b(Gallons?|BBL|Barrels?|MCF|Lbs?)\b", snip, re.IGNORECASE)
        treat=re.search(r"\b(Acid|Fracture|Frac|Injection)\b", snip, re.IGNORECASE)
        acid = re.search(r"\b(\d{1,2})\s*%?\b", snip)
        def to_float(v):
            try: return float(str(v).replace(",", "").strip())
            except: return None
        stages.append({
            "date_stimulated": m.group(),
            "stimulated_formation": form_m.group(1) if form_m else "",
            "top_ft": to_float(top), "bottom_ft": to_float(bottom),
            "stimulation_stages": to_float(stages_num),
            "volume": to_float(vol),
            "volume_units": units.group(1) if units else "",
            "type_treatment": treat.group(1) if treat else "",
            "acid_pct": to_float(acid.group(1)) if acid else None,
            "lbs_proppant": None, "max_treatment_pressure": None,
            "max_treatment_rate": None, "details": "",
        })
    return stages


def run_folder(pdf_folder, db_file, csv_file):
    pdfs=sorted([os.path.join(pdf_folder, f) for f in os.listdir(pdf_folder) if f.endswith(".pdf")])
    if not pdfs:
        print("no pdfs found, check the folder path")
        return
    rows = []
    for pdf in pdfs:
        try:
            txt=get_text(pdf)
        except Exception as e:
            print(f"couldn't read {os.path.basename(pdf)}: {e}")
            continue
        if not txt:
            print(f"empty text for {os.path.basename(pdf)}, skipping")
            continue
        info = parse_well(txt)
        stims=parse_stims(txt)
        info["source_pdf"] = os.path.basename(pdf)
        # clean strings and fill blanks so df doesnt have NaNs everywhere
        info = {k: (v.strip() if isinstance(v, str) else v) for k, v in info.items()}
        for nf in ["oil_bbl", "gas_mcf"]:
            try: info[nf]=float(info.get(nf, 0))
            except: info[nf] = 0.0
        for f in fields:
            if f not in info or info[f] in ["", None]:
                info[f]="N/A"
        stims_clean = []
        for s in stims:
            s = {k: (v.strip() if isinstance(v, str) else v) for k, v in s.items()}
            for nf in ["top_ft", "bottom_ft", "stimulation_stages", "volume",
                       "acid_pct", "lbs_proppant", "max_treatment_pressure", "max_treatment_rate"]:
                try: s[nf]=float(str(s.get(nf, "")).replace(",", "").strip())
                except: s[nf] = None
            stims_clean.append(s)
        info["stimulation"]=stims_clean
        rows.append(info)
        print(f"\n{os.path.basename(pdf)}")
        for k, v in info.items():
            if k != "stimulation":
                print(f"  {k}: {v}")
        print(f"  stimulation stages found: {len(info['stimulation'])}")
        print("-" * 50)

    df = pd.DataFrame(rows)
    df["stimulation"]=df["stimulation"].apply(json.dumps)
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    conn=duckdb.connect(db_file)
    conn.execute("DROP TABLE IF EXISTS OIL_DATA")
    conn.execute("CREATE TABLE OIL_DATA AS SELECT * FROM df")
    conn.close()
    df.to_csv(csv_file, index=False)
    print(f"Saved to {db_file}, {csv_file}")


if __name__ == "__main__":
    run_folder(pdf_folder, db_file, csv_file)