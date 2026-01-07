from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
import pandas as pd
import os
import json
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ----------------------------
# FastAPI setup
# ----------------------------
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# ----------------------------
# Google Sheets API setup
# ----------------------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
gc = gspread.authorize(creds)

CONTROL_SHEET_ID = "1lKAvIcCLBtwv5zKnCwdqP0xgBpUCjEeTewSwLuglDNU"
EXCLUSION_TABS = ["Active Titan/SPIRE", "Ex-Membership", "BGC", "New Intake"]
SHARED_FOLDER_ID = "1YSwdmvSzfcqTCY3Ud10qmOG9ycRbThDE"
MASTER_SHEET_ID = "1bnlemWPBoZ6wLXJlWEMPS8adB5GrnL9IX-3Zwvkl6Js"

# ----------------------------
# Utilities
# ----------------------------
def normalize_ic(v):
    return "".join(filter(str.isdigit, str(v or "")))

def normalize_email(v):
    return str(v or "").lower().strip()

def normalize_phone(v):
    return "".join(filter(str.isdigit, str(v or ""))).lstrip("0")

def format_phone(v, cc):
    d = normalize_phone(v)
    return cc + d if d and not d.startswith(cc) else d

# 🔽 UPDATED: track IC source tab
def build_exclusion_sets():
    exclude_ic, exclude_email, exclude_phone = set(), set(), set()
    ic_source = {}

    sh = gc.open_by_key(CONTROL_SHEET_ID)
    for tab in EXCLUSION_TABS:
        try:
            ws = sh.worksheet(tab)
            rows = ws.get_all_values()
        except Exception:
            continue

        for row in rows:
            for cell in row:
                if not cell:
                    continue
                s = str(cell).strip()
                if "@" in s:
                    exclude_email.add(s.lower())
                else:
                    digits = normalize_ic(s)
                    if len(digits) >= 6:
                        exclude_ic.add(digits)
                        exclude_phone.add(digits)
                        ic_source[digits] = tab

    return exclude_ic, exclude_email, exclude_phone, ic_source

def get_or_create_ws(sh, title, rows=1000, cols=20):
    try:
        return sh.worksheet(title)
    except:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)

# ----------------------------
# UI
# ----------------------------
@app.get("/", response_class=HTMLResponse)
def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/auto-data-cleaner", response_class=HTMLResponse)
def ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# ----------------------------
# Download route
# ----------------------------
@app.get("/download/{filename}")
def download_file(filename: str):
    path = os.path.join(os.getcwd(), filename)
    if os.path.exists(path):
        return FileResponse(path, filename=filename)
    return {"status": "ERROR", "message": "File not found"}

# ----------------------------
# Clean route
# ----------------------------
@app.post("/clean")
def clean_form(
    file: UploadFile = File(...),
    sheet_name: str = Form(None),
    country_code: str = Form("60")
):
    try:
        if sheet_name:
            df = pd.read_excel(file.file, sheet_name=sheet_name)
        else:
            df = pd.read_excel(file.file)
    except Exception as e:
        return {"status": "ERROR", "message": f"Failed to read Excel: {e}"}

    ic_cols = ["IC", "ICNumber", "IC Number", "Identification Number"]
    email_cols = ["Email", "EmailAddress", "E-mail"]
    phone_cols = ["Mobile", "Phone", "PhoneNumber"]

    def find_column(df, candidates):
        for col in candidates:
            if col in df.columns:
                return col
        return None

    ic_col = find_column(df, ic_cols)
    email_col = find_column(df, email_cols)
    phone_col = find_column(df, phone_cols)

    if not any([ic_col, email_col, phone_col]):
        return {"status": "ERROR", "message": "No IC, Email, or Phone column found"}

    try:
        exclude_ic, exclude_email, exclude_phone, ic_source = build_exclusion_sets()
    except Exception as e:
        return {"status": "ERROR", "message": f"Failed to access control sheet: {e}"}

    if ic_col:
        df["IC_norm"] = df[ic_col].apply(normalize_ic)
    if email_col:
        df["Email_norm"] = df[email_col].apply(normalize_email)
    if phone_col:
        df["Phone_norm"] = df[phone_col].apply(normalize_phone)

    df["FinalStatus"] = ""
    mask_ok = df["FinalStatus"] == ""

    if ic_col:
        df.loc[df["IC_norm"].isin(exclude_ic), "FinalStatus"] = "Excluded - Control List"
        df.loc[mask_ok & df.duplicated("IC_norm"), "FinalStatus"] = "Excluded – Duplicate IC"
    if email_col:
        df.loc[df["Email_norm"].isin(exclude_email), "FinalStatus"] = "Excluded - Control List"
        df.loc[mask_ok & df.duplicated("Email_norm"), "FinalStatus"] = "Excluded – Duplicate Email"
    if phone_col:
        df.loc[df["Phone_norm"].isin(exclude_phone), "FinalStatus"] = "Excluded - Control List"
        df.loc[mask_ok & df.duplicated("Phone_norm"), "FinalStatus"] = "Excluded – Duplicate Phone"

    cleaned_df = df[df["FinalStatus"] == ""].copy()
    excluded_df = df[df["FinalStatus"] != ""].copy()

    if phone_col and phone_col in cleaned_df.columns:
        cleaned_df[phone_col] = cleaned_df[phone_col].apply(lambda x: format_phone(x, country_code))

    # 🔽 ADD 4 EXCLUSION TAG COLUMNS
    excluded_df["Active Membership"] = ""
    excluded_df["BGC"] = ""
    excluded_df["New Intake"] = ""
    excluded_df["Ex Membership"] = ""

    if ic_col:
        for idx, row in excluded_df.iterrows():
            ic = row.get("IC_norm")
            source = ic_source.get(ic)

            if source == "Active Titan/SPIRE":
                excluded_df.at[idx, "Active Membership"] = "Active SPIRE / Active Titan"
            elif source == "BGC":
                excluded_df.at[idx, "BGC"] = "BGC"
            elif source == "New Intake":
                excluded_df.at[idx, "New Intake"] = "Closing"
            elif source == "Ex-Membership":
                excluded_df.at[idx, "Ex Membership"] = "Ex Membership"

    cleaned_df = cleaned_df.where(pd.notnull(cleaned_df), None)
    excluded_df = excluded_df.where(pd.notnull(excluded_df), None)

    cleaned_df.drop(columns=["IC_norm", "Email_norm", "Phone_norm", "FinalStatus"], errors="ignore", inplace=True)
    excluded_df.drop(columns=["IC_norm", "Email_norm", "Phone_norm"], errors="ignore", inplace=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    sh_master = gc.open_by_key(MASTER_SHEET_ID)

    ws_cleaned = get_or_create_ws(sh_master, "Cleaned", rows=len(cleaned_df) + 1)
    ws_excluded = get_or_create_ws(sh_master, "Excluded", rows=len(excluded_df) + 1)

    ws_cleaned.clear()
    ws_excluded.clear()

    ws_cleaned.update([cleaned_df.columns.tolist()] + cleaned_df.values.tolist())
    ws_excluded.update([excluded_df.columns.tolist()] + excluded_df.values.tolist())

    return {
        "status": "OK",
        "summary": {
            "totalRaw": len(df),
            "totalCleaned": len(cleaned_df),
            "totalExcluded": len(excluded_df)
        },
        "sheet_url": sh_master.url
    }

# ----------------------------
# Run locally
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

