from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pandas as pd
import os

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

creds = Credentials.from_service_account_file(
    "service_account.json",
    scopes=SCOPES
)
gc = gspread.authorize(creds)

CONTROL_SHEET_ID = "1lKAvIcCLBtwv5zKnCwdqP0xgBpUCjEeTewSwLuglDNU"

EXCLUSION_TABS = [
    "Active Titan/SPIRE",
    "Ex-Membership",
    "BGC",
    "New Intake"
]

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

def build_exclusion_sets():
    exclude_ic, exclude_email, exclude_phone = set(), set(), set()
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

    return exclude_ic, exclude_email, exclude_phone


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
# Download route (optional)
# ----------------------------
@app.get("/download/{filename}")
def download_file(filename: str):
    path = os.path.join(os.getcwd(), filename)
    if os.path.exists(path):
        return FileResponse(path, filename=filename)
    return {"status": "ERROR", "message": "File not found"}

@app.post("/clean")
def clean_form(
    file: UploadFile = File(...),
    sheet_name: str = Form(None),
    country_code: str = Form("60")
):
    # --- Load Excel ---
    try:
        if sheet_name:
            df = pd.read_excel(file.file, sheet_name=sheet_name)
        else:
            df = pd.read_excel(file.file)
    except Exception as e:
        return {"status": "ERROR", "message": f"Failed to read Excel: {e}"}

    # --- Column detection ---
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

    # --- Build exclusion sets ---
    try:
        exclude_ic, exclude_email, exclude_phone = build_exclusion_sets()
    except Exception as e:
        return {"status": "ERROR", "message": f"Failed to access control sheet: {e}"}

    # --- Normalize input ---
    if ic_col:
        df["IC_norm"] = df[ic_col].apply(normalize_ic)
    if email_col:
        df["Email_norm"] = df[email_col].apply(normalize_email)
    if phone_col:
        df["Phone_norm"] = df[phone_col].apply(normalize_phone)

    # --- Duplicates & Exclusions ---
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

    # --- Split cleaned / excluded ---
    cleaned_df = df[df["FinalStatus"] == ""].copy()
    excluded_df = df[df["FinalStatus"] != ""].copy()

    # --- Format phone ---
    if phone_col and phone_col in cleaned_df.columns:
        cleaned_df[phone_col] = cleaned_df[phone_col].apply(lambda x: format_phone(x, country_code))

    # --- Utilities for clean matching ---
    def clean_str(s):
        return str(s or "").replace("\u00A0", " ").strip().lower()

    sh_control = gc.open_by_key(CONTROL_SHEET_ID)

    # --- Load control lists ---
    def load_control_list(sheet_name, col_ranges=None):
        try:
            ws = sh_control.worksheet(sheet_name)
            rows = ws.get_all_values()
            lst = []
            for r_idx, row in enumerate(rows):
                for c_idx, cell in enumerate(row):
                    if cell:
                        cell_val = clean_str(cell)
                        if col_ranges:
                            if r_idx in col_ranges.get('rows', range(len(rows))) and \
                               c_idx in col_ranges.get('cols', range(len(row))):
                                lst.append(cell_val)
                        else:
                            lst.append(cell_val)
            return lst
        except Exception:
            return []

    titan_list = load_control_list("Active Titan/SPIRE", {"rows": range(0,5), "cols": range(0,5)})
    spire_list = load_control_list("Active Titan/SPIRE", {"rows": range(7,12), "cols": range(7,12)})
    new_intake_list = load_control_list("New Intake")
    bgc_list = load_control_list("BGC")
    exmem_list = load_control_list("Ex-Membership")

    # --- Tagging functions ---
    def tag_titan_spire(value):
        if pd.isna(value):
            return ""
        s = clean_str(value)
        if s in titan_list:
            return "Active Titan"
        elif s in spire_list:
            return "Active SPIRE"
        return ""

    def tag_lookup(value, lookup_list, tag_name):
        if pd.isna(value):
            return ""
        return tag_name if clean_str(value) in lookup_list else ""

    # --- Apply tagging row-wise ---
    tag_columns = ["Active Titan/SPIRE", "New Intake", "BGC", "Ex-Membership"]
    for col in tag_columns:
        excluded_df[col] = ""  # initialize

    def tag_row(row):
        for col_name in tag_columns:
            tag = ""
            # Check IC
            if ic_col and pd.notna(row[ic_col]):
                if col_name == "Active Titan/SPIRE":
                    tag = tag_titan_spire(row[ic_col])
                elif col_name == "New Intake":
                    tag = tag_lookup(row[ic_col], new_intake_list, "Closing")
                elif col_name == "BGC":
                    tag = tag_lookup(row[ic_col], bgc_list, "BGC")
                elif col_name == "Ex-Membership":
                    tag = tag_lookup(row[ic_col], exmem_list, "Ex-Membership")

            # If no tag, check Email
            if not tag and email_col and pd.notna(row[email_col]):
                if col_name == "Active Titan/SPIRE":
                    tag = tag_titan_spire(row[email_col])
                elif col_name == "New Intake":
                    tag = tag_lookup(row[email_col], new_intake_list, "Closing")
                elif col_name == "BGC":
                    tag = tag_lookup(row[email_col], bgc_list, "BGC")
                elif col_name == "Ex-Membership":
                    tag = tag_lookup(row[email_col], exmem_list, "Ex-Membership")

            # If no tag, check Phone
            if not tag and phone_col and pd.notna(row[phone_col]):
                if col_name == "Active Titan/SPIRE":
                    tag = tag_titan_spire(row[phone_col])
                elif col_name == "New Intake":
                    tag = tag_lookup(row[phone_col], new_intake_list, "Closing")
                elif col_name == "BGC":
                    tag = tag_lookup(row[phone_col], bgc_list, "BGC")
                elif col_name == "Ex-Membership":
                    tag = tag_lookup(row[phone_col], exmem_list, "Ex-Membership")
            row[col_name] = tag
        return row

    excluded_df = excluded_df.apply(tag_row, axis=1)

    # --- Handle NaN for JSON & Google Sheets ---
    cleaned_df = cleaned_df.where(pd.notnull(cleaned_df), None)
    excluded_df = excluded_df.where(pd.notnull(excluded_df), None)

    # --- Cleanup ---
    cleaned_df.drop(columns=["IC_norm", "Email_norm", "Phone_norm", "FinalStatus"], errors="ignore", inplace=True)
    excluded_df.drop(columns=["IC_norm", "Email_norm", "Phone_norm"], errors="ignore", inplace=True)

    # --- Create Google Sheet ---
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    sh_master = gc.open_by_key(MASTER_SHEET_ID)

    ws_cleaned = get_or_create_ws(sh_master, "Cleaned", rows=len(cleaned_df)+1)
    ws_excluded = get_or_create_ws(sh_master, "Excluded", rows=len(excluded_df)+1)

    ws_cleaned.clear()
    ws_excluded.clear()

    ws_cleaned.update([cleaned_df.columns.tolist()] + cleaned_df.values.tolist())
    ws_excluded.update([excluded_df.columns.tolist()] + excluded_df.values.tolist())

    sheet_url = sh_master.url

    return {
        "status": "OK",
        "summary": {
            "totalRaw": len(df),
            "totalCleaned": len(cleaned_df),
            "totalExcluded": len(excluded_df)
        },
        "sheet_url": sheet_url
    }

    if __name__ == "__main__":
       import uvicorn
       port = int(os.environ.get("PORT", 8000))
       uvicorn.run("main:app", host="0.0.0.0", port=port)
