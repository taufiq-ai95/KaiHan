import polars as pl

# -----------------------------
# 0. READ INPUT FILES (TOP OF SCRIPT)
# -----------------------------
# Assuming parquet versions of ADDRFILE and AELEFILE exist
addr = pl.read_parquet("ADDRFILE.parquet")
aele = pl.read_parquet("AELEFILE.parquet")

# -----------------------------
# 1. FORMAT & INPUT MAPPING (ADDR)
# -----------------------------
# In SAS, ADDREF is numeric, LINExADR are strings
addr = addr.with_columns([
    pl.col("ADDREF1").alias("ADDREF"),
    pl.col("LINE1ADR").fill_null(""),
    pl.col("LINE2ADR").fill_null(""),
    pl.col("LINE3ADR").fill_null(""),
    pl.col("LINE4ADR").fill_null(""),
    pl.col("LINE5ADR").fill_null("")
])

# Sort by ADDREF
addr = addr.sort("ADDREF")
print(addr.head(5))

# -----------------------------
# 2. FORMAT & INPUT MAPPING (AELE)
# -----------------------------
aele = aele.with_columns([
    pl.col("ADDREF1").alias("ADDREF"),
    pl.col("STREET").fill_null(""),
    pl.col("CITY").fill_null(""),
    pl.col("ZIP").fill_null(""),
    pl.col("ZIP2").fill_null(""),
    pl.col("COUNTRY").fill_null("")
])

aele = aele.sort("ADDREF")
print(aele.head(5))

# -----------------------------
# 3. MERGE ADDR + AELE
# -----------------------------
addr_aele = addr.join(aele, on="ADDREF", how="inner")

# Concatenate address lines
addr_aele = addr_aele.with_columns(
    (pl.col("LINE1ADR") + pl.col("LINE2ADR") + pl.col("LINE3ADR") + 
     pl.col("LINE4ADR") + pl.col("LINE5ADR")).alias("ADDRLINE")
)

# Filter out certain countries
exclude_countries = [
    'SINGAPORE ','CANADA    ','SINGAPORE`','LONDON    ','AUS       ','AUSTRIA   ',
    'BAHRAIN   ','BANGLADESH','BRUNEI DAR','CAMBODIA  ','CAN       ','CAYMAN ISL',
    'CHINA     ','BRUNEI    ','INDONESIA ','DARUSSALAM','DENMARK   ','EMIRATES  ',
    'ENGLAND   ','EUROPEAN  ','FRANCE    ','GERMANY   ','HONG KONG ','INDIA     ',
    'IRAN (ISLA','IRELAND   ','JAPAN     ','KOREA REPU','MACAU     ','MAURITIUS ',
    'MEXICO    ','MYANMAR   ','NEPAL     ','NETHERLAND','NEW ZEALAN','NEWZEALAND',
    'NIGERIA   ','NORWAY    ','OMAN      ','PAKISTAN  ','PANAMA    ','PHILIPPINE',
    'ROC       ','S ARABIA  ','SAMOA     ','SAUDI ARAB','SIGAPORE  ','SIMGAPORE ',
    'SINGAPOREW','SINGPAORE ','SINGPORE  ','SINAGPORE ','SNGAPORE  ','SINGOPORE ',
    'SPAIN     ','SRI LANKA ','SWAZILAND ','SWEDEN    ','SWITZERLAN','TAIWAN    ',
    'TAIWAN,PRO','THAILAND  ','U KINGDOM ','U.K.      ','UNITED ARA','UK        ',
    'UNITED KIN','UNITED STA','VIRGIN ISL','USA       ','PAPUA NEW ','AUSTRALIA '
]

addr_aele = addr_aele.filter(~pl.col("COUNTRY").is_in(exclude_countries))

# -----------------------------
# 4. CHECK ADDR LINES FOR ZIP/CITY/COUNTRY
# -----------------------------
def extract_malaysia_address(df: pl.DataFrame) -> pl.DataFrame:
    for line in ["LINE2ADR","LINE3ADR","LINE4ADR","LINE5ADR"]:
        mask = (
            (pl.col(line).str.slice(0,5).cast(pl.Int32) > 1) &
            (pl.col(line).str.slice(0,5).cast(pl.Int32) < 99998) &
            (pl.col(line).str.slice(5,1) == " ")
        )
        df = df.with_columns([
            pl.when(mask).then(pl.col(line).str.slice(0,5)).otherwise(pl.col("NEW_ZIP")).alias("NEW_ZIP"),
            pl.when(mask).then(pl.col(line).str.slice(6,25)).otherwise(pl.col("NEW_CITY")).alias("NEW_CITY"),
            pl.when(mask).then(pl.lit("MALAYSIA")).otherwise(pl.col("NEW_COUNTRY")).alias("NEW_COUNTRY")
        ])
    return df

addr_aele = extract_malaysia_address(addr_aele)
print(addr_aele.head(5))

# -----------------------------
# 5. REMOVE FOREIGN ADDRESSES
# -----------------------------
exclude_words = ['SINGAPORE','HONG HONG','QATAR','TAMIL NADU','STAFFORDSHIRE','HANOI',
                 'VIETNAM','NEW ZEALAND','ENGLAND','AUCKLAND','SHANGHAI','DOHA QATAR',
                 'THAILAND','HONG KONG','SEOUL','#','NSW','NETHERLANDS','AUSTRALIA',"S'PORE"]

addr_aele = addr_aele.filter(~pl.col("ADDRLINE").str.contains("|".join(exclude_words)))

# -----------------------------
# 6. FILL STATECODE BASED ON ZIP
# -----------------------------
def assign_state(zip_code: pl.Expr) -> pl.Expr:
    return pl.when((zip_code >= 79000) & (zip_code <= 86999), "JOH")\
             .when((zip_code >= 5000) & (zip_code <= 9999), "KED")\
             .when((zip_code >= 15000) & (zip_code <= 18999), "KEL")\
             .when((zip_code >= 75000) & (zip_code <= 78999), "MEL")\
             .when((zip_code >= 70000) & (zip_code <= 73999), "NEG")\
             .when(((zip_code >= 25000) & (zip_code <= 28999)) | (zip_code == 69000), "PAH")\
             .when((zip_code >= 10000) & (zip_code <= 14999), "PEN")\
             .when(((zip_code >= 30000) & (zip_code <= 36999)) | ((zip_code >= 39000) & (zip_code <= 39999)), "PRK")\
             .when((zip_code >= 1000) & (zip_code <= 2999), "PER")\
             .when((zip_code >= 88000) & (zip_code <= 91999), "SAB")\
             .when((zip_code >= 93000) & (zip_code <= 98999), "SAR")\
             .when(((zip_code >= 40000) & (zip_code <= 49999)) | ((zip_code >= 63000) & (zip_code <= 64999)) | ((zip_code >= 68000) & (zip_code <= 68199)), "SEL")\
             .when((zip_code >= 20000) & (zip_code <= 24999), "TER")\
             .when((zip_code >= 50000) & (zip_code <= 60999), "W P")\
             .when((zip_code >= 87000) & (zip_code <= 87999), "LAB")\
             .when((zip_code >= 62000) & (zip_code <= 62999), "PUT")\
             .otherwise(pl.lit(""))

addr_aele = addr_aele.with_columns([
    pl.when((pl.col("STATEX").is_null()) | (pl.col("STATEX") == "N/A"), assign_state(pl.col("NEW_ZIP").cast(pl.Int32)))
    .otherwise(pl.col("STATEX")).alias("STATEX")
])

# -----------------------------
# 7. OUTPUT FILE (VERIFY)
# -----------------------------
def write_outfile(df: pl.DataFrame, file_path: str):
    with open(file_path, "w") as f:
        # Header
        f.write(f"{'CIS #':<11}-{'ADDR REF':<12}{'ADDLINE1':<41}{'ADDLINE2':<41}{'ADDLINE3':<41}"
                f"{'ADDLINE4':<41}{'ADDLINE5':<41} {'ZIP':<5}{'CITY':<25}{'COUNTRY':<10}"
                f"{'ZIP':<5}{'CITY':<25}{'COUNTRY':<10}\n")
        for row in df.iter_rows(named=True):
            if row["NEW_ZIP"].strip() == "":
                continue
            f.write(f"{row['CUSTNO']:<11}-{row['ADDREF']:011}{row['LINE1ADR']:<40}{row['LINE2ADR']:<40}"
                    f"{row['LINE3ADR']:<40}{row['LINE4ADR']:<40}{row['LINE5ADR']:<40}*OLD*"
                    f"{row['ZIP']:<5}{row['CITY']:<25}{row['COUNTRY']:<10}*NEW*"
                    f"{row['NEW_ZIP']:<5}{row['NEW_CITY']:<25}{row['STATEX']:<3}{row['NEW_COUNTRY']:<10}\n")

write_outfile(addr_aele, "CCRSADR4.VERIFY.txt")

# -----------------------------
# 8. UPDATE FILE
# -----------------------------
def write_updfile(df: pl.DataFrame, file_path: str):
    with open(file_path, "w") as f:
        for row in df.iter_rows(named=True):
            if row["NEW_ZIP"].strip() == "":
                continue
            f.write(f"{row['CUSTNO']:<11}{row['ADDREF']:011}{row['NEW_CITY'].upper():<25}"
                    f"{row['STATEX']:<3}{row['NEW_ZIP']:<5}{row['NEW_COUNTRY']:<10}\n")

write_updfile(addr_aele, "CCRSADR4.UPDATE.txt")
