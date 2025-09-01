import polars as pl

# -------------------------------------------------------------------
#  Load parquet datasets
#  (Assume you already converted raw files to parquet with the same names)
# -------------------------------------------------------------------
primary = pl.read_parquet("NONJOINT.parquet")   # PRIMREC
ccrlen1 = pl.read_parquet("RLENIND.parquet")   # Individual
ccrlen  = pl.read_parquet("RLENORG.parquet")   # Organisation

# -------------------------------------------------------------------
#  Ensure schema matches SAS layout
#  (Rename / select fields explicitly to align)
# -------------------------------------------------------------------
primary = primary.select([
    pl.col("ACCTNO").cast(pl.Utf8),
    pl.col("ACCTCODE").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8)
])

ccrlen1 = ccrlen1.select([
    pl.col("CUSTNO1").cast(pl.Utf8),
    pl.col("CUSTTYPE1").cast(pl.Utf8),
    pl.col("RLENCODE1").cast(pl.Utf8),
    pl.col("DESC1").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("CUSTTYPE").cast(pl.Utf8),
    pl.col("RLENCODE").cast(pl.Utf8),
    pl.col("DESC").cast(pl.Utf8),
    pl.col("CUSTNAME1").cast(pl.Utf8),
    pl.col("ALIAS1").cast(pl.Utf8),
    pl.col("CUSTNAME").cast(pl.Utf8),
    pl.col("ALIAS").cast(pl.Utf8)
])

ccrlen = ccrlen.select([
    pl.col("CUSTNO1").cast(pl.Utf8),
    pl.col("CUSTTYPE1").cast(pl.Utf8),
    pl.col("RLENCODE1").cast(pl.Utf8),
    pl.col("DESC1").cast(pl.Utf8),
    pl.col("CUSTNO").cast(pl.Utf8),
    pl.col("CUSTTYPE").cast(pl.Utf8),
    pl.col("RLENCODE").cast(pl.Utf8),
    pl.col("DESC").cast(pl.Utf8),
    pl.col("CUSTNAME1").cast(pl.Utf8),
    pl.col("ALIAS1").cast(pl.Utf8),
    pl.col("CUSTNAME").cast(pl.Utf8),
    pl.col("ALIAS").cast(pl.Utf8)
])

# -------------------------------------------------------------------
#  Merge organisation CCRLEN with PRIMARY accounts
#  Equivalent to PROC SQL join CCRLEN, PRIMARY on CUSTNO
# -------------------------------------------------------------------
cc_primary = (
    ccrlen.join(primary, on="CUSTNO", how="inner")
)

# Sort (like PROC SORT)
cc_primary = cc_primary.sort(["CUSTNO", "ACCTCODE", "ACCTNO"])

# -------------------------------------------------------------------
#  Union with CCRLEN1 (individual relationship)
# -------------------------------------------------------------------
out = pl.concat([cc_primary, ccrlen1], how="diagonal")

# -------------------------------------------------------------------
#  Write PARTIES flat file (fixed width)
# -------------------------------------------------------------------
def to_fixed_width(df: pl.DataFrame, filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        for row in df.iter_rows(named=True):
            line = (
                f"{row['CUSTNO1']:<11}"
                f"{row['CUSTTYPE1']:<1}"
                f"{row['RLENCODE1']:<3}"
                f"{row['DESC1']:<15}"
                f"{row['CUSTNO']:<11}"
                f"{row['CUSTTYPE']:<1}"
                f"{row['RLENCODE']:<3}"
                f"{row['DESC']:<15}"
                f"{row.get('ACCTCODE',''):<5}"
                f"{row.get('ACCTNO',''):<20}"
                f"{row['CUSTNAME1']:<40}"
                f"{row['ALIAS1']:<20}"
                f"{row['CUSTNAME']:<40}"
                f"{row['ALIAS']:<20}"
            )
            f.write(line + "\n")

to_fixed_width(out, "PARTIES.txt")

# -------------------------------------------------------------------
#  Write IMIS flat file (CSV style with quotes and \N at end)
# -------------------------------------------------------------------
def to_imis(df: pl.DataFrame, filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        for row in df.iter_rows(named=True):
            line = (
                f"\"{row['CUSTNO1']}\",\"{row['CUSTTYPE1']}\","
                f"\"{row['RLENCODE1']}\",\"{row['DESC1']}\","
                f"\"{row['CUSTNO']}\",\"{row['CUSTTYPE']}\","
                f"\"{row['RLENCODE']}\",\"{row['DESC']}\","
                f"\"{row.get('ACCTCODE','')}\",\"{row.get('ACCTNO','')}\","
                f"\"{row['CUSTNAME1']}\",\"{row['ALIAS1']}\","
                f"\"{row['CUSTNAME']}\",\"{row['ALIAS']}\", \\N"
            )
            f.write(line + "\n")

to_imis(out, "IMIS.txt")
