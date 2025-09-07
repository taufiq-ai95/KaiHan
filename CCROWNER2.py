import polars as pl

# ======================================================
# Read Input Parquet Files (all upfront)
# ======================================================
ccrfile1 = pl.read_parquet("DP.parquet")
ccrfile2 = pl.read_parquet("CISDEMO_SAFD.parquet")
ccrfile3 = pl.read_parquet("CISDEMO_LN.parquet")
rlencc_in = pl.read_parquet("RLENCC_input.parquet")

# ======================================================
# Part 1 - GET CA RELATIONSHIP
# ======================================================
ccrfile = pl.concat([ccrfile1, ccrfile2, ccrfile3])

cis = (
    ccrfile
    .with_columns([
        pl.when(pl.col("ACCTNOR").is_in(["01","03","04","05","06","07"]))
          .then("DP")
          .when(pl.col("ACCTNOR").is_in(["02","08"]))
          .then("LN")
          .otherwise(None)
          .alias("ACCTCODE")
    ])
    .filter(pl.col("PRISEC") == "901")
    .sort("ACCTNOC")
)

print("=== Part 1 Output (first 5 rows) ===")
print(cis.head(5))

# ======================================================
# Part 2 - FLIP CC RELATIONSHIP
# ======================================================
flipped = (
    rlencc_in
    .select([
        pl.col("CUST2").alias("CUST1"),
        pl.col("CODE2").alias("CODE1"),
        pl.col("CUST1").alias("CUST2"),
        pl.col("CODE1").alias("CODE2"),
    ])
)

print("=== Part 2 Output (first 5 rows) ===")
print(flipped.head(5))

# ======================================================
# Part 3 - MATCH RECORD WITH CC RELATIONSHIP
# ======================================================
merge1 = (
    cis.join(flipped, left_on="CUSTNO", right_on="CUST1", how="inner")
    .select([
        "ACCTCODE","ACCTNOC","CUSTNO","RLENCODE",
        "CODE1","CUST2","CODE2"
    ])
    .sort(["ACCTNOC","CUSTNO","CUST2"])
)

print("=== Part 3 Output (first 5 rows) ===")
print(merge1.head(5))

# ======================================================
# Part 4 - REMOVE DUPLICATES
# ======================================================
final = merge1.unique()

final.write_parquet("CISOWNER.parquet")

print("=== Part 4 Output (first 5 rows) ===")
print(final.head(5))
print("✅ Final output written to CISOWNER.parquet")
