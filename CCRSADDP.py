import polars as pl

# -----------------------------------------------
# Part 0: Read input datasets (like SAS INFILE)
# -----------------------------------------------
aown = pl.read_parquet("ADDRAOWN.FB.parquet")
deposit = pl.read_parquet("DAILY.ADDRACC.parquet")

# -----------------------------------------------
# Part 1: Filter AOWN dataset
# Equivalent to DATA AOWN and IF conditions in SAS
# -----------------------------------------------
aown = (
    aown.filter(
        (pl.col("O_APPL_CODE").is_in(["DP   "])) &
        (pl.col("ACCTNO") > "010000000000")
    )
    .unique(subset=["ACCTNO"])  # PROC SORT NODUPKEY BY ACCTNO
)

print("AOWN (first 5 rows):")
print(aown.head(5))

# -----------------------------------------------
# Part 2: Filter DPADDR dataset
# Equivalent to DATA DPADDR in SAS
# -----------------------------------------------
dpaddr = (
    deposit.filter(pl.col("ACCTNO") > "010000000000")
    .unique(subset=["ACCTNO"])  # PROC SORT NODUPKEY BY ACCTNO
)

print("DEPOSIT ADDRESS (first 5 rows):")
print(dpaddr.head(5))

# -----------------------------------------------
# Part 3: Merge datasets on ACCTNO (inner join)
# Equivalent to DATA MERGE with IF A AND B
# -----------------------------------------------
merged = dpaddr.join(aown, on="ACCTNO", how="inner")

# Optional: sort by ACCTNO
merged = merged.sort("ACCTNO")

print("MERGED (first 5 rows):")
print(merged.head(5))

# -----------------------------------------------
# Part 4: Write output to fixed-width file
# Equivalent to DATA OUT and FILE OUTFILE in SAS
# -----------------------------------------------
def write_fixed_width(df, output_file):
    with open(output_file, "w") as f:
        for row in df.iter_rows(named=True):
            line = (
                f"{row['O_APPL_CODE']:<5}"
                f"{row['ACCTNO']:<20}"
                f"{row['NA_LINE_TYP1']:<1}"
                f"{row['ADD_NAME_1']:<40}"
                f"{row['NA_LINE_TYP2']:<1}"
                f"{row['ADD_NAME_2']:<40}"
                f"{row['NA_LINE_TYP3']:<1}"
                f"{row['ADD_NAME_3']:<40}"
                f"{row['NA_LINE_TYP4']:<1}"
                f"{row['ADD_NAME_4']:<40}"
                f"{row['NA_LINE_TYP5']:<1}"
                f"{row['ADD_NAME_5']:<40}"
                f"{row['NA_LINE_TYP6']:<1}"
                f"{row['ADD_NAME_6']:<40}"
                f"{row['NA_LINE_TYP7']:<1}"
                f"{row['ADD_NAME_7']:<40}"
                f"{row['NA_LINE_TYP8']:<1}"
                f"{row['ADD_NAME_8']:<40}"
            )
            f.write(line + "\n")

write_fixed_width(merged, "DAILY.ADDRACC.out")

# -----------------------------------------------
# Part 5: Print merged dataset again (like SAS PROC PRINT)
# -----------------------------------------------
print("MERGED (first 5 rows, after writing):")
print(merged.head(5))
