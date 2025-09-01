import polars as pl
from datetime import datetime

# ==========================================================
# PROCESSING LEFT SIDE
# ==========================================================

# CCCODES (LEFT SIDE)
cccde = pl.read_parquet("cccde.parquet")
cccdes_left = (
    cccde
    .select([
        pl.col("TYPE"),
        pl.col("CODE1"),
        pl.col("DESC1")
    ])
    .unique(subset=["CODE1"])
)

# CISCUST (LEFT SIDE)
custfile = pl.read_parquet("custfile.parquet")
ciscust_left = (
    custfile
    .select([
        pl.col("CUSTNO"),
        pl.col("OLDIC1"),
        pl.col("BASICGRPCODE1")
    ])
    .unique(subset=["CUSTNO"])
)

# CISNAME (LEFT SIDE)
namefile = pl.read_parquet("namefile.parquet")
cisname_left = (
    namefile
    .select([
        pl.col("CUSTNO"),
        pl.col("INDORG1"),
        pl.col("CUSTNAME1")
    ])
    .unique(subset=["CUSTNO"])
)

# CISALIAS (LEFT SIDE)
aliasfile = pl.read_parquet("aliasfile.parquet")
cisalias_left = aliasfile.select(["CUSTNO", "ALIAS1"])

# INFILE1
infile1 = pl.read_parquet("infile1.parquet")
ccrlen1 = (
    infile1
    .with_columns([
        pl.col("EXPDATE1"),
        pl.col("EXPDATE").str.strptime(pl.Date, "%Y-%m-%d", strict=False)
    ])
    # IF EXPDATE1 NE '    ' THEN DELETE;
    .filter(pl.col("EXPDATE1").str.strip_chars().is_null() | (pl.col("EXPDATE1").str.strip_chars() == ""))
)

# Merge sequence LEFT
idx_l01 = ccrlen1.join(cccdes_left, on="CODE1", how="left")
idx_l02 = idx_l01.join(cisname_left, on="CUSTNO", how="left")
idx_l03 = idx_l02.join(cisalias_left, on="CUSTNO", how="left")
idx_l04 = idx_l03.join(ciscust_left, on="CUSTNO", how="left")

leftout = idx_l04
# leftout.write_parquet("leftout.parquet")


# ==========================================================
# PROCESSING RIGHT SIDE
# ==========================================================

# CCCODES (RIGHT SIDE)
cccdes_right = (
    cccde
    .select([
        pl.col("TYPE"),
        pl.col("CODE2"),
        pl.col("DESC2")
    ])
    .unique(subset=["CODE2"])
)

# CISCUST (RIGHT SIDE)
ciscust_right = (
    custfile
    .select([
        pl.col("CUSTNO"),
        pl.col("OLDIC2"),
        pl.col("BASICGRPCODE2")
    ])
    .unique(subset=["CUSTNO"])
)

# CISNAME (RIGHT SIDE)
cisname_right = (
    namefile
    .select([
        pl.col("CUSTNO"),
        pl.col("INDORG2"),
        pl.col("CUSTNAME2")
    ])
    .unique(subset=["CUSTNO"])
)

# CISALIAS (RIGHT SIDE)
cisalias_right = aliasfile.select(["CUSTNO", "ALIAS2"])

# INFILE2
infile2 = pl.read_parquet("infile2.parquet")
ccrlen2 = (
    infile2
    .with_columns([
        pl.when(
            (pl.col("EXPMM").is_not_null()) &
            (pl.col("EXPDD").is_not_null()) &
            (pl.col("EXPYY").is_not_null())
        ).then(
            pl.date(pl.col("EXPYY"), pl.col("EXPMM"), pl.col("EXPDD"))
        ).otherwise(None).alias("EXPDATE")
    ])
)

# Merge sequence RIGHT
idx_r01 = ccrlen2.join(cccdes_right, on="CODE2", how="left")
idx_r02 = idx_r01.join(cisname_right, on="CUSTNO", how="left")
idx_r03 = idx_r02.join(cisalias_right, on="CUSTNO", how="left")
idx_r04 = idx_r03.join(ciscust_right, on="CUSTNO", how="left")

rightout = idx_r04
# rightout.write_parquet("rightout.parquet")


# ==========================================================
# FILE TO SEARCH ONE SIDE ONLY
# ==========================================================

# Control Date
datefile = pl.read_parquet("datefile.parquet")
today_date = datetime(
    int(datefile["SRSYY"][0]),
    int(datefile["SRSMM"][0]),
    int(datefile["SRSDD"][0])
)

# INPUT1 (filter expired)
input1 = (
    leftout
    .filter((pl.col("EXPDATE").is_null()) | (pl.col("EXPDATE") >= today_date))
)

# INPUT2 (filter expired)
input2 = (
    rightout
    .filter((pl.col("EXPDATE").is_null()) | (pl.col("EXPDATE") >= today_date))
)

# Final Output
tempo = pl.concat([input1, input2])

# Write to output file
tempo.write_parquet("outfileall.parquet")
