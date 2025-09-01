import polars as pl

# -------------------------------------------------------------------
# READ PARQUET DATASETS
# -------------------------------------------------------------------
main_df = pl.read_parquet("CUST.parquet")
indv_df = pl.read_parquet("INDVT.parquet")
org_df  = pl.read_parquet("ORGT.parquet")
cart_df = pl.read_parquet("CART.parquet")

# -------------------------------------------------------------------
# PROCESS MAIN CUSTOMER FILE (INDIVIDUAL PART)
#   - Drop GENDER = 'O'
# -------------------------------------------------------------------
main_indv = (
    main_df
    .filter(pl.col("GENDER") != "O")
    .select([
        "CUSTNO", "BANKNO", "MAIN_ENTITY_TYPE", "BRANCH",
        "CUSTNAME", "BIRTHDATE", "GENDER"
    ])
    .sort("CUSTNO")
)

print("MAIN (INDIVIDUAL)")
print(main_indv.head(5))


# -------------------------------------------------------------------
# PROCESS INDIVIDUAL FILE
# -------------------------------------------------------------------
indv = (
    indv_df
    .select([
        "CUSTNO","IDTYPE","ID","CUSTBRANCH","FIRST_CREATE_DATE",
        "FIRST_CREATE_TIME","FIRST_CREATE_OPER","LAST_UPDATE_DATE",
        "LAST_UPDATE_TIME","LAST_UPDATE_OPER","LONGNAME","ENTITYTYPE",
        "BNM_ASSIGNED_ID","OLDIC","CITIZENSHIP","PRCOUNTRY",
        "RESIDENCY_STATUS","CUSTOMER_CODE","ADDRLINE1","ADDRLINE2",
        "ADDRLINE3","ADDRLINE4","ADDRLINE5","POSTCODE","TOWN_CITY",
        "STATE_CODE","COUNTRY","ADDR_LAST_UPDATE","ADDR_LAST_UPTIME",
        "PHONE_HOME","PHONE_BUSINESS","PHONE_FAX","PHONE_MOBILE",
        "PHONE_PAC","EMPLOYER_NAME","MASCO2008","MASCO2012",
        "EMPLOYMENT_TYPE","EMPLOYMENT_SECTOR","EMPLOYMENT_LAST_UPDATE",
        "EMPLOYMENT_LAST_UPTIME","INCOME_AMT","ENABLE_TAB"
    ])
    .sort(["CUSTNO","IDTYPE","ID"])
)

print("INDIVIDUAL FILE")
print(indv.head(5))


# -------------------------------------------------------------------
# PROCESS CART FILE
# -------------------------------------------------------------------
cart = (
    cart_df
    .select([
        "APPL_CODE","APPL_NO","PRI_SEC","RELATIONSHIP",
        "CUSTNO","IDTYPE","ID","AA_REF_NO","EFF_DATE",
        "EFF_TIME","LAST_MNT_DATE","LAST_MNT_TIME"
    ])
    .sort(["CUSTNO","IDTYPE","ID"])
)

print("CART FILE")
print(cart.head(5))


# -------------------------------------------------------------------
# MERGE INDIVIDUAL + CART
# -------------------------------------------------------------------
custinfo_indv = (
    indv.join(cart, on=["CUSTNO","IDTYPE","ID"], how="left")
)

print("CUSTINFO (INDIVIDUAL)")
print(custinfo_indv.head(5))


# -------------------------------------------------------------------
# MERGE MAIN + CUSTINFO (INDIVIDUAL)
# -------------------------------------------------------------------
indvdly = (
    main_indv.join(custinfo_indv, on="CUSTNO", how="inner")
)

# Create index equivalent (set unique keys)
indvdly = indvdly.unique(subset=["CUSTNO","IDTYPE","ID"])

print("FINAL INDIVIDUAL DATASET")
print(indvdly.head(5))


# -------------------------------------------------------------------
# PROCESS MAIN CUSTOMER FILE (ORGANISATION PART)
#   - Keep only GENDER = 'O'
# -------------------------------------------------------------------
main_org = (
    main_df
    .filter(pl.col("GENDER") == "O")
    .select([
        "CUSTNO", "BANKNO", "MAIN_ENTITY_TYPE", "BRANCH",
        "CUSTNAME", "BIRTHDATE", "GENDER"
    ])
    .sort("CUSTNO")
)

print("MAIN (ORGANISATION)")
print(main_org.head(5))


# -------------------------------------------------------------------
# PROCESS ORG FILE
# -------------------------------------------------------------------
org = (
    org_df
    .select([
        "CUSTNO","IDTYPE","ID","BRANCH","FIRST_CREATE_DATE",
        "FIRST_CREATE_TIME","FIRST_CREATE_OPER","LAST_UPDATE_DATE",
        "LAST_UPDATE_TIME","LAST_UPDATE_OPER","LONG_NAME","ENTITY_TYPE",
        "BNM_ASSIGNED_ID","REGISTRATION_DATE","MSIC2008","RESIDENCY_STATUS",
        "CORPORATE_STATUS","CUSTOMER_CODE","CITIZENSHIP","ADDR_LINE_1",
        "ADDR_LINE_2","ADDR_LINE_3","ADDR_LINE_4","ADDR_LINE_5","POSTCODE",
        "TOWN_CITY","STATE_CODE","COUNTRY","ADDR_LAST_UPDATE","ADDR_LAST_UPTIME",
        "PHONE_PRIMARY","PHONE_SECONDARY","PHONE_FAX","PHONE_MOBILE",
        "PHONE_PAC","ENABLE_TAB"
    ])
    .sort(["CUSTNO","IDTYPE","ID"])
)

print("ORGANISATION FILE")
print(org.head(5))


# -------------------------------------------------------------------
# MERGE ORG + CART
# -------------------------------------------------------------------
custinfo_org = (
    org.join(cart, on=["CUSTNO","IDTYPE","ID"], how="left")
)

print("CUSTINFO (ORG)")
print(custinfo_org.head(5))


# -------------------------------------------------------------------
# MERGE MAIN (ORG) + CUSTINFO (ORG)
# -------------------------------------------------------------------
orgdly = (
    main_org.join(custinfo_org, on="CUSTNO", how="inner")
)

orgdly = orgdly.unique(subset=["CUSTNO","IDTYPE","ID"])

print("FINAL ORGANISATION DATASET")
print(orgdly.head(5))


# -------------------------------------------------------------------
# SAVE RESULTS TO PARQUET (like SASFILE LIBRARY)
# -------------------------------------------------------------------
indvdly.write_parquet("INDVDLY.parquet")
orgdly.write_parquet("ORGDLY.parquet")

print("Processing complete. Files written: INDVDLY.parquet, ORGDLY.parquet")
