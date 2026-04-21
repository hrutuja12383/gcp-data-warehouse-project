# /*

# DDL Script: Create Silver Tables (BigQuery)

Script Purpose:
This script creates tables in the 'silver' dataset in Google BigQuery.
Existing tables are replaced if they already exist.

```
Run this script to define the structure of Silver layer tables
based on transformed data from the Bronze layer.
```

===============================================================================
*/

-- =====================================================================
-- Table: silver.crm_cust_info
-- =====================================================================
CREATE OR REPLACE TABLE `datawarehouse-491414.silver.crm_cust_info` AS
SELECT
cst_id,
TRIM(cst_key) AS cst_key,
COALESCE(TRIM(cst_firstname), 'Unknown') AS cst_firstname,
COALESCE(TRIM(cst_lastname), 'Unknown') AS cst_lastname,
CASE
WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
ELSE 'n/a'
END AS cst_marital_status,
CASE
WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
ELSE 'n/a'
END AS cst_gndr,
SAFE_CAST(cst_create_date AS DATE) AS cst_create_date,
CURRENT_TIMESTAMP() AS dwh_create_date
FROM `datawarehouse-491414.bronze.crm_cust_info`
WHERE cst_id IS NOT NULL;

-- =====================================================================
-- Table: silver.crm_prd_info
-- =====================================================================
CREATE OR REPLACE TABLE `datawarehouse-491414.silver.crm_prd_info` AS
SELECT
prd_id,
REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTR(prd_key, 7) AS prd_key,
prd_nm,
IFNULL(prd_cost, 0) AS prd_cost,
CASE
WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
ELSE 'n/a'
END AS prd_line,
DATE(prd_start_dt) AS prd_start_dt,
DATE(
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
) - 1 AS prd_end_dt,
CURRENT_TIMESTAMP() AS dwh_create_date
FROM `datawarehouse-491414.bronze.crm_prd_info`;

-- =====================================================================
-- Table: silver.crm_sales_details
-- =====================================================================
CREATE OR REPLACE TABLE `datawarehouse-491414.silver.crm_sales_details` AS
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,

```
CASE 
    WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL
    ELSE PARSE_DATE('%Y%m%d', CAST(sls_order_dt AS STRING))
END AS sls_order_dt,

CASE 
    WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL
    ELSE PARSE_DATE('%Y%m%d', CAST(sls_ship_dt AS STRING))
END AS sls_ship_dt,

CASE 
    WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL
    ELSE PARSE_DATE('%Y%m%d', CAST(sls_due_dt AS STRING))
END AS sls_due_dt,

CASE 
    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales,

sls_quantity,

CASE 
    WHEN sls_price IS NULL OR sls_price <= 0 
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END AS sls_price,

CURRENT_TIMESTAMP() AS dwh_create_date
```

FROM `datawarehouse-491414.bronze.crm_sales_details`;

-- =====================================================================
-- Table: silver.erp_cust_az12
-- =====================================================================
CREATE OR REPLACE TABLE `datawarehouse-491414.silver.erp_CUST_AZ12` AS
SELECT
CASE
WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4)
ELSE cid
END AS cid,

```
CASE
    WHEN bdate > CURRENT_DATE() THEN NULL
    ELSE bdate
END AS bdate,

CASE
    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
    ELSE 'n/a'
END AS gen,

CURRENT_TIMESTAMP() AS dwh_create_date
```

FROM `datawarehouse-491414.bronze.erp_CUST_AZ12`;

-- =====================================================================
-- Table: silver.erp_loc_a101
-- =====================================================================
CREATE OR REPLACE TABLE `datawarehouse-491414.silver.erp_LOC_A101` AS
SELECT
REPLACE(cid, '-', '') AS cid,

```
CASE
    WHEN TRIM(cntry) = 'DE' THEN 'Germany'
    WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
END AS cntry,

CURRENT_TIMESTAMP() AS dwh_create_date
```

FROM `datawarehouse-491414.bronze.erp_LOC_A101`;

-- =====================================================================
-- Table: silver.erp_px_cat_g1v2
-- =====================================================================
CREATE OR REPLACE TABLE `datawarehouse-491414.silver.erp_PX_CAT_G1V2` AS
SELECT
id,
cat,
subcat,
maintenance,
CURRENT_TIMESTAMP() AS dwh_create_date
FROM `datawarehouse-491414.bronze.erp_PX_CAT_G1V2`;
