# %%
from settings import settings, logger
import pandas as pd
import sqlite3
import pyodbc
def main():
    conn_str = (
        'Driver={SQL Server};'
        'SERVER=' + settings.servername + ';'
        'DATABASE=' + settings.database + ';'
        'Trusted_Connection=yes;'
    )

    # Establishing the connection
    export_conn = pyodbc.connect(conn_str)
    export_cursor = export_conn.cursor()
    export_cursor

    #Connecties en dataframes
    inventory_levels_df = pd.read_csv(settings.inventory_levels_csv, index_col=False)
    sales_product_forecast_df = pd.read_csv(settings.go_sales_product_forecast_csv, index_col=False)
    goCrm = sqlite3.connect(settings.go_crm_sqlite)
    goSales = sqlite3.connect(settings.go_sales_sqlite)
    goStaff = sqlite3.connect(settings.go_staff_sqlite)

    #inventory_levels
    product_df = pd.read_sql_query("SELECT PRODUCT_NUMBER, MARGIN, LANGUAGE, DESCRIPTION, INTRODUCTION_DATE FROM product", goSales)
    inventory_levels_df['PRODUCT_NUMBER'] = inventory_levels_df['PRODUCT_NUMBER'].astype(str)
    merged_df = pd.merge(inventory_levels_df, product_df, on='PRODUCT_NUMBER', how='left')
    #Replace nan with empty string if exist
    merged_df[['MARGIN', 'LANGUAGE', 'DESCRIPTION']] = merged_df[['MARGIN', 'LANGUAGE', 'DESCRIPTION']].fillna('')
    # Establish connection to SQL Server database
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()

    # Iterate over the rows of the merged DataFrame and insert them into the database
    for index, row in merged_df.iterrows():
        cursor.execute("INSERT INTO [PR4].[dbo].[INVENTORY_SALES_LEVELS] (INVENTORY_YEAR, INVENTORY_MONTH, PRODUCT_NUMBER, INVENTORY_COUNT, MARGIN, LANGUAGE, DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       row['INVENTORY_YEAR'], row['INVENTORY_MONTH'], row['PRODUCT_NUMBER'], row['INVENTORY_COUNT'], row['MARGIN'], row['LANGUAGE'], row['DESCRIPTION'])

    # Commit the transaction and close the cursor and connection
    export_conn.commit()
    cursor.close()
    export_conn.close()

    #sales_product_forecast
    sales_product_forecast_df['PRODUCT_NUMBER'] = sales_product_forecast_df['PRODUCT_NUMBER'].astype(str)
    # Merge inventory_levels_df with product_df on PRODUCT_NUMBER
    merged_df = pd.merge(sales_product_forecast_df, product_df, on='PRODUCT_NUMBER', how='left')
    # Replace NaN values in MARGIN, LANGUAGE, and DESCRIPTION with empty strings
    merged_df[['MARGIN', 'LANGUAGE', 'DESCRIPTION']] = merged_df[['MARGIN', 'LANGUAGE', 'DESCRIPTION']].fillna('')
    # Establish connection to SQL Server database
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()

    # Iterate over the rows of the merged DataFrame and insert them into the database
    for index, row in merged_df.iterrows():
        cursor.execute("INSERT INTO [PR4].[dbo].[SALES_PRODUCT_FORECAST] (YEAR, MONTH, PRODUCT_NUMBER, EXPECTED_VOLUME, MARGIN, LANGUAGE, DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       row['YEAR'], row['MONTH'], row['PRODUCT_NUMBER'], row['EXPECTED_VOLUME'], row['MARGIN'], row['LANGUAGE'], row['DESCRIPTION'])

    # Commit the transaction and close the cursor and connection
    export_conn.commit()
    cursor.close()
    export_conn.close()

    #returned_item_df
    # Read data from the 'returned_item' table into a DataFrame
    returned_item_df = pd.read_sql_query("SELECT RETURN_CODE, RETURN_DATE, ORDER_DETAIL_CODE, RETURN_REASON_CODE, RETURN_QUANTITY FROM returned_item", goSales)

    # Establish connection to SQL Server database
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()

    # Iterate over the rows of the DataFrame and insert them into the SQL Server database
    for index, row in returned_item_df.iterrows():
        cursor.execute("INSERT INTO [PR4].[dbo].[RETURNED_ITEM] (RETURN_CODE, RETURN_DATE, ORDER_NUMBER, RETURN_REASON_CODE, RETURN_QUANTITY) VALUES (?, ?, ?, ?, ?)",
                       row['RETURN_CODE'], row['RETURN_DATE'], row['ORDER_DETAIL_CODE'], row['RETURN_REASON_CODE'], row['RETURN_QUANTITY'])

    # Commit the transaction and close the cursor and connection
    export_conn.commit()
    cursor.close()
    export_conn.close()

    #Sales_target_data
    # Read data from the 'SALES_TARGETData' table into a DataFrame
    sales_targetdata_df = pd.read_sql_query("SELECT Id, SALES_STAFF_CODE, SALES_YEAR, SALES_PERIOD, RETAILER_NAME, PRODUCT_NUMBER, SALES_TARGET, RETAILER_CODE FROM SALES_TARGETData", goSales)

    # Establish connection to SQL Server database
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()

    # Merge with product_df on PRODUCT_NUMBER to get additional columns
    merged_sales_targetdata_df = pd.merge(sales_targetdata_df, product_df, on='PRODUCT_NUMBER', how='left')

    # Iterate over the rows of the DataFrame and insert them into the SQL Server database
    for index, row in merged_sales_targetdata_df.iterrows():
        cursor.execute("INSERT INTO [PR4].[dbo].[SALES_TARGET_DATA] (SALES_TARGET_DATA_id, SALES_STAFF_CODE, SALES_YEAR, SALES_PERIODS, RETAILER_NAME, PRODUCT_NUMBER, SALES_TARGET, INTRODUCTION_DATE, RETAILER_CODE, MARGIN, LANGUAGE, DESCRIPTION) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       row['Id'], row['SALES_STAFF_CODE'], row['SALES_YEAR'], row['SALES_PERIOD'], row['RETAILER_NAME'], row['PRODUCT_NUMBER'], row['SALES_TARGET'], row['INTRODUCTION_DATE'], row['RETAILER_CODE'], row['MARGIN'], row['LANGUAGE'], row['DESCRIPTION'])

    # Commit the transaction and close the cursor and connection
    export_conn.commit()
    cursor.close()
    export_conn.close()

    #Satisfaction
    satisfaction_df = pd.read_sql_query("SELECT YEAR, SALES_STAFF_CODE, SATISFACTION_TYPE_CODE FROM SATISFACTION", goStaff)

    satisfaction_type_df = pd.read_sql_query("SELECT SATISFACTION_TYPE_CODE, SATISFACTION_TYPE_DESCRIPTION FROM satisfaction_type", goStaff)

    sales_staff_df = pd.read_sql_query("SELECT SALES_STAFF_CODE, POSITION_EN, EXTENSION, DATE_HIRED FROM sales_staff", goStaff)

    merged_satisfaction_df = pd.merge(satisfaction_df, satisfaction_type_df, on='SATISFACTION_TYPE_CODE', how='left')

    final_merged_df = pd.merge(merged_satisfaction_df, sales_staff_df, on='SALES_STAFF_CODE', how='left')

    # Establish connection to SQL Server database
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()

    # Iterate over the rows of the DataFrame and insert them into the SQL Server database
    for index, row in final_merged_df.iterrows():
        cursor.execute("INSERT INTO [PR4].[dbo].[SATISFACTION] (YEAR, SALES_STAFF_CODE, SATISFACTION_TYPE_CODE, SATISFACTION_TYPE_DESCRIPTION, POSITION_EN, EXTENSION, DATE_HIRED) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       row['YEAR'], row['SALES_STAFF_CODE'], row['SATISFACTION_TYPE_CODE'], row['SATISFACTION_TYPE_DESCRIPTION'], row['POSITION_EN'], row['EXTENSION'], row['DATE_HIRED'])

    # Commit the transaction and close the cursor and connection
    export_conn.commit()
    cursor.close()
    export_conn.close()

    #SALES_DEMOGRAPHIC
    # Read data from the 'SALES_DEMOGRAPHIC' table into a DataFrame
    SALES_DEMOGRAPHIC_df = pd.read_sql_query("SELECT DEMOGRAPHIC_CODE, RETAILER_CODEMR, AGE_GROUP_CODE, SALES_PERCENT FROM SALES_DEMOGRAPHIC", goCrm)

    # Read data from the 'AGE_GROUP' table into a DataFrame
    AGE_GROUP_df = pd.read_sql_query("SELECT UPPER_AGE, LOWER_AGE, AGE_GROUP_CODE FROM AGE_GROUP", goCrm)

    # Read data from the 'RETAILER' table into a DataFrame
    RETAILER_df = pd.read_sql_query("SELECT COMPANY_NAME, RETAILER_CODEMR FROM RETAILER", goCrm)

    # Merge SALES_DEMOGRAPHIC_df with AGE_GROUP_df on AGE_GROUP_CODE to get additional columns
    merged_sales_demographic_df = pd.merge(SALES_DEMOGRAPHIC_df, AGE_GROUP_df, on='AGE_GROUP_CODE', how='left')

    # Merge merged_sales_demographic_df with RETAILER_df on RETAILER_CODEMR to get additional columns
    final_merged_df = pd.merge(merged_sales_demographic_df, RETAILER_df, on='RETAILER_CODEMR', how='left')

    # Establish connection to SQL Server database
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()

    # Iterate over the rows of the DataFrame and insert them into the SQL Server database
    for index, row in final_merged_df.iterrows():
        cursor.execute("INSERT INTO [PR4].[dbo].[SALES_DEMOGRAPHIC] (DEMOGRAPHIC_CODE, UPPER_AGE, LOWER_AGE, AGE_GROUP_CODE, SALES_PERCENT, COMPANY_NAME, RETAILER_CODEMR) VALUES (?, ?, ?, ?, ?, ?, ?)",
                       str(row['DEMOGRAPHIC_CODE']), str(row['UPPER_AGE']), str(row['LOWER_AGE']), str(row['AGE_GROUP_CODE']), str(row['SALES_PERCENT']), str(row['COMPANY_NAME']), str(row['RETAILER_CODEMR']))

    # Commit the transaction and close the cursor and connection
    export_conn.commit()
    cursor.close()
    export_conn.close()

    trainingDF = pd.read_sql_query("SELECT * FROM TRAINING", goStaff)
    courseDF = pd.read_sql_query("SELECT * FROM COURSE", goStaff)
    staffDF = pd.read_sql_query("SELECT * FROM SALES_STAFF", goStaff)
    merged_trainingCourse_df = pd.merge(trainingDF, courseDF, on='COURSE_CODE')
    merged_helemaal = pd.merge(merged_trainingCourse_df, staffDF, on='SALES_STAFF_CODE')
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()
    for index, row in merged_helemaal.iterrows():
        cursor.execute("INSERT INTO [PR4].[dbo].[TRAINING]([YEAR],[SALES_STAFF_CODE], [COURSE_CODE], [COURSE_DESCRIPTION],[POSITION_EN],[EXTENSION],[DATE_HIRED]) VALUES (?, ?, ?,?,?,?,?)", row["YEAR"], row["SALES_STAFF_CODE"],row["COURSE_CODE"],row["COURSE_DESCRIPTION"],row["POSITION_EN"],row["EXTENSION"],row["DATE_HIRED"])

    export_conn.commit()

    # Establishing the connection
    export_conn = pyodbc.connect(conn_str)
    cursor = export_conn.cursor()

    retailerDF = pd.read_sql_query("SELECT * FROM RETAILER_SITE", goSales)
    productDF = pd.read_sql_query("SELECT * FROM PRODUCT", goSales)
    sales_staffDF = pd.read_sql_query("SELECT * FROM SALES_STAFF", goSales)
    orderheaderDF = pd.read_sql_query("SELECT * FROM ORDER_HEADER", goSales)
    orderdetailsDF = pd.read_sql_query("SELECT * FROM ORDER_DETAILS", goSales)
    sales_branchDF = pd.read_sql_query("SELECT * FROM SALES_BRANCH", goSales)

    mergedOrdersDF = pd.merge(orderheaderDF, orderdetailsDF, on='ORDER_NUMBER', how='left', suffixes=('_orderheader', '_orderdetails'))
    merged_products = pd.merge(mergedOrdersDF, productDF, on='PRODUCT_NUMBER', how='left', suffixes=('_mergedOrders', '_product'))
    mergedsiteDF = pd.merge(merged_products, retailerDF, on='RETAILER_SITE_CODE', how='left', suffixes=('_mergedProducts', '_retailer'))
    mergeSalesBranchDF = pd.merge(mergedsiteDF, sales_branchDF, on='SALES_BRANCH_CODE', how='left', suffixes=('_mergedsite', '_sales_branch'))
    merged_allesDF = pd.merge(mergeSalesBranchDF, sales_staffDF, on='SALES_STAFF_CODE', how='left', suffixes=('_mergeSalesBranch', '_sales_staff'))

    for index, row in merged_allesDF.iterrows():
        cursor.execute(
            "INSERT INTO [PR4].[dbo].[ORDER_DETAILS1]([ORDER_NUMBER],[ORDER_DATE], [QUANTITY], [UNIT_PRICE],[UNIT_SALE_PRICE],[RETAILER_NAME],[PRODUCT_NUMBER],[INTRODUCTION_DATE],[MARGIN],[LANGUAGE],[DESCRIPTION],[SALES_STAFF_CODE],[POSITION_EN],[EXTENSION],[DATE_HIRED],[RETAILER_CONTACT_CODE],[RETAILER_SITE_CODE]) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            row["ORDER_NUMBER"], row["ORDER_DATE"], row["QUANTITY"], row["UNIT_PRICE"], row["UNIT_SALE_PRICE"], row["RETAILER_NAME"], row["PRODUCT_NUMBER"], row["INTRODUCTION_DATE"], row["MARGIN"], row["LANGUAGE"], row["DESCRIPTION"], row["SALES_STAFF_CODE"], row["POSITION_EN"], row["EXTENSION"], row["DATE_HIRED"], row["RETAILER_CONTACT_CODE"], row["RETAILER_SITE_CODE"])

    export_conn.commit()
    cursor.close()
    export_conn.close()

if __name__ == "__main__":
    main()