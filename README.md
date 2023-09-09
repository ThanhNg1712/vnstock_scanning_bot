# vnstock_scanning_bot
![Project Pipeline](https://github.com/ThanhNg1712/vnstock_scanning_bot/raw/main/DEC-final_project.drawio.png)

This project shows a tool to scan Viet Nam stocks from an open API
Finding stable increasing stock by utilize linear regression, runs parralel in different cluster with pyspark in dataproc











![DAGs](https://github.com/ThanhNg1712/vnstock_scanning_bot/blob/main/DAGs_vnstock.png)

## Airflow Google Cloud Composer set up ( PYPI packages)
- vnstock
- beautifulsoup4
- python-telegram-bot

## Extract Stock Data
Script: `extract_yeardata_DAG.py`,`extract_daily.py`
- Stock data extraction occurs daily at 10 a.m. through the Airflow scheduling system.
- The initial step involves obtaining yearly data from the provided API via the first pipeline
- The yearly extracted data is stored in a bucket with the format 2023_data_currentdate.csv
- The saved data is moved to the data warehouse within BigQuery for further analysis and reporting.
- To conserve storage space, data from one day ago is regularly removed
- In addition to these tasks, daily data extraction is performed; however, this data is not transferred to BigQuery.

## Scanning Stable Stocks
Script: `stock_scan_dataproc.py`,`stock_scan_dataproc_DAG.py`

- The `stock_scan_dataproc.py` accesses the stock data by reading the file format 2023_data_currentdate.csv
- This script is implemented in PySpark to enable parallel execution across multiple clusters within Dataproc, aiming to optimize performance and efficiency
- The job is calculating the moving average 20 of the closing price in the last 3 months
- A linear regression model has been applied to the moving average index over time, revealing a positive slope for the chosen stock where the stock exhibits a stable, consistent increase.
- The `stock_scan_dataproc_DAG.py` generates an airflow pipeline that creates a cluster, submits the pyspark jobs then deletes the cluster afterward
- it is scheduled to run daily by airflow

## Stock Subscriptions
Script: `stock_subcribe_DAG.py`

-




  
