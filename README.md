# vnstock_scanning_bot
![Project Pipeline](https://github.com/ThanhNg1712/vnstock_scanning_bot/raw/main/DEC-final_project.drawio.png)

This project shows a tool to scan Viet Nam stocks from an open API
Finding stable increasing stock by utilize linear regression, runs parralel in different cluster with pyspark in dataproc











![DAGs](https://github.com/ThanhNg1712/vnstock_scanning_bot/blob/main/DAGs_vnstock.png)
Script: `extract_yeardata_DAG.py`,`extract_daily.py`
## Airflow Google Cloud Composer set up ( PYPI packages)
- vnstock
- beautifulsoup4
- python-telegram-bot

## Extract Stock Data
- Stock data extraction occurs daily at 10 a.m. through the Airflow scheduling system.
- The initial step involves obtaining yearly data from the provided API via the first pipeline
-  The yearly extracted data is stored in a bucket with the format 2023_data_currentdate.csv
-  The saved data is moved to the data warehouse within BigQuery for further analysis and reporting.\
-  To conserve storage space, data from one day ago is regularly removed
-  In addition to these tasks, daily data extraction is performed; however, this data is not transferred to BigQuery.
