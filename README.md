# vnstock_scanning_bot
![Project Pipeline](https://github.com/ThanhNg1712/vnstock_scanning_bot/raw/main/DEC-final_project.drawio.png)

This project transforms the stock data landscape by seamlessly integrating data extraction, analysis, and real-time communication. Leveraging an open API:https://thinhvu.com/2022/09/22/vnstock-api-tai-du-lieu-chung-khoan-python/

The data extraction process occurs daily at 10 a.m. and efficiently stores yearly data in a structured format for further analysis. It optimizes storage by routinely removing the previous day's data. The stock analysis component, powered by PySpark across Dataproc clusters, calculates the 20-day moving average and employs linear regression to identify stocks on a stable upward trajectory. Stock subscriptions are facilitated through Pub/Sub and Google Cloud Function, enabling users to customize their stock information preferences. Additionally, an hourly update to a Telegram chatbot enhances real-time communication. This project, combining data, automation, and real-time updates, offers a versatile approach to stock market insights.

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
- It is scheduled to run daily by airflow

## Stock Subscriptions
Script: `stock_subcribe_DAG.py`,`send_telegram_message_DAG`

- These jobs allow subscribers to choose the stock they want to receive information from the stock they subscribe
- The initial service utilized in this context is Pub/Sub. The Airflow DAG is configured to send selected stock information to Pub/Sub every hour, catering to the preferences of the customer.
- A Google Cloud Function has been established with the purpose of decoding, converting, and pushing Pub/Sub messages into BigQuery whenever a new message is published. `pubsub2biguqery_cloudfunction`
- Additionally, the data is dispatched to a Telegram chatbot on an hourly basis, with this action also initiated by the Airflow scheduling.
  
![Pub/Sub Message](https://github.com/ThanhNg1712/vnstock_scanning_bot/raw/main/pubsub_message.png)

![Telegram Message](https://github.com/ThanhNg1712/vnstock_scanning_bot/raw/main/telegram_message.png)

  




  
