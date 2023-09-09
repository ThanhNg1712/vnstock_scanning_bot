from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.window import Window
from pyspark.sql.functions import avg
from datetime import datetime, timedelta

try:
    # Create a Spark session with the BigQuery connector JAR
    spark = SparkSession.builder \
    .appName("StableStockProcessing") \
    .config("spark.jars.packages", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

# Rest of your Spark job 

    # Set the bucket and file prefix
    bucket_name = "vnstock_year"  # Replace with your actual bucket name
    file_prefix = "2023_data_"

    # Calculate the current date for the file name
    current_date = datetime.now().strftime('%Y%m%d')
    file_name = file_prefix + current_date + '.csv'

    # Read the CSV file from the specified bucket
    file_path = f'gs://{bucket_name}/{file_name}'
    stock_scanning = spark.read.csv(file_path, header=True, inferSchema=True)

    # Convert 'time' column to date format
    stock_scanning = stock_scanning.withColumn('time', col('time').cast('date'))

    # Get the maximum date from the dataset
    max_date = stock_scanning.selectExpr('max(time)').collect()[0][0]

    # Set the end date as the maximum date
    end_date = max_date

    # Set the date range for the last 3 months
    start_date = end_date - timedelta(days=90)

    # Filter data within the last 3 months
    filtered_data = stock_scanning.filter((col('time') >= start_date) & (col('time') <= end_date))

    # Calculate 20-day moving averages
    window_spec = Window.partitionBy('ticker').orderBy('time').rowsBetween(-19, 0)
    filtered_data = filtered_data.withColumn('20_day_moving_avg', avg('close').over(window_spec))

    # List to store tickers with positive correlation between 0.5 and 0.7
    selected_correlation_tickers = []

    for ticker in filtered_data.select('ticker').distinct().rdd.flatMap(lambda x: x).collect():
        ticker_data = filtered_data.filter(col('ticker') == ticker)

        # Remove rows with missing '20_day_moving_avg' values
        ticker_data = ticker_data.dropna(subset=['20_day_moving_avg'])

        # Calculate the correlation coefficient between index and '20_day_moving_avg'
        correlation_coefficient = ticker_data.stat.corr('20_day_moving_avg', 'close')

        # Check if the correlation coefficient is between 0.5 and 0.7
        if 0.5 <= correlation_coefficient <= 0.7:
            selected_correlation_tickers.append((ticker, correlation_coefficient))

    # Create a DataFrame to store the selected tickers and their correlation coefficients
    stable_stock = spark.createDataFrame(selected_correlation_tickers, ['Ticker', 'Correlation_Coefficient'])

    # Add a 'time' column with the test end_date
    stable_stock = stable_stock.withColumn('time', lit(end_date))
    stable_stock = stable_stock.withColumn('Correlation_Coefficient', col('Correlation_Coefficient').cast('float'))

    stable_stock.show(5)

    # Write stable stock data to BigQuery
    stable_stock.write \
    .format('bigquery') \
    .mode('append') \
    .option("table", "pyspark-practice-395516.stable_stock.stable_tickers") \
    .option("temporaryGcsBucket", "staging_bucket_stable_stock") \
    .save()
    # Stop the Spark session
    spark.stop()

except Exception as e:
    # Print the error message
    print(f"An error occurred: {str(e)}")
