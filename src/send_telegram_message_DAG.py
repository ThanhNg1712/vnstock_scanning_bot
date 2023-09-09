# Import necessary libraries
import pandas as pd
from vnstock import *
import telegram
import random
import asyncio
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),  # Set the execution timeout to 30 minutes
    # Other default arguments
}

# Define the Airflow DAG
dag = DAG(
    'send_Telegram_message',
    default_args=default_args,
    schedule_interval='@hourly', # Set the schedule to run every 1 hour
    start_date=days_ago(2),
    catchup=False,
)

# Define an asynchronous function to send a Telegram message
async def send_test_message(ticker, price, macd_signal):
    try:
        # Initialize the Telegram bot
        telegram_notify = telegram.Bot("6628072675:AAFfDd-COUx8AT9jG96JLCiF5_5Axp6Al0I")  # Replace with your actual bot token
        
        # Create the message content
        message = f"`The '{ticker}' is good to buy at price {price} - MACD signal: {macd_signal}`"

        # Send the message
        await telegram_notify.send_message(chat_id="-937396441", text=message, parse_mode='Markdown')
    except Exception as ex:
        print(ex)

# Define a function to get stock data and send a Telegram message
def get_data_and_send_message():
    try:
        # Retrieve stock data for specified tickers
        tickers = 'TCB,SSI,VND,VNM,VIC,FPT'
        price = price_board(tickers)
        
        # Keep only the first 15 columns
        price = price.iloc[:, :15]

        new_column_names = [
            'Ma_CP',
            'Gia',
            'KLBD_TB5D',
            'T_do_GD',
            'KLGD_rong_CM',
            '%KLGD_rong_CM',
            'RSI',
            'MACD_Hist',
            'MACD_Signal',
            'Tin_hieu_KT',
            'Tin_hieu_TB_dong',
            'MA20',
            'MA50',
            'MA100',
            'Phien',
        ]
        
        # Rename the columns using the new names
        price.columns = new_column_names

        # Filter rows with 'Buy' signal in the 'MACD_Signal' column
        buy_signals = price[price['MACD_Signal'] == 'Buy']

        # Print the buy_signals DataFrame to check if it's empty
        print(buy_signals)
        
        if not buy_signals.empty:
            # Extract the relevant information from the first row
            ticker = buy_signals['Ma_CP'].values[0]
            price_value = buy_signals['Gia'].values[0]
            macd_signal = buy_signals['MACD_Signal'].values[0]

            # Send Telegram message asynchronously
            loop = asyncio.get_event_loop()
            loop.run_until_complete(send_test_message(ticker, price_value, macd_signal))
    except Exception as ex:
        print(f"Error in get_data_and_send_message: {ex}")

# Define a PythonOperator to trigger the message sending task
trigger_message_telegram = PythonOperator(
    task_id='trigger_message_to_telegram',
    python_callable=get_data_and_send_message,
    dag=dag,
)

# Specify the Airflow task dependencies
trigger_message_telegram
