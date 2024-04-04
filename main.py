import os

from reportlab.lib.pdfencrypt import os_urandom

from stock_dataset import stock_dataset
from pyspark.sql.functions import col, round, format_string, avg, stddev, sqrt, lit, desc, lag
from pyspark.sql import Window
#I assume that 252 is the number of trading days in a year.
TRADIG_DAYS = 252


def question_1_average_daily_return(df):
    average_return_df = df.groupBy("date").agg(avg("return").alias("average_return")).orderBy("date", ascending=False)
    rounded_df = average_return_df.withColumn("average_return", round(col("average_return"), 2))
    formatted_df = rounded_df.withColumn("average_return", format_string("%.2f%%", col("average_return")))
    return formatted_df

def question_2_most_frequently(df):
    avg_trade_value_by_stock = df.groupBy("ticker").agg(avg("trade_value").alias("frequency"))
    most_traded_stock = avg_trade_value_by_stock.orderBy(desc("frequency")).limit(1)
    return most_traded_stock


def question_3_most_volatile(df):
    volatility_df = df.groupBy("ticker") \
        .agg((stddev(col("return")) * sqrt(lit(TRADIG_DAYS))).alias("standard deviation")) \
        .orderBy(col("standard deviation").desc())
    return volatility_df.limit(1)


def question_4_top_3_30_days_return(df):
    windowSpec = Window.partitionBy("ticker").orderBy("date")
    df_with_lag = df.withColumn("close_30_days_prior", lag("close", 30).over(windowSpec))
    df_with_returns = df_with_lag.withColumn("30_day_return", ((col("close") - col("close_30_days_prior")) / col("close_30_days_prior")) * 100)
    top_returns = df_with_returns.orderBy(col("30_day_return").desc()).select("ticker", "date", "30_day_return")
    return top_returns.limit(3)


def run_objectives(csv_path, output_dir,  schema=None):
    stocks = stock_dataset(csv_path, schema=schema)
    res_1 = question_1_average_daily_return(stocks.stocks_df)
    res_1.write.csv(os.path.join(output_dir, 'question_1_results.csv'),header=True)
    print(res_1.show())
    res_2 = question_2_most_frequently(stocks.stocks_df)
    res_2.write.csv(os.path.join(output_dir, 'question_2_results.csv'), header=True)
    print(res_2.show())
    res_3 = question_3_most_volatile(stocks.stocks_df)
    res_3.write.csv(os.path.join(output_dir, 'question_3_results.csv'), header=True)
    print(res_3.show())
    res_4 = question_4_top_3_30_days_return(stocks.stocks_df)
    res_4.write.csv(os.path.join(output_dir, 'question_4_results.csv'), header=True)
    print(res_4.show())





if __name__ == '__main__':
    csv_path = '/home/eliad/Downloads/stock_prices.csv'
    output_dir = ''
    run_objectives(path)