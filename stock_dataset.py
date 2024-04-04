from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder \
    .appName("stock_csv_reader") \
    .getOrCreate()

csv_file_path = ""

# Read the CSV file into a DataFrame
stock_read_schema = StructType([

    StructField("date", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", StringType(), True),
    StructField("ticker", StringType(), True)
    # Add more fields with appropriate data types as necessary
])

class stock_dataset(object):
    def __init__(self,file_path,schema=None):
        self.load_stock_file(file_path=file_path, schema=schema)

    def load_stock_file(self, file_path, schema=None):
        if schema is None:
            schema = stock_read_schema
        df = spark.read.option("header", "true").schema(schema).csv(file_path)
        df = df.withColumn('date', to_date(col('date'), 'M/d/yyyy'))
        df = df.withColumn("return", (col("close") - col("open")) / col("open"))
        df = df.withColumn("trade_value", col("close") * col("volume"))


        self.stocks_df  = df
        #todo - maybe add some tests






if __name__ == '__main__':
    pass
