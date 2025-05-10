from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, date_format

spark = SparkSession.builder \
    .appName("CleanAndMergeBankTxnData") \
    .getOrCreate()

gcs_input_path = "gs://sumas-bucket/injection/*.csv"
gcs_output_path = "gs://sumas-bucket/cleaned_data.csv"

df = spark.read.option("header", True).csv(gcs_input_path)

print("Schema of raw data:")
df.printSchema()

required_fields = ["TransactionID", "BranchName", "BranchCity", "Date", "Status", "Amount"]

df_trimmed = df.select([trim(col(c)).alias(c) for c in df.columns])

for field in required_fields:
    df_trimmed = df_trimmed.filter((col(field).isNotNull()) & (col(field) != ""))

# Convert 'Date' from 'dd-MM-yyyy' to 'yyyy-MM-dd'
df_transformed = df_trimmed.withColumn("Date", date_format(to_date(col("Date"), "dd-MM-yyyy"), "yyyy-MM-dd"))

df_cleaned = df_transformed.dropDuplicates()

df_cleaned.write.mode("overwrite").csv(gcs_output_path, header=True)

print(f"Cleaned and merged data written to {gcs_output_path}")
spark.stop()
