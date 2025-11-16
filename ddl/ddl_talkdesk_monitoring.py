from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

storage_account = dbutils.secrets.get("talkdesk-scope", "adls-storage-account")

spark.sql("CREATE DATABASE IF NOT EXISTS talkdesk_prod")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS talkdesk_prod.job_monitoring (
    run_id STRING,
    from_date STRING,
    to_date STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    total_reports INT,
    success_count INT,
    failed_count INT,
    error_message STRING
)
USING DELTA
LOCATION 'abfss://config@{storage_account}.dfs.core.windows.net/talkdesk/jobs/'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS talkdesk_prod.report_monitoring (
    run_id STRING,
    report_name STRING,
    from_date STRING,
    to_date STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING,
    rows_written INT,
    error_message STRING
)
USING DELTA
LOCATION 'abfss://config@{storage_account}.dfs.core.windows.net/talkdesk/reports/'
""")

