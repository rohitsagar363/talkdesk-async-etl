from pyspark.sql import SparkSession


spark = SparkSession.builder.getOrCreate()

storage_account = dbutils.secrets.get("talkdesk-scope", "adls-storage-account")

spark.sql("CREATE DATABASE IF NOT EXISTS talkdesk_prod")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS talkdesk_prod.report_config (
    report_name STRING,
    enabled BOOLEAN,
    endpoint_type STRING,
    retries INT,
    timeout_sec INT,
    env STRING
)
USING DELTA
LOCATION 'abfss://config@{storage_account}.dfs.core.windows.net/talkdesk/report_config/'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS talkdesk_prod.endpoint_config (
    endpoint_type STRING,
    base_url STRING,
    auth_endpoint STRING,
    post_endpoint STRING,
    get_endpoint STRING,
    env STRING
)
USING DELTA
LOCATION 'abfss://config@{storage_account}.dfs.core.windows.net/talkdesk/endpoint_config/'
""")

# Seed default dev/prod endpoint rows (run once or idempotently after clearing duplicates)
spark.sql("""
INSERT INTO talkdesk_prod.endpoint_config (
    endpoint_type,
    base_url,
    auth_endpoint,
    post_endpoint,
    get_endpoint,
    env
)
VALUES
  ('standard',
   'https://api.talkdesk.com/api/v1',
   '/oauth/token',
   '/reports/generate',
   '/reports/download',
   'prod'),
  ('standard',
   'https://api.talkdesk.com/api/v1',
   '/oauth/token',
   '/reports/generate',
   '/reports/download',
   'dev')
""")

# Seed default report configs for prod and dev (run once or clear before re-running)
spark.sql("""
INSERT INTO talkdesk_prod.report_config (
    report_name,
    enabled,
    endpoint_type,
    retries,
    timeout_sec,
    env
)
VALUES
  ('agent_activity',     true, 'standard', 3, 30, 'prod'),
  ('call_volume',        true, 'standard', 3, 30, 'prod'),
  ('queue_activity',     true, 'standard', 3, 30, 'prod'),
  ('call_details',       true, 'standard', 3, 60, 'prod'),
  ('inbound_calls',      true, 'standard', 3, 30, 'prod'),
  ('outbound_calls',     true, 'standard', 3, 30, 'prod'),
  ('service_level',      true, 'standard', 3, 30, 'prod'),
  ('call_dispositions',  true, 'standard', 3, 30, 'prod'),

  ('agent_activity',     true, 'standard', 3, 30, 'dev'),
  ('call_volume',        true, 'standard', 3, 30, 'dev'),
  ('queue_activity',     true, 'standard', 3, 30, 'dev'),
  ('call_details',       true, 'standard', 3, 60, 'dev'),
  ('inbound_calls',      true, 'standard', 3, 30, 'dev'),
  ('outbound_calls',     true, 'standard', 3, 30, 'dev'),
  ('service_level',      true, 'standard', 3, 30, 'dev'),
  ('call_dispositions',  true, 'standard', 3, 30, 'dev')
""")

