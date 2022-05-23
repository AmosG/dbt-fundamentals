# Databricks notebook source
print("installing dbt")
%pip install git+https://github.com/dbt-labs/dbt-spark.git
%pip install dbt-core==1.1.0b1
print("freeze")
%pip freeze

# COMMAND ----------

# MAGIC %sh
# MAGIC git clone https://github.com/dbt-labs/jaffle_shop.git

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /root/.dbt 
# MAGIC cat <<EOT > /root/.dbt/profiles.yml
# MAGIC jaffle_shop:
# MAGIC   target: dev
# MAGIC   outputs:
# MAGIC     dev:
# MAGIC       type: spark
# MAGIC       method: session
# MAGIC       host: localhost
# MAGIC       schema: "dev_lyogev"
# MAGIC       connect_retries: 5
# MAGIC       connect_timeout: 60
# MAGIC       threads: 10
# MAGIC EOT

# COMMAND ----------

import os
os.chdir('jaffle_shop')

# COMMAND ----------

# needed for local seed files
spark.conf.set("spark.hadoop.fs.default.name", "file:///")

# COMMAND ----------

import dbt.main

# COMMAND ----------

dbt.main.handle_and_check(['seed'])

# COMMAND ----------

# used to fix some weird bug
from dbt.logger import log_manager
log_manager._file_handler.reset()
dbt.main.handle_and_check(['run', '--full-refresh'])

# COMMAND ----------
