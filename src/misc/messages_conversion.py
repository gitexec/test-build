# import os
# from pyspark.sql import SQLContext
# from pyspark import SparkContext
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /Users/adityaallamraju/hadoop-install/ \
# mysql-connector-java-5.1.45/mysql-connector-java-5.1.45-bin.jar  pyspark-shell'
# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName('abc').getOrCreate()

# df = spark.read.format('jdbc').options(url='jdbc:mysql//10.92.32.3/exigo',
#                                        driver="com.mysql.jdbc.Driver",
#                                        dbtable='messages',
#                                        user='root',
#                                        password='icentris').load()
# df.write.json('gs://icentris-uploads/sql_export/messages.json')
