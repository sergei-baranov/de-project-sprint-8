import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC_IN = 'student.topic.cohort5.sergei_baranov_in'
TOPIC_OUT = 'student.topic.cohort5.sergei_baranov_out'

spark_master = 'local'
spark_app_name = "RestaurantSubscribeStreamingService"

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

postgresql_settings = {
    'user': 'master',
    'password': 'de-master-password',
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'marketing_companies',
    # 'user': 'student',
    # 'password': 'de-student',
}

kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-admin" password="de-kafka-admin-2022";',
}
kafka_bootstrap_servers = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .master(spark_master) \
    .appName(spark_app_name) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .options(**kafka_security_options) \
    .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
    .option('subscribe', TOPIC_IN) \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), nullable=True),
    StructField("adv_campaign_id", StringType(), nullable=True),
    StructField("adv_campaign_content", StringType(), nullable=True),
    StructField("adv_campaign_owner", StringType(), nullable=True),
    StructField("adv_campaign_owner_contact", StringType(), nullable=True),
    StructField("adv_campaign_datetime_start", LongType(), nullable=True),  # TimestampType
    StructField("adv_campaign_datetime_end", LongType(), nullable=True),  # TimestampType
    StructField("datetime_created", LongType(), nullable=True),  # TimestampType
])

# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
        .withColumn('key_str', col('key').cast(StringType())) \
        .withColumn('value_json', col('value').cast(StringType())) \
        .drop('key', 'value') \
        .withColumn('key', col('key_str')) \
        .withColumn('value', from_json(col('value_json'), incomming_message_schema)) \
        .drop('key_str', 'value_json') \
        .select(
            col('value.restaurant_id').alias('restaurant_id'),
            col('value.adv_campaign_id').alias('adv_campaign_id'),
            col('value.adv_campaign_content').alias('adv_campaign_content'),
            col('value.adv_campaign_owner').alias('adv_campaign_owner'),
            col('value.adv_campaign_owner_contact').alias('adv_campaign_owner_contact'),
            col('value.adv_campaign_datetime_start').alias('adv_campaign_datetime_start'),
            col('value.adv_campaign_datetime_end').alias('adv_campaign_datetime_end'),
            col('value.datetime_created').alias('datetime_created'),
        ) \
        .filter(
            (col("adv_campaign_datetime_start") <= current_timestamp_utc)
            & (col("adv_campaign_datetime_end") >= current_timestamp_utc)
        )

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://localhost:5432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'jovyan') \
        .option('password', 'jovyan') \
        .load()

# запускаем стриминг
if __name__ == "__main__":
    result = filtered_read_stream_df

    query = (result
             .writeStream
             .outputMode("append")
             .format("console")
             .option("truncate", False)
             .start())
    query.awaitTermination()
