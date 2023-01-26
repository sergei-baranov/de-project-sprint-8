import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC_IN = 'student.topic.cohort5.sergei_baranov_in'
TOPIC_OUT = 'student.topic.cohort5.sergei_baranov.out'

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
    'user': 'jovyan',
    'password': 'jovyan',
    'driver': 'org.postgresql.Driver',
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
        .option('driver', postgresql_settings['driver']) \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', postgresql_settings['user']) \
        .option('password', postgresql_settings['password']) \
        .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
# Добавляем время создания события.
result_df = filtered_read_stream_df \
    .join(subscribers_restaurant_df,
          subscribers_restaurant_df.restaurant_id == filtered_read_stream_df.restaurant_id, how='inner') \
    .select(
        filtered_read_stream_df.restaurant_id,
        filtered_read_stream_df.adv_campaign_id,
        filtered_read_stream_df.adv_campaign_content,
        filtered_read_stream_df.adv_campaign_owner,
        filtered_read_stream_df.adv_campaign_owner_contact,
        filtered_read_stream_df.adv_campaign_datetime_start,
        filtered_read_stream_df.adv_campaign_datetime_end,
        filtered_read_stream_df.datetime_created,
        subscribers_restaurant_df.client_id,
    ) \
    .withColumn('trigger_datetime_created', lit(current_timestamp_utc))


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    # ...
    # записываем df в PostgreSQL с полем feedback
    df.persist()
    # static df, so write()
    df_postgres = df.withColumn('feedback', lit(''))
    df_postgres \
        .write \
        .jdbc(url='jdbc:postgresql://localhost:5432/de',
              table="subscribers_feedback", mode="append", properties=postgresql_settings)
    # создаём df для отправки в Kafka. Сериализация в json.
    df_kafka = df \
        .withColumn('value', to_json(struct(
            col('restaurant_id'),
            col('adv_campaign_id'),
            col('adv_campaign_content'),
            col('adv_campaign_owner'),
            col('adv_campaign_owner_contact'),
            col('adv_campaign_datetime_start'),
            col('adv_campaign_datetime_end'),
            col('datetime_created'),
            col('client_id'),
            col('trigger_datetime_created'),
        ))) \
        .select('value')
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_kafka.write.format("kafka") \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .options(**kafka_security_options) \
        .option("topic", TOPIC_OUT) \
        .option("truncate", False) \
        .save()
    # очищаем память от df
    df.unpersist()


# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
