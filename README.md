# Проект 8-го спринта


## Шаг 0: предсоздаём структуры в бд

```
vonbraun@VonBraun:/$ ssh ya_de_sprint10
Welcome to Ubuntu 20.04.3 LTS (GNU/Linux 5.4.0-97-generic x86_64)
...

yc-user@fhmn5v5fmqubhop4hm4e:~$ docker ps -a
...

yc-user@fhmn5v5fmqubhop4hm4e:~$ docker exec -it baadb23f4140
...

root@fhmn5v5fmqubhop4hm4e:/.utils# psql -h localhost -p 5432 -U jovyan -d de
Password for user jovyan: 
psql (13.8 (Ubuntu 13.8-1.pgdg20.04+1))
...

de=# \dt
Did not find any relations.
de=# 

de=# CREATE TABLE public.subscribers_feedback (
    id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);
CREATE TABLE
de=# 

de=# CREATE TABLE public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);
CREATE TABLE
de=# 

de=# INSERT INTO public.subscribers_restaurants (id, client_id, restaurant_id)
VALUES
(1,  '223e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
(2,  '323e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
(3,  '423e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
(4,  '523e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
(5,  '623e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
(6,  '723e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
(7,  '823e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000'),
(8,  '923e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001'),
(9,  '923e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174001'),
(10, '023e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000')
;
INSERT 0 10
de=#

```


## Шаг 1. Проверить работу потока.

пароль и путь к сертификату см. в теме "5. Работа с Kafka через kcat"

Сначала иду в терминал, в котором буду ОТПРАВЛЯТЬ сообщение (чтобы топик создался)

```bash
vonbraun@VonBraun:/$ ssh ya_de_sprint10
Welcome to Ubuntu 20.04.3 LTS (GNU/Linux 5.4.0-97-generic x86_64)
...

yc-user@fhmn5v5fmqubhop4hm4e:~$ docker ps -a
...

yc-user@fhmn5v5fmqubhop4hm4e:~$ docker exec -it baadb23f4140
...

# читаю метадату по контейнеру
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -P -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username='de-student' -X sasl.password='***' -X ssl.ca.location=/usr/local/share/ca-certificates/*** -L

Metadata for all topics (from broker 3: sasl_ssl://rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091/3):
 5 brokers:
...

# отправляю сообщение
kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/*** \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}

# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils#
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/*** \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

Гоша:Хороший

# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/*** \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key777:Гоша Плохой

# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils# 

```

Теперь иду в соседний терминал, чтобы ЧИТАТЬ сообщение

```bash
vonbraun@VonBraun:/$ ssh ya_de_sprint10
Welcome to Ubuntu 20.04.3 LTS (GNU/Linux 5.4.0-97-generic x86_64)
...

yc-user@fhmn5v5fmqubhop4hm4e:~$ docker ps -a
...

yc-user@fhmn5v5fmqubhop4hm4e:~$ docker exec -it baadb23f4140
...

root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/*** \
-t student.topic.cohort5.sergei_baranov_in \
-C \
-o beginning
{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
% Reached end of topic student.topic.cohort5.sergei_baranov_in [0] at offset 1
Хороший
% Reached end of topic student.topic.cohort5.sergei_baranov_in [0] at offset 2
Гоша Плохой
% Reached end of topic student.topic.cohort5.sergei_baranov_in [0] at offset 3

```

**Всё получилось.**


## Шаг 2. Прочитать данные об акциях из Kafka.

1. Напишите с помощью PySpark код стриминга для:
   - чтения сообщений об акциях из Kafka;
   - вывода сообщений в консоль.
2. Протестируйте написанный код.

```bash
# ~/de-project-sprint-8/src/scripts/step2_read_kafka_campaigns.py
# читаю стрим из топика "student.topic.cohort5.sergei_baranov_in",
# вывожу на консоль

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# python3 ./step2_read_kafka_campaigns.py
...
-------------------------------------------
Batch: 0
-------------------------------------------
+---+-----+-----+---------+------+---------+-------------+
|key|value|topic|partition|offset|timestamp|timestampType|
+---+-----+-----+---------+------+---------+-------------+
+---+-----+-----+---------+------+---------+-------------+

```

иду в соседний терминал, отправляю сообщение

```bash

root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}
root@fhmn5v5fmqubhop4hm4e:/.utils# 

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils#

```

иду в первый терминал, с работающим приложением, и вижу:

```bash

-------------------------------------------
Batch: 1
-------------------------------------------
+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------+---------+------+-----------------------+-------------+
|key       |value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |topic                                  |partition|offset|timestamp              |timestampType|
+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------+---------+------+-----------------------+-------------+
|[6B 65 79]|[7B 22 72 65 73 74 61 75 72 61 6E 74 5F 69 64 22 3A 20 22 31 32 33 65 34 35 36 37 2D 65 38 39 62 2D 31 32 64 33 2D 61 34 35 36 2D 34 32 36 36 31 34 31 37 34 30 30 30 22 2C 22 61 64 76 5F 63 61 6D 70 61 69 67 6E 5F 69 64 22 3A 20 22 31 32 33 65 34 35 36 37 2D 65 38 39 62 2D 31 32 64 33 2D 61 34 35 36 2D 34 32 36 36 31 34 31 37 34 30 30 33 22 2C 22 61 64 76 5F 63 61 6D 70 61 69 67 6E 5F 63 6F 6E 74 65 6E 74 22 3A 20 22 66 69 72 73 74 20 63 61 6D 70 61 69 67 6E 22 2C 22 61 64 76 5F 63 61 6D 70 61 69 67 6E 5F 6F 77 6E 65 72 22 3A 20 22 49 76 61 6E 6F 76 20 49 76 61 6E 20 49 76 61 6E 6F 76 69 63 68 22 2C 22 61 64 76 5F 63 61 6D 70 61 69 67 6E 5F 6F 77 6E 65 72 5F 63 6F 6E 74 61 63 74 22 3A 20 22 69 69 69 76 61 6E 6F 76 40 72 65 73 74 61 75 72 61 6E 74 5F 69 64 22 2C 22 61 64 76 5F 63 61 6D 70 61 69 67 6E 5F 64 61 74 65 74 69 6D 65 5F 73 74 61 72 74 22 3A 20 31 36 35 39 32 30 33 35 31 36 2C 22 61 64 76 5F 63 61 6D 70 61 69 67 6E 5F 64 61 74 65 74 69 6D 65 5F 65 6E 64 22 3A 20 32 36 35 39 32 30 37 31 31 36 2C 22 64 61 74 65 74 69 6D 65 5F 63 72 65 61 74 65 64 22 3A 20 31 36 35 39 31 33 31 35 31 36 7D]|student.topic.cohort5.sergei_baranov_in|0        |3     |2023-01-25 23:15:45.195|0            |
+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------+---------+------+-----------------------+-------------+

```

**Приложение работает**


### Шаг 3. Прочитать данные о подписчиках из Postgres.

1. Дополните код чтением статичных данных из таблицы Postgres
`subscribers_restaurants` и выводом содержимого на консоль.
2. Протестируйте написанный код.

```bash
# ~/de-project-sprint-8/src/scripts/step3_read_postgres_subscribers.py
# читаю dataframe из таблицы "`subscribers_restaurants`",
# вывожу на консоль

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# python3 ./step3_read_postgres_subscribers.py
23/01/25 23:40:05 WARN Utils: Your hostname, fhmn5v5fmqubhop4hm4e resolves to a loopback address: 127.0.1.1; using 10.128.0.20 instead (on interface eth0)
...

To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
+---+--------------------+--------------------+                                 
| id|           client_id|       restaurant_id|
+---+--------------------+--------------------+
|  1|223e4567-e89b-12d...|123e4567-e89b-12d...|
|  2|323e4567-e89b-12d...|123e4567-e89b-12d...|
|  3|423e4567-e89b-12d...|123e4567-e89b-12d...|
|  4|523e4567-e89b-12d...|123e4567-e89b-12d...|
|  5|623e4567-e89b-12d...|123e4567-e89b-12d...|
|  6|723e4567-e89b-12d...|123e4567-e89b-12d...|
|  7|823e4567-e89b-12d...|123e4567-e89b-12d...|
|  8|923e4567-e89b-12d...|123e4567-e89b-12d...|
|  9|923e4567-e89b-12d...|123e4567-e89b-12d...|
| 10|023e4567-e89b-12d...|123e4567-e89b-12d...|
+---+--------------------+--------------------+

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# 

```

**Приложение работает**


## Шаг 4. Преобразование JSON в датафейм. 

Сообщения из Kafka находятся в формате переменных «ключ-значение» (англ. “key-value”). В переменной `value` лежит содержимое JSON-файла, то есть текст в формате JSON. Вам же для работы нужен датафрейм, который в качестве колонок использует ключи JSON, поэтому придётся сначала десериализовать `value` в JSON, а затем преобразовать JSON в датафрейм.

1. Дополните код, чтобы он преобразовал JSON из `value` в датафрейм, где колонками будут ключи в формате JSON.
2. Отфильтруйте сообщения с рекламными кампаниями, для которых текущее время находится в промежутке между временем старта и временем окончания кампании.
3. Протестируйте написанный код.

```bash
# ~/de-project-sprint-8/src/scripts/step4_kafka_campaigns_filtered.py
# читаю стрим, вывожу поля из value выше, фильтрую
# вывожу на консоль

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# python3 ./step4_kafka_campaigns_filtered.py
...

-------------------------------------------
Batch: 0
-------------------------------------------
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+
|restaurant_id|adv_campaign_id|adv_campaign_content|adv_campaign_owner|adv_campaign_owner_contact|adv_campaign_datetime_start|adv_campaign_datetime_end|datetime_created|
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+


```

Иду в соседний терминал, отправляю сообщение заведомо проходящее по дате акции

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1,"adv_campaign_datetime_end": 9999999999,"datetime_created": 9999999999}

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils# 
```

Иду в терминал с приложением, вижу:

```bash
-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+
|restaurant_id                       |adv_campaign_id                     |adv_campaign_content|adv_campaign_owner   |adv_campaign_owner_contact|adv_campaign_datetime_start|adv_campaign_datetime_end|datetime_created|
+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |
+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+

```

Отлично.
Теперь отправим сообщение с заведомо устаревшим периодом акции:

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1,"adv_campaign_datetime_end": 777,"datetime_created": 9999999999}

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils# 
```

Иду в терминал с приложением, вижу:

```bash
Batch: 2
-------------------------------------------
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+
|restaurant_id|adv_campaign_id|adv_campaign_content|adv_campaign_owner|adv_campaign_owner_contact|adv_campaign_datetime_start|adv_campaign_datetime_end|datetime_created|
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+

```

**Приложение работает**


## Шаг 5. Провести JOIN потоковых и статичных данных.

1. Дополните код, чтобы сджойнить данные из Kafka с данными из Postgres по полю `restaurant_id`.
2. Добавьте колонку с датой создания — текущей датой. В датафрейме не должно быть повторяющихся колонок.
3. Протестируйте написанный код.

```bash
# ~/de-project-sprint-8/src/scripts/step5_joined.py
# читаю стрим, вывожу поля из value выше, фильтрую,
# джойню с ресторанами по ид ресторана по inner-у,
# оставляю поля из стрима и client_id из постгреса,
# добавляю trigger_datetime_created,
# вывожу на консоль

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# python3 step5_joined.py
...

-------------------------------------------
Batch: 0
-------------------------------------------
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+---------+------------------------+
|restaurant_id|adv_campaign_id|adv_campaign_content|adv_campaign_owner|adv_campaign_owner_contact|adv_campaign_datetime_start|adv_campaign_datetime_end|datetime_created|client_id|trigger_datetime_created|
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+---------+------------------------+
+-------------+---------------+--------------------+------------------+--------------------------+---------------------------+-------------------------+----------------+---------+------------------------+
```

Иду в соседний терминал, отправляю сообщение заведомо проходящее по дате акции

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1,"adv_campaign_datetime_end": 9999999999,"datetime_created": 9999999999}

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils# 
```

Иду в терминал с приложением, вижу:

```bash
-------------------------------------------                                     
Batch: 1
-------------------------------------------
+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+------------------------------------+------------------------+
|restaurant_id                       |adv_campaign_id                     |adv_campaign_content|adv_campaign_owner   |adv_campaign_owner_contact|adv_campaign_datetime_start|adv_campaign_datetime_end|datetime_created|client_id                           |trigger_datetime_created|
+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+------------------------------------+------------------------+
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |223e4567-e89b-12d3-a456-426614174000|1674694338              |
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |323e4567-e89b-12d3-a456-426614174000|1674694338              |
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |423e4567-e89b-12d3-a456-426614174000|1674694338              |
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |523e4567-e89b-12d3-a456-426614174000|1674694338              |
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |623e4567-e89b-12d3-a456-426614174000|1674694338              |
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |723e4567-e89b-12d3-a456-426614174000|1674694338              |
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |823e4567-e89b-12d3-a456-426614174000|1674694338              |
|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant_id    |1                          |9999999999               |9999999999      |023e4567-e89b-12d3-a456-426614174000|1674694338              |
+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+------------------------------------+------------------------+
```

**Приложение работает**


## Шаг 6. Отправить результаты JOIN в Postgres для аналитики фидбэка.


Вам нужно отправлять результаты обработки сразу в два стока — Kafka для push-уведомлений и Postgres для аналитики фидбэка.
В обоих случаях вам потребуется метод `foreachBatch(…)`.
C его помощью вы можете указать, какие функции нужно применить к каждому микробатчу выходных данных.

Начнём с Postgres. Дополните код функцией, которая будет отправлять данные в Postgres для получения фидбэка. Для этого:

1. Измените код в теле функции, чтобы записать df в Postgres с полем `feedback`. Данные нужно записывать в локальную БД, для которой вы создавали таблицу `subscribers_feedback`. Параметры подключения:
- адрес подключения — `localhost:5432`
- логин/пароль — `jovyan/jovyan`

2. Доработайте запуск стриминга методом `foreachBatch(…)`, который будет вызывать функцию.
3. Протестируйте написанный код.

```
# ~/de-project-sprint-8/src/scripts/step6_foreach_postgres.py
# foreachBatch(), в нём запись в постгрес, в `subscribers_feedback`

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# python3 step6_foreach_postgres.py
...
```

Иду в соседний терминал, отправляю сообщение заведомо проходящее по дате акции

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1,"adv_campaign_datetime_end": 9999999999,"datetime_created": 9999999999}

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils# 
```

и теперь в psql проверяем, что там у нас в бд

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# psql -h localhost -p 5432 -U jovyan
Password for user jovyan: 
psql (13.8 (Ubuntu 13.8-1.pgdg20.04+1))
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, bits: 256, compression: off)
Type "help" for help.

jovyan=# \c de
SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, bits: 256, compression: off)
You are now connected to database "de" as user "jovyan".
de=# select count(1) from public.subscribers_feedback
de-# ;
 count 
-------
     8
(1 row)

de=# select * from public.subscribers_feedback;
 id |            restaurant_id             |           adv_campaign_id            | adv_campaign_content |  adv_campaign_owner   |
 adv_campaign_owner_contact | adv_campaign_datetime_start | adv_campaign_datetime_end | datetime_created |              client_id 
              | trigger_datetime_created | feedback 
----+--------------------------------------+--------------------------------------+----------------------+-----------------------+
----------------------------+-----------------------------+---------------------------+------------------+------------------------
--------------+--------------------------+----------
  1 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Ivanovich |
 iiivanov@restaurant_id     |                           1 |                9999999999 |       9999999999 | 223e4567-e89b-12d3-a456
-426614174000 |               1674696463 | 
  2 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Ivanovich |
 iiivanov@restaurant_id     |                           1 |                9999999999 |       9999999999 | 323e4567-e89b-12d3-a456
-426614174000 |               1674696463 | 
...
(8 rows)

de=# quit

```

теперь ещё одно сообщение

```
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "sss-eee","adv_campaign_content": "another campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1,"adv_campaign_datetime_end": 9999999999,"datetime_created": 9999999999}

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils#
...
```

и опять в постгресе смотрим

```
root@fhmn5v5fmqubhop4hm4e:/.utils# psql -h localhost -p 5432 -d de -U jovyan
Password for user jovyan: 
psql (13.8 (Ubuntu 13.8-1.pgdg20.04+1))
...

de=# select count(1) from public.subscribers_feedback;
 count 
-------
    16
(1 row)

de=# select * from public.subscribers_feedback;
 id |            restaurant_id             |           adv_campaign_id            | adv_campaign_content |  adv_campaign_owner   |
 adv_campaign_owner_contact | adv_campaign_datetime_start | adv_campaign_datetime_end | datetime_created |              client_id 
              | trigger_datetime_created | feedback 
----+--------------------------------------+--------------------------------------+----------------------+-----------------------+
----------------------------+-----------------------------+---------------------------+------------------+------------------------
--------------+--------------------------+----------
...
  8 | 123e4567-e89b-12d3-a456-426614174000 | 123e4567-e89b-12d3-a456-426614174003 | first campaign       | Ivanov Ivan Ivanovich |
 iiivanov@restaurant_id     |                           1 |                9999999999 |       9999999999 | 023e4567-e89b-12d3-a456
-426614174000 |               1674696463 | 
  9 | 123e4567-e89b-12d3-a456-426614174000 | sss-eee                              | another campaign     | Ivanov Ivan Ivanovich |
 iiivanov@restaurant_id     |                           1 |                9999999999 |       9999999999 | 223e4567-e89b-12d3-a456
-426614174000 |               1674696463 | 
...
de=# quit
root@fhmn5v5fmqubhop4hm4e:/.utils# 


```

**Приложение работает**


## Шаг 7. Отправить данные, сериализованные в формат JSON, в Kafka для push-уведомлений.

Дополните функцию из прошлого пункта, чтобы добавить ещё один сток — Kafka. 

1. Создайте датафрейм для отправки в Kafka.
2. Сериализуйте данные в сообщение формата «ключ-значение» и заполните только `value`. Для этого сериализуйте данные из датафрейма в JSON и положите JSON в колонку `value`.
3. Отправьте сообщения в результирующий топик Kafka без поля `feedback`.
4. В вызове `foreachBatch(...)` ничего менять не нужно.
5. Протестируйте написанный код.

сначала надо создать топик `student.topic.cohort5.sergei_baranov_out`

```bash
ОНО ЕГО НЕ СОЗДАЁТ

возможно потому, что уже есть топик `student.topic.cohort5.sergei_baranov.out`

!!! меняю значение в константе в приложении на `student.topic.cohort5.sergei_baranov.out`
```

далее запускаем приложение

```bash
# ~/de-project-sprint-8/src/scripts/step7_foreach_all.py
# foreachBatch(), в нём запись и в постгрес,
# и в Kafka топик `student.topic.cohort5.sergei_baranov.out`
# NB: static df: write(), no trigger(), no start() but save()

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# python3 step7_foreach_all.py
...
```

далее отправляем сообщение стандартно уже

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "sss-eee","adv_campaign_content": "another campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1,"adv_campaign_datetime_end": 9999999999,"datetime_created": 9999999999}

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils#
```

далее откроем ещё один терминал - читать `student.topic.cohort5.sergei_baranov.out`

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov.out \
-C \
-o beginning
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"223e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"323e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"423e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"523e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"623e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"723e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"823e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999999,"datetime_created":9999999999,"client_id":"023e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700011}
% Reached end of topic student.topic.cohort5.sergei_baranov.out [0] at offset 8747
```

**УРА**


## Шаг 8. Персистентность датафрейма.

1. Сохраните датафрейм в памяти после объединения данных.
2. После отправки данных в два стока очистите память от датафрейма.
3. Протестируйте написанный код.

```bash
# ~/de-project-sprint-8/src/scripts/project.py
# всё то же, с persist() и unpersist()

root@fhmn5v5fmqubhop4hm4e:/de-project-sprint-8/src/scripts# python3 project.py
...

```

отправляем сообщение

```bash
root@fhmn5v5fmqubhop4hm4e:/.utils# kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username=de-student \
-X sasl.password=*** \
-X ssl.ca.location=/usr/local/share/ca-certificates/Yandex/YandexCA.crt \
-t student.topic.cohort5.sergei_baranov_in \
-K: \
-P

# Enter

key:{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "sss-eee","adv_campaign_content": "another campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant_id","adv_campaign_datetime_start": 1,"adv_campaign_datetime_end": 9999999777,"datetime_created": 9999999888}

# Enter
# Ctrl+D, чтобы отправить сообщение

root@fhmn5v5fmqubhop4hm4e:/.utils#
```

читать `student.topic.cohort5.sergei_baranov.out` - смотрим открытый уже на предыдущем шаге терминал

```bash
...
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999777,"datetime_created":9999999888,"client_id":"823e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700321}
{"restaurant_id":"123e4567-e89b-12d3-a456-426614174000","adv_campaign_id":"sss-eee","adv_campaign_content":"another campaign","adv_campaign_owner":"Ivanov Ivan Ivanovich","adv_campaign_owner_contact":"iiivanov@restaurant_id","adv_campaign_datetime_start":1,"adv_campaign_datetime_end":9999999777,"datetime_created":9999999888,"client_id":"023e4567-e89b-12d3-a456-426614174000","trigger_datetime_created":1674700321}
% Reached end of topic student.topic.cohort5.sergei_baranov.out [0] at offset 8755

```

**КАЖЕТСЯ, ВУАЛЯ**
