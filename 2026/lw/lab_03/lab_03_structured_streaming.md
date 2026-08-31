# Лабораторная работа № 3. Потоковая обработка сенсорных событий в Spark Structured Streaming

* **Оригинальные репозитории:**
  * Sucharitha G — `sucharithag18/real-time-iot-air-quality-pipeline`: https://github.com/sucharithag18/real-time-iot-air-quality-pipeline
  * Guido K — `guidok91/spark-structured-streaming-kafka`: https://github.com/guidok91/spark-structured-streaming-kafka
  * Apache Spark: https://github.com/apache/spark
* **Цель работы:** реализовать Colab-safe потоковую обработку сенсорных событий с event time, sliding windows и watermark; понять, как тот же код переносится на Kafka source.
* **Инструменты:** `Google Colab`, `PySpark 4.2.0`, Spark Structured Streaming, встроенный `rate` source; Kafka рассматривается как production/source-адаптация.
* **Этап пайплайна:** `Stream -> Process`.
* **Входные данные:** поток генерируется встроенным Spark `rate` source; внешний Kafka broker не требуется.

### 1. Минимальные теоретические сведения

Structured Streaming представляет поток как неограниченную таблицу. **Event time** — время возникновения физического события, **processing time** — время обработки событием вычислительной системой. В беспроводной робототехнической сети события могут приходить не по порядку. Watermark задаёт допустимую задержку и ограничивает объём состояния, которое движок обязан хранить для старых окон.

Для sliding window длиной $W$ и шага $S$:

$$
W_k=[t_0+kS,\;t_0+kS+W).
$$

Задержка события:

$$
L=t_{processing}-t_{event}.
$$

Если $L$ превышает watermark и состояние соответствующего окна уже удалено, позднее событие может не повлиять на итоговую агрегацию.

### 2. Исходный код (Google Colab)

```python
!pip -q install "pyspark==4.2.0"

from pyspark.sql import SparkSession, functions as F
import time

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab03_StructuredStreaming")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# Colab-safe источник неограниченного потока.
# Он заменяет Kafka broker в обязательной части лабораторной,
# сохраняя семантику unbounded stream + event time + windows.
source = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 30)
    .option("numPartitions", 2)
    .load()
)

telemetry = (
    source
    .withColumn("robot_id", F.concat(F.lit("robot-"), (F.col("value") % 4).cast("string")))
    .withColumn(
        "event_time",
        F.when(
            F.col("value") % 17 == 0,
            F.col("timestamp") - F.expr("INTERVAL 20 SECONDS")
        ).otherwise(F.col("timestamp"))
    )
    .withColumn("motor_temp", F.lit(43.0) + (F.col("value") % 25).cast("double") * 0.55)
    .withColumn("vibration", F.abs(F.sin(F.col("value") / 8.0)))
    .select("robot_id", "event_time", "motor_temp", "vibration")
)

windowed = (
    telemetry
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        F.window("event_time", "10 seconds", "5 seconds"),
        "robot_id"
    )
    .agg(
        F.count("*").alias("events"),
        F.avg("motor_temp").alias("avg_temp"),
        F.max("motor_temp").alias("max_temp"),
        F.sqrt(F.avg(F.pow("vibration", 2))).alias("vibration_rms")
    )
)

query = (
    windowed.writeStream
    .format("memory")
    .queryName("robot_windows")
    .outputMode("update")
    .trigger(processingTime="1 second")
    .start()
)

# Накопить несколько micro-batch и корректно остановить query.
time.sleep(12)
query.processAllAvailable()
query.stop()

result = spark.sql("""
SELECT
    window.start AS window_start,
    window.end AS window_end,
    robot_id,
    events,
    ROUND(avg_temp, 2) AS avg_temp,
    ROUND(max_temp, 2) AS max_temp,
    ROUND(vibration_rms, 4) AS vibration_rms
FROM robot_windows
ORDER BY window_start DESC, robot_id
""")

result.show(40, truncate=False)
print("Rows in materialized streaming result:", result.count())

# Техническое задание: изменить watermark 10s -> 30s и сравнить
# число сохранённых опоздавших событий.
```

> **Адаптация к Kafka:** в промышленной версии `rate` заменяется на `spark.readStream.format("kafka")`; архитектурные примеры producer/consumer и Docker-инфраструктуры приведены в двух исходных репозиториях. Обязательная зачётная часть не требует внешнего broker, поэтому полностью воспроизводится в бесплатном Colab.

### 3. Ход работы

1. Запустите поток `rate` со скоростью 30 событий/с.
2. Сформируйте `robot_id`, температуру и вибрацию; для части событий искусственно сдвиньте `event_time` на 20 секунд назад.
3. Настройте watermark 10 секунд и sliding window `10 s / 5 s`.
4. Рассчитайте количество событий, среднюю/максимальную температуру и RMS вибрации в каждом окне.
5. Запустите query в `memory` sink на 10–15 секунд и корректно остановите его.
6. Повторите эксперимент с watermark `30 seconds` и сравните число/состав агрегатов.
7. Укажите, какие строки конфигурации необходимо заменить для чтения JSON-событий из Kafka.

### 4. Критерии оценки (ровно 10 баллов)

* **2 балла** — поток запускается и корректно формирует телеметрию с `event_time`.
* **2 балла** — реализованы sliding windows и агрегаты, включая RMS.
* **2 балла** — корректно применён watermark и проведён эксперимент минимум с двумя значениями.
* **2 балла** — объяснена замена `rate` на Kafka source и назначение offset/partition в целевой архитектуре.
* **2 балла** — корректно заполненная форма `TEACH CARD` (педагогическая адаптация инженерной задачи).


### 5. `TEACH CARD`

Заполните паспорт педагогической адаптации выполненной инженерной задачи.

| Поле | Содержание |
|---|---|
| **Название учебного проекта** |  |
| **Целевая аудитория** | 8–9 класс / 10–11 класс / кружок робототехники / СПО |
| **Исследовательский вопрос** |  |
| **Источник данных** |  |
| **Инженерная концепция, сохраняемая без упрощения** |  |
| **Что упрощается относительно университетской лабораторной** |  |
| **Алгоритм действий обучающегося (4–6 шагов)** |  |
| **Измеримый результат / метрика** |  |
| **Критерий успешного выполнения** |  |
| **Вариант усложнения для НТО / хакатона** |  |
| **Риски и ограничения** |  |

