# Лабораторная работа № 1. Распределённая пакетная обработка логов робототехнических комплексов в Apache Spark

* **Оригинальные репозитории:**
  * Pierre Navaro — `pnavaro/big-data`: https://github.com/pnavaro/big-data
  * PySpark notebook: https://github.com/pnavaro/big-data/blob/master/notebooks/15-PySpark.ipynb
  * Giuseppe Tolomei — `gtolomei/big-data-computing`: https://github.com/gtolomei/big-data-computing
  * PySpark tutorial: https://github.com/gtolomei/big-data-computing/blob/master/notebooks/PySpark_Tutorial.ipynb
* **Цель работы:** освоить пакетную обработку большого журнала робототехнической телеметрии средствами `PySpark`, сравнить DataFrame API и Spark SQL, исследовать партиционирование, lazy evaluation и shuffle.
* **Инструменты:** `Google Colab`, `Python 3`, `PySpark 4.2.0`, режим `local[*]`, Spark SQL, DataFrame API.
* **Этап пайплайна:** `Collect -> Store -> Process`.
* **Входные данные:** генерируются в ноутбуке; внешний датасет не требуется.

### 1. Минимальные теоретические сведения

RDD является распределённой неизменяемой коллекцией, а DataFrame — распределённой таблицей со схемой, для которой Spark может строить оптимизированный logical/physical plan. Вычисления ленивые: `select`, `filter`, `groupBy` формируют lineage, а `count`, `show`, `write` запускают job. При агрегации по ключу записи должны быть перераспределены между partition; эта стадия называется **shuffle** и связана с сетевым/дисковым I/O в настоящем кластере.

MapReduce-представление агрегации:

$$
\operatorname{map}(k_1,v_1)\rightarrow[(k_2,v_2)],
$$

$$
\operatorname{reduce}(k_2,[v_2])\rightarrow v_3.
$$

Для оценки вибрации/ускорения используется RMS:

$$
x_{RMS}=\sqrt{\frac{1}{N}\sum_{i=1}^N x_i^2}.
$$

### 2. Исходный код (Google Colab)

```python
!pip -q install "pyspark==4.2.0" pandas pyarrow

from pyspark.sql import SparkSession, functions as F
from pyspark.storagelevel import StorageLevel
import time

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab01_RobotBatchProcessing")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

print("Spark:", spark.version)
print("Default parallelism:", spark.sparkContext.defaultParallelism)

# 1. Генерация большого журнала телеметрии без внешнего датасета.
N = 500_000

logs = (
    spark.range(N)
    .withColumn("robot_id", F.concat(F.lit("robot-"), (F.col("id") % 8).cast("string")))
    .withColumn("sensor_id", F.concat(F.lit("imu-"), (F.col("id") % 4).cast("string")))
    .withColumn("ts_ms", (F.lit(1_720_000_000_000) + F.col("id") * 50).cast("long"))
    .withColumn("ax", F.sin(F.col("id") / 25.0) + ((F.col("id") % 7) - 3) * 0.01)
    .withColumn("ay", F.cos(F.col("id") / 40.0) + ((F.col("id") % 5) - 2) * 0.01)
    .withColumn("az", F.lit(9.81) + F.sin(F.col("id") / 60.0) * 0.08)
    .withColumn("motor_temp", F.lit(42.0) + (F.col("id") % 100) * 0.12)
    .withColumn("battery_v", F.lit(12.6) - (F.col("id") % 1000) * 0.0008)
    .drop("id")
    .repartition(8, "robot_id")
)

print("Partitions:", logs.rdd.getNumPartitions())
logs.show(5, truncate=False)

# 2. Кэширование набора: повторные аналитические запросы не должны
#    заново строить весь lineage.
logs.persist(StorageLevel.MEMORY_AND_DISK)
_ = logs.count()

# 3. MapReduce-подобная агрегация: key = robot_id.
t0 = time.perf_counter()

summary = (
    logs.groupBy("robot_id")
    .agg(
        F.count("*").alias("records"),
        F.avg("motor_temp").alias("avg_motor_temp"),
        F.max("motor_temp").alias("max_motor_temp"),
        F.avg("battery_v").alias("avg_battery_v"),
        F.sqrt(F.avg(F.pow("ax", 2) + F.pow("ay", 2) + F.pow("az", 2))).alias("accel_rms")
    )
    .orderBy("robot_id")
)

summary.show(truncate=False)
elapsed = time.perf_counter() - t0
print(f"Aggregation time: {elapsed:.3f} s")

# 4. Spark SQL над тем же DataFrame.
logs.createOrReplaceTempView("robot_logs")

sql_result = spark.sql("""
SELECT
    robot_id,
    COUNT(*) AS n,
    ROUND(AVG(motor_temp), 3) AS avg_temp,
    ROUND(MAX(motor_temp), 3) AS max_temp,
    ROUND(AVG(battery_v), 4) AS avg_voltage
FROM robot_logs
GROUP BY robot_id
HAVING MAX(motor_temp) > 50
ORDER BY max_temp DESC
""")

sql_result.show(truncate=False)

# 5. План выполнения: найти Exchange/HashAggregate и обсудить shuffle.
summary.explain(mode="formatted")

# 6. Проверка распределения записей по partition.
partition_sizes = (
    logs.rdd
    .mapPartitionsWithIndex(lambda idx, it: [(idx, sum(1 for _ in it))])
    .collect()
)
print("Partition sizes:", partition_sizes)

assert sum(x[1] for x in partition_sizes) == N
assert summary.count() == 8

logs.unpersist()
```

### 3. Ход работы

1. Откройте Colab, выполните установочную ячейку и создайте `SparkSession` в режиме `local[*]`.
2. Сгенерируйте 500 000 записей телеметрии восьми роботов; зафиксируйте число partition.
3. Выполните `persist(MEMORY_AND_DISK)` и материализуйте DataFrame действием `count()`.
4. Рассчитайте число записей, среднюю/максимальную температуру, напряжение и RMS ускорения по каждому `robot_id`.
5. Повторите часть аналитики через Spark SQL.
6. Выполните `explain(mode="formatted")`; найдите операции `Exchange` и `HashAggregate`/`ObjectHashAggregate` и объясните их назначение.
7. Измените число partition на `2`, `4`, `8`, повторите агрегацию и сравните время на одном и том же Colab runtime.

### 4. Критерии оценки (ровно 10 баллов)

* **2 балла** — корректно создан `SparkSession`, сгенерирован набор и показано фактическое партиционирование.
* **2 балла** — корректно выполнена DataFrame-агрегация с расчётом RMS и температурных показателей.
* **2 балла** — эквивалентная аналитика выполнена через Spark SQL; результаты интерпретированы.
* **2 балла** — исследован physical plan и выполнено сравнение минимум двух вариантов числа partition.
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

