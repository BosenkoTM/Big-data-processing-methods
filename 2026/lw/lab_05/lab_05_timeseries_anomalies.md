# Лабораторная работа № 5. Временные ряды телеметрии и алгоритмы обнаружения аномалий

* **Оригинальные репозитории:**
  * InfluxData Community — `InfluxCommunity/Notebooks`: https://github.com/InfluxCommunity/Notebooks
  * Striim Labs — `striim-labs/lstm-autoencoder-spark-kafka`: https://github.com/striim-labs/lstm-autoencoder-spark-kafka
* **Цель работы:** научиться выделять аномальные состояния робототехнической телеметрии по временным признакам, используя Z-score и скользящий RMS; выполнить расчёт оконных признаков в Spark.
* **Инструменты:** `Google Colab`, `PySpark 4.2.0`, Spark Window API, `NumPy`, `Pandas`, `Plotly`.
* **Этап пайплайна:** `Store -> Process -> Learn`.
* **Входные данные:** синтетический временной ряд генерируется в notebook; примеры открытых time-series notebooks и sample data: https://github.com/InfluxCommunity/Notebooks/tree/master/Anomaly_Detection

### 1. Минимальные теоретические сведения

Для ряда $x_i$ стандартный Z-score равен

$$
z_i=\frac{x_i-\mu}{\sigma}.
$$

Типовое правило $|z_i|\ge 3$ выделяет сильные отклонения относительно базового распределения. Для вибрационного сигнала удобно применять RMS на скользящем окне длины $N$:

$$
RMS_t=
\sqrt{\frac{1}{N}\sum_{i=t-N+1}^t x_i^2}.
$$

Глобальный Z-score и rolling RMS обнаруживают разные типы нарушений: температурный выброс и длительный рост энергии вибрации. Для production-систем пороги калибруются на нормальном режиме и проверяются по precision/recall, а не задаются произвольно.

### 2. Исходный код (Google Colab)

```python
!pip -q install "pyspark==4.2.0" pandas numpy plotly

import numpy as np
import pandas as pd
import plotly.express as px

from pyspark.sql import SparkSession, functions as F, Window

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab05_TimeSeriesAnomalyDetection")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# 1. Синтетическая телеметрия нескольких роботов.
rng = np.random.default_rng(42)
robots = ["robot-0", "robot-1", "robot-2"]
n_per_robot = 1800  # 30 минут при 1 Гц

frames = []
start = pd.Timestamp("2026-08-31T09:00:00Z")

for ridx, robot in enumerate(robots):
    t = np.arange(n_per_robot)
    temperature = 44 + ridx + 0.002 * t + rng.normal(0, 0.25, n_per_robot)
    vibration = 0.20 + 0.03 * np.sin(t / 15) + rng.normal(0, 0.015, n_per_robot)

    # Вставка аномалий.
    temperature[850:870] += 10
    vibration[1200:1225] += 0.75

    frames.append(pd.DataFrame({
        "robot_id": robot,
        "timestamp": start + pd.to_timedelta(t, unit="s"),
        "temperature": temperature,
        "vibration": vibration,
    }))

pdf = pd.concat(frames, ignore_index=True)
sdf = spark.createDataFrame(pdf)

# 2. Z-score температуры внутри каждого робота.
w_robot = Window.partitionBy("robot_id")
sdf = (
    sdf
    .withColumn("temp_mean", F.avg("temperature").over(w_robot))
    .withColumn("temp_std", F.stddev_pop("temperature").over(w_robot))
    .withColumn("temp_z", (F.col("temperature") - F.col("temp_mean")) / F.col("temp_std"))
)

# 3. Скользящий RMS вибрации за последние 20 измерений.
w_roll = (
    Window.partitionBy("robot_id")
    .orderBy(F.col("timestamp").cast("long"))
    .rowsBetween(-19, 0)
)

sdf = sdf.withColumn(
    "vibration_rms",
    F.sqrt(F.avg(F.pow("vibration", 2)).over(w_roll))
)

# 4. Аномалия по двум независимым признакам.
sdf = (
    sdf
    .withColumn(
        "is_anomaly",
        (F.abs("temp_z") >= 3.0) | (F.col("vibration_rms") >= 0.45)
    )
)

anomalies = (
    sdf.filter("is_anomaly")
    .select("robot_id", "timestamp", "temperature", "temp_z", "vibration", "vibration_rms")
    .orderBy("robot_id", "timestamp")
)

print("Detected anomalies:", anomalies.count())
anomalies.show(20, truncate=False)

# 5. Визуализация.
plot_df = (
    sdf.select("robot_id", "timestamp", "temperature", "vibration_rms", "is_anomaly")
    .toPandas()
)

fig = px.line(
    plot_df,
    x="timestamp",
    y="vibration_rms",
    color="robot_id",
    title="Rolling RMS вибрации"
)
fig.show()

anom_pdf = plot_df[plot_df["is_anomaly"]]
fig2 = px.scatter(
    anom_pdf,
    x="timestamp",
    y="temperature",
    color="robot_id",
    title="События, классифицированные как аномальные"
)
fig2.show()

# 6. Сводка.
summary = (
    sdf.groupBy("robot_id")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("is_anomaly").cast("int")).alias("anomalies")
    )
    .withColumn("anomaly_rate", F.col("anomalies") / F.col("total"))
)
summary.show()

assert anomalies.count() > 0
```

### 3. Ход работы

1. Сгенерируйте не менее 30 минут телеметрии для трёх роботов и добавьте два разных типа аномалий.
2. Создайте Spark DataFrame и вычислите среднее/стандартное отклонение температуры отдельно по каждому `robot_id`.
3. Рассчитайте `temp_z`.
4. Создайте rolling window на 20 измерений и вычислите `vibration_rms`.
5. Сформируйте логическое правило аномалии и получите таблицу с обнаруженными событиями.
6. Постройте временные графики RMS и аномальных точек.
7. Измените пороги Z-score/RMS, сравните количество срабатываний и объясните компромисс false positive/false negative.

### 4. Критерии оценки (ровно 10 баллов)

* **2 балла** — корректно сформирован временной ряд с контролируемыми аномалиями.
* **2 балла** — корректно рассчитан Z-score по каждому роботу.
* **2 балла** — корректно рассчитан rolling RMS средствами Spark Window API.
* **2 балла** — выполнен анализ чувствительности порогов и построены информативные графики.
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

