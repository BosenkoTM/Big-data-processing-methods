# Лабораторная работа № 6. Распределённое машинное обучение на Spark MLlib и исследование робототехнических датасетов

* **Оригинальные репозитории:**
  * Susan Li — `susanli2016/PySpark-and-MLlib`: https://github.com/susanli2016/PySpark-and-MLlib
  * MLlib notebook: https://github.com/susanli2016/PySpark-and-MLlib/blob/master/Machine%20Learning%20PySpark%20and%20MLlib.ipynb
  * Hugging Face — `huggingface/lerobot`: https://github.com/huggingface/lerobot
  * LeRobot quickstart notebook: https://github.com/huggingface/lerobot/blob/main/examples/notebooks/quickstart.ipynb
  * Google DeepMind — `google-deepmind/open_x_embodiment`: https://github.com/google-deepmind/open_x_embodiment
  * Open X-Embodiment dataset Colab: https://github.com/google-deepmind/open_x_embodiment/blob/main/colabs/Open_X_Embodiment_Datasets.ipynb
* **Цель работы:** построить воспроизводимый MLlib Pipeline классификации технического состояния робота и исследовать структуру современных крупномасштабных робототехнических датасетов.
* **Инструменты:** `Google Colab`, `PySpark 4.2.0`, `Spark MLlib`, `RandomForestClassifier`; дополнительный блок — `LeRobotDataset`, Open X-Embodiment.
* **Этап пайплайна:** `Learn -> Teach`.
* **Входные данные:** основной ML-набор генерируется внутри Colab. Для исследования реального robotics dataset используется публичный `lerobot/pusht`; описание инструментов: https://huggingface.co/docs/lerobot/using_dataset_tools

### 1. Минимальные теоретические сведения

Spark MLlib использует единый Pipeline API. **Transformer** преобразует DataFrame методом `transform()`, **Estimator** обучается методом `fit()` и возвращает Transformer-модель. В данной работе `VectorAssembler` является Transformer, `StandardScaler` — Estimator, а `RandomForestClassifier` — Estimator.

Для бинарной классификации используется F1:

$$
F_1=
2\cdot\frac{Precision\cdot Recall}{Precision+Recall}.
$$

Разделение train/test выполняется до обучения. Для реальных робототехнических datasets единицей анализа может быть frame, transition или episode; LeRobot стандартизует состояние, действие и мультимодальные наблюдения, а Open X-Embodiment объединяет множество наборов в унифицированной episode-oriented форме.

### 2. Исходный код (Google Colab)

```python
!pip -q install "pyspark==4.2.0" pandas numpy

import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab06_MLlibRobotics")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

# 1. Генерация размеченной телеметрии.
rng = np.random.default_rng(7)
N = 12_000

temperature = rng.normal(47, 3.0, N)
vibration_rms = np.abs(rng.normal(0.22, 0.07, N))
battery_v = rng.normal(12.0, 0.35, N)
current_a = np.abs(rng.normal(2.8, 0.8, N))
accel_rms = rng.normal(9.82, 0.18, N)

risk = (
    0.40 * (temperature > 52).astype(float)
    + 0.35 * (vibration_rms > 0.34).astype(float)
    + 0.20 * (battery_v < 11.5).astype(float)
    + 0.15 * (current_a > 4.0).astype(float)
    + rng.normal(0, 0.08, N)
)

label = (risk > 0.42).astype(int)

pdf = pd.DataFrame({
    "temperature": temperature,
    "vibration_rms": vibration_rms,
    "battery_v": battery_v,
    "current_a": current_a,
    "accel_rms": accel_rms,
    "label": label
})

data = spark.createDataFrame(pdf)
train, test = data.randomSplit([0.8, 0.2], seed=42)

# 2. Pipeline: Transformer -> Estimator -> Transformer -> Estimator.
assembler = VectorAssembler(
    inputCols=["temperature", "vibration_rms", "battery_v", "current_a", "accel_rms"],
    outputCol="raw_features"
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True
)

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=80,
    maxDepth=8,
    seed=42
)

pipeline = Pipeline(stages=[assembler, scaler, rf])

model = pipeline.fit(train)
pred = model.transform(test)

evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1"
)

evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

f1 = evaluator_f1.evaluate(pred)
acc = evaluator_acc.evaluate(pred)

print(f"F1       = {f1:.4f}")
print(f"Accuracy = {acc:.4f}")

pred.select("temperature", "vibration_rms", "battery_v", "label", "prediction", "probability").show(15, truncate=False)

rf_model = model.stages[-1]
print("Feature importances:", rf_model.featureImportances)

assert f1 > 0.75

# 3. Необязательная Colab-ячейка исследования современного robotics dataset.
# Выполняется отдельно, т.к. установка LeRobot включает дополнительные зависимости.
#
# !pip -q install "lerobot[dataset]"
# from lerobot.datasets import LeRobotDataset
# robot_ds = LeRobotDataset("lerobot/pusht")
# print("Episodes:", robot_ds.meta.total_episodes)
# print("Frames:", robot_ds.meta.total_frames)
# print("Features:", list(robot_ds.meta.features.keys()))
#
# Официальный Colab LeRobot:
# https://github.com/huggingface/lerobot/blob/main/examples/notebooks/quickstart.ipynb
#
# Официальный Open X-Embodiment Colab:
# https://github.com/google-deepmind/open_x_embodiment/blob/main/colabs/Open_X_Embodiment_Datasets.ipynb
```

### 3. Ход работы

1. Сгенерируйте размеченный набор телеметрии не менее чем из 10 000 объектов и выполните train/test split.
2. Создайте Pipeline `VectorAssembler -> StandardScaler -> RandomForestClassifier`.
3. Обучите модель и вычислите `F1` и `Accuracy`.
4. Проанализируйте `featureImportances` и соотнесите их с правилом генерации неисправности.
5. Измените минимум один гиперпараметр (`numTrees`, `maxDepth`) и сравните F1.
6. Откройте официальный LeRobot quickstart или загрузите `LeRobotDataset("lerobot/pusht")`; зафиксируйте число episodes/frames/features. Для альтернативы изучите официальный Open X-Embodiment Colab.
7. Сформулируйте, какие признаки из реального robotics dataset можно агрегировать в Spark и какие данные нецелесообразно переносить в Spark MLlib (например, исходное видео для deep learning).

### 4. Критерии оценки (ровно 10 баллов)

* **2 балла** — корректно подготовлены признаки, разметка и train/test split.
* **2 балла** — корректно реализован и обучен MLlib Pipeline с Transformer/Estimator.
* **2 балла** — рассчитаны F1/Accuracy и проведён эксперимент с гиперпараметром.
* **2 балла** — исследован LeRobot или Open X-Embodiment и сделан технический вывод о структуре данных/масштабировании.
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

