# Лекция 4. Масштабируемая аналитика, машинное обучение и образовательные проекты

**Дисциплина:** «Методы обработки больших данных»  
**Направление:** 44.03.05 Педагогическое образование  
**Профиль:** «Информатика и дополнительное образование (робототехника)»  
**Этапы пайплайна:** `Learn -> Teach`

$$
\boxed{
\text{Sense}\rightarrow
\text{Collect}\rightarrow
\text{Stream}\rightarrow
\text{Store}\rightarrow
\text{Process}\rightarrow
\text{Learn}\rightarrow
\text{Teach}
}
$$

## 1. Тема и план лекции

1. Spark MLlib Pipeline API: `Transformer`, `Estimator`, признаки, разбиение train/test и утечка данных (`data leakage`).
2. Метрики классификации и регрессии, функции потерь.
3. Распределённое обучение с параллелизмом по данным (`data parallelism`) и стоимость синхронизации.
4. Крупные робототехнические наборы данных: Open X-Embodiment и Hugging Face LeRobot.
5. Потоковый доступ к наборам данных и проектирование школьных/олимпиадных проектов по данным.

После лекции студент должен уметь построить воспроизводимый ML-конвейер над телеметрией, выбрать адекватные метрики, объяснить синхронный параллелизм по данным и спроектировать педагогическую адаптацию сложного робототехнического конвейера данных.

> **Технологическая база на 31.08.2026:** Apache Spark 4.2.0; LeRobotDataset v3 поддерживает хранение нескольких эпизодов в Parquet/MP4 и потоковый доступ через `StreamingLeRobotDataset`. Версии библиотек в лабораторных работах следует фиксировать.

---

## 2. ML-конвейер

Корректная последовательность:

```text
raw telemetry
      |
validate / clean
      |
feature extraction
      |
train/test split
      |
fit preprocessing on train
      |
fit model
      |
evaluate on test
      |
deploy / monitor
```

Главное ограничение: операции, параметры которых оцениваются по данным, не должны использовать тестовую выборку на этапе обучения.

### Утечка данных (Data leakage)

Если среднее и стандартное отклонение для нормализации рассчитаны на полном наборе данных, то при $N=N_{train}+N_{test}$:

$$
\mu=
\frac{1}{N}
\sum_{i\in train\cup test}x_i,
$$

информация тестовой выборки участвует в предварительной обработке. В результате оценка качества может стать оптимистичной.

Корректно оценивать параметры преобразования только на обучающей выборке:

$$
\mu_{train}=
\frac{1}{N_{train}}
\sum_{i\in train}x_i.
$$

Затем уже обученный преобразователь применяется к `test` без повторной подгонки (`fit`).

---

## 3. Transformer и Estimator в Spark MLlib

**Transformer**

$$
T:D\rightarrow D'.
$$

Имеет:

```text
transform(dataset)
```

Примеры:

- `VectorAssembler`;
- обученная модель `StandardScalerModel`;
- обученная модель классификации или регрессии.

**Estimator**

$$
E:D\rightarrow T.
$$

Имеет:

```text
fit(dataset)
```

Примеры:

- `StandardScaler`;
- `LogisticRegression`;
- `RandomForestClassifier`.

```text
VectorAssembler
    |
    v
StandardScaler
  Estimator
    |
    v
ScalerModel
 Transformer
    |
    v
RandomForestClassifier
    |
    v
RandomForestClassificationModel
```

`Pipeline` сам является `Estimator`: вызов `fit()` последовательно обучает стадии, которым требуется подгонка, и формирует `PipelineModel` из готовых `Transformer`-стадий.

---

## 4. Проектирование признаков телеметрии

Окно сырого сигнала:

$$
x_1,\dots,x_N
$$

преобразуется в признаки:

$$
\mathbf{z}
=
[
\mu,
\sigma,
RMS,
min,
max,
slope,
entropy
]^T.
$$

Для робота:

```text
temperature_mean
temperature_max
vibration_rms
battery_drop_rate
current_peak
accel_rms
```

Модель должна получать признаки, имеющие физическую и статистическую интерпретацию.

---

## 5. Метрики классификации

```text
                 predicted
               0           1
actual 0       TN          FP
actual 1       FN          TP
```

Precision:

$$
Precision=
\frac{TP}{TP+FP}.
$$

Recall:

$$
Recall=
\frac{TP}{TP+FN}.
$$

F1:

$$
F_1=
2\cdot
\frac{Precision\cdot Recall}
{Precision+Recall}.
$$

Для диагностики отказов высокая `Recall` уменьшает число пропущенных неисправностей, а низкая `Precision` означает большое число ложных тревог (`false positives`).

### Macro-F1

Для $K$ классов:

$$
F_1^{macro}
=
\frac{1}{K}
\sum_{k=1}^{K}F_{1,k}.
$$

Macro-F1 полезна при несбалансированных классах, когда вклад каждого класса в итоговую оценку должен быть одинаковым независимо от его частоты. Важно: `MulticlassClassificationEvaluator(metricName="f1")` в Spark MLlib вычисляет **weighted F1**, а не Macro-F1.

---

## 6. Метрики регрессии

MAE:

$$
MAE=
\frac{1}{N}
\sum_{i=1}^{N}
|y_i-\hat{y}_i|.
$$

MSE:

$$
MSE=
\frac{1}{N}
\sum_{i=1}^{N}
(y_i-\hat{y}_i)^2.
$$

RMSE:

$$
RMSE=
\sqrt{
\frac{1}{N}
\sum_{i=1}^{N}
(y_i-\hat{y}_i)^2
}.
$$

RMSE сильнее штрафует крупные ошибки.

---

## 7. Cross-entropy

Для многоклассовой классификации:

$$
\mathcal{L}
=
-\sum_{k=1}^{K}y_k\log p_k.
$$

Для batch:

$$
\mathcal{L}_{batch}
=
-\frac{1}{N}
\sum_{i=1}^{N}
\sum_{k=1}^{K}
y_{ik}\log p_{ik}.
$$

Cross-entropy — дифференцируемая функция потерь для обучения модели, а F1 — метрика дискретных предсказаний после применения порога (`threshold`) или `argmax`. Эти показатели решают разные задачи и не являются взаимозаменяемыми.

---

## 8. MLlib Pipeline: практический пример

```python
!pip -q install "pyspark==4.2.0"

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lecture04")
    .getOrCreate()
)

data = [
    (46.0, 0.16, 12.4, 2.0, 0.0),
    (47.0, 0.18, 12.3, 2.1, 0.0),
    (48.0, 0.20, 12.2, 2.5, 0.0),
    (49.0, 0.22, 12.1, 2.8, 0.0),
    (50.0, 0.25, 12.0, 3.0, 0.0),
    (51.0, 0.28, 11.9, 3.1, 0.0),
    (58.0, 0.55, 11.6, 4.0, 1.0),
    (60.0, 0.62, 11.5, 4.3, 1.0),
    (61.0, 0.65, 11.4, 4.4, 1.0),
    (64.0, 0.70, 11.2, 4.7, 1.0),
    (65.0, 0.76, 11.1, 4.9, 1.0),
    (66.0, 0.82, 11.0, 5.0, 1.0),
]

df = spark.createDataFrame(
    data,
    [
        "temperature",
        "vibration_rms",
        "battery_v",
        "current_a",
        "label",
    ]
)

train, test = df.randomSplit([0.8, 0.2], seed=42)

assembler = VectorAssembler(
    inputCols=[
        "temperature",
        "vibration_rms",
        "battery_v",
        "current_a",
    ],
    outputCol="raw_features",
)

scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True,
)

classifier = LogisticRegression(
    labelCol="label",
    featuresCol="features",
    maxIter=100,
    regParam=0.0,
    elasticNetParam=0.0,
)

pipeline = Pipeline(
    stages=[assembler, scaler, classifier]
)

model = pipeline.fit(train)
pred = model.transform(test)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1",
)

print("Weighted F1 =", evaluator.evaluate(pred))

pred.select(
    "temperature",
    "vibration_rms",
    "label",
    "prediction",
    "probability",
).show(truncate=False)
```

В реальной лабораторной работе набор данных должен быть существенно больше; этот небольшой синтетический пример предназначен только для демонстрации API. Для деревьев решений и Random Forest масштабирование признаков обычно не требуется, поэтому в примере выбран `LogisticRegression`, для которой `StandardScaler` методически оправдан.

---

## 9. Масштабируемое обучение

Пусть:

$$
D=
D_1\cup
D_2\cup\dots\cup
D_P.
$$

Каждый worker вычисляет локальный градиент:

$$
g_p=
\nabla_\theta
\mathcal{L}(D_p;\theta).
$$

При синхронном параллелизме по данным и одинаковом размере локальных batch градиенты усредняются:

$$
g=
\frac{1}{P}
\sum_{p=1}^{P}g_p.
$$

Обновление:

$$
\theta_{t+1}
=
\theta_t-\eta g.
$$

### Архитектура

```text
                     model theta_t
                         |
       +-----------------+-----------------+
       |                 |                 |
       v                 v                 v
+-------------+   +-------------+   +-------------+
| Worker 0    |   | Worker 1    |   | Worker 2    |
| batch D0    |   | batch D1    |   | batch D2    |
| gradient g0 |   | gradient g1 |   | gradient g2 |
+------+------+   +------+------+   +------+------+
       |                 |                 |
       +-----------------+-----------------+
                         |
                         v
                  AllReduce / sync
                         |
                         v
                    averaged g
                         |
                         v
                    update theta
```

Время шага:

$$
T_{step}
=
T_{forward}
+
T_{backward}
+
T_{sync}.
$$

Эффективность:

$$
E(P)=
\frac{T(1)}{P\,T(P)}.
$$

Для модели с $M$ параметров `float32` размер одного gradient vector:

$$
S_g=4M\;bytes.
$$

Для $M=100$ млн:

$$
S_g\approx400\;MB.
$$

Следовательно, пропускная способность межсоединения и коллективные операции (`AllReduce`) становятся частью проектирования ML-системы. При различном размере локальных batch простое среднее градиентов необходимо заменить взвешенным по числу примеров.

---

## 10. Spark MLlib и deep learning

Spark MLlib хорошо подходит для:

- табличных признаков;
- распределённой предварительной обработки;
- классических алгоритмов машинного обучения;
- конвейеров признаков и моделей;
- крупной агрегированной телеметрии.

PyTorch/JAX/TensorFlow обычно применяются для:

- CNN;
- Transformer-моделей;
- Vision-Language-Action (VLA) моделей;
- imitation learning;
- крупных нейросетевых политик управления.

```text
Data Lake / robotics dataset
         |
         +------> Spark / SQL
         |        quality
         |        filtering
         |        statistics
         |        metadata
         |
         v
PyTorch/JAX training
         |
         v
robot policy
```

Инженерия больших данных не сводится к обучению нейросети: сбор, валидация, фильтрация, хранение и контроль качества данных часто образуют отдельную распределённую подсистему.

---

## 11. Open X-Embodiment

Open X-Embodiment объединяет открытые робототехнические наборы данных в унифицированном представлении для последующего использования в исследованиях и обучении моделей.

```text
dataset
 |
 +-- episode
 |    |
 |    +-- step
 |    |    +-- observation
 |    |    +-- action
 |    |
 |    +-- step
 |
 +-- episode
```

Масштабная линия:

```text
single robot log
      |
many episodes
      |
many robots
      |
many institutions
      |
cross-embodiment dataset
```

В курсе Open X-Embodiment используется не как обязательная тяжёлая задача обучения, а как пример масштабирования робототехнических данных, унификации схем и междоменных различий между роботами.

Официальный репозиторий:

https://github.com/google-deepmind/open_x_embodiment

---

## 12. Hugging Face LeRobotDataset

LeRobotDataset стандартизует синхронизированные мультимодальные временные ряды: видео, состояние робота, действия, описания задач и границы эпизодов.

LeRobotDataset v3 хранит множество эпизодов в более крупных Parquet/MP4-файлах и использует отдельные метаданные для схемы, статистик, задач и границ эпизодов. Это снижает нагрузку на файловую систему по сравнению с подходом «один эпизод — один файл».

```text
LeRobotDataset
 |
 +-- metadata
 |
 +-- Parquet data
 |    +-- actions
 |    +-- states
 |    +-- timestamps
 |
 +-- video files
 |
 +-- episode boundaries
```

Это прямой пример связи:

```text
robotics
+
columnar storage
+
video
+
metadata
+
streaming access
```

---

## 13. Потоковый доступ к наборам данных

Пусть набор данных содержит $N$ образцов.

Полная локальная копия требует объёма хранения порядка:

$$
Storage=O(N).
$$

Для потокового итератора с ограниченным буфером $B$ объём состояния буфера имеет порядок:

$$
BufferMemory=O(B),
\quad B\ll N.
$$

LeRobot предоставляет `StreamingLeRobotDataset`, который позволяет итерировать наборы данных непосредственно из Hugging Face Hub без предварительного скачивания полной локальной копии.

Концептуальный пример:

```python
# Версию API следует фиксировать перед семестром
# по официальной документации LeRobot.

from lerobot.datasets import StreamingLeRobotDataset

dataset = StreamingLeRobotDataset(
    repo_id="your-dataset-repo-id",
    buffer_size=1000,
)

for i, sample in enumerate(dataset):
    print(sample.keys())

    if i >= 9:
        break
```

Преимущество — существенно меньшие требования к локальному хранилищу. Ограничения — зависимость от сети, более сложный произвольный доступ и приближённое перемешивание (`shuffle`) через ограниченный буфер. Формула $O(B)$ относится к состоянию буфера итератора, а не ко всей памяти процесса, где дополнительно размещаются декодированные изображения/видео, batch и модель.

---

## 14. Качество набора данных

### Missing rate

$$
MissingRate_j=
\frac{N_{missing,j}}{N}.
$$

### Duplicate rate

$$
DuplicateRate=
\frac{N_{duplicate}}{N}.
$$

### Class probability

$$
p_k=
\frac{N_k}{N}.
$$

### Entropy разметки

$$
H(Y)=
-\sum_k p_k\log_2p_k.
$$

Низкая энтропия меток может быть сигналом сильного дисбаланса классов, но сама по себе не определяет качество набора данных. Дополнительно необходимо анализировать пропуски, дубликаты, покрытие режимов работы, дрейф распределений и корректность временной синхронизации.

---

## 15. Сквозной проект курса

```text
+-------------+
| Sensors     |
| IMU/LiDAR   |
+------+------+ 
       |
       v
+-------------+
| ROS 2       |
+------+------+
       |
       v
+-------------+
| rosbag/MCAP |
+------+------+
       |
       +-------------------+
       |                   |
       v                   v
+-------------+      +-------------+
| Kafka /     |      | Parquet     |
| stream      |      | archive     |
+------+------+      +------+------+
       |                    |
       v                    v
+-------------+       +------------+
| Streaming   |       | Spark SQL  |
| features    |       | batch      |
+------+------+       +------+-----+
       |                     |
       +----------+----------+
                  |
                  v
          +---------------+
          | feature table |
          +-------+-------+
                  |
                  v
          +---------------+
          | MLlib / DL    |
          +-------+-------+
                  |
                  v
            prediction
                  |
                  v
          +---------------+
          | Teach project |
          +---------------+
```

---

## 16. Teach как инженерно-педагогическая операция

`Teach` означает уменьшение инфраструктурной сложности при сохранении ключевых понятий и причинно-следственных связей исходной инженерной задачи.

Университетская версия:

```text
Kafka
Spark
MCAP
distributed ML
large robotics dataset
```

Школьная:

```text
CSV / small Parquet
Python stream generator
Pandas / small PySpark demo
prepared sensor segment
simple classifier
```

Требуется сохранить:

```text
timestamp
feature
window
train/test
metric
false positive / false negative
reproducibility
```

Формально:

$$
InfrastructureComplexity\downarrow,
$$

$$
ConceptualCore\approx\text{const}.
$$

---

## 17. Типичные педагогические ошибки

1. Давать модель до объяснения физического происхождения данных.
2. Использовать `Accuracy` как основную метрику при редком положительном классе.
3. Нормализовать данные по всему набору до разбиения train/test.
4. Показывать только готовую нейросеть без конвейера подготовки данных.
5. Скачивать огромный набор данных ради нескольких демонстрационных образцов.
6. Считать визуализацию конечным результатом без исследовательского вопроса, базовой линии и метрики.

---

## 18. Микрокейс для НТО / хакатона

Задача: классифицировать состояние мобильного робота:

```text
0 = normal
1 = warning
```

Признаки:

```text
temperature
vibration_rms
battery_v
current_a
accel_rms
```

Требуется:

1. разбиение на обучающую и тестовую выборки;
2. конвейер предварительной обработки и классификатор;
3. `Precision`, `Recall`, F1;
4. выбор приоритета между FN и FP;
5. анализ важности признаков (`feature importance`);
6. оформление `TEACH CARD`.

**Усложнение:** несколько роботов/доменов, `domain shift`, потоковый инференс и мониторинг дрейфа данных/качества.

---

## 19. Контрольные вопросы

1. Почему `scaler` должен обучаться только на train-выборке?
2. Почему высокий `Accuracy` не гарантирует качественную диагностику редкой неисправности?
3. Почему коммуникационные накладные расходы ограничивают ускорение распределённого обучения с параллелизмом по данным?
4. Чем потоковый доступ к набору данных отличается от предварительного скачивания и какие ограничения возникают?
5. Какие элементы университетской лабораторной по Big Data можно упростить для школьника без потери ключевых понятий?

---

## 20. Основные источники

1. Apache Spark. **ML Pipeline API**.  
   https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.Pipeline.html
2. Apache Spark. **MLlib Guide**.  
   https://spark.apache.org/docs/latest/ml-guide.html
3. Hugging Face. **LeRobot**.  
   https://huggingface.co/docs/lerobot/
4. Hugging Face. **LeRobotDataset v3**.  
   https://huggingface.co/docs/lerobot/lerobot-dataset-v3
5. Google DeepMind. **Open X-Embodiment**.  
   https://github.com/google-deepmind/open_x_embodiment
