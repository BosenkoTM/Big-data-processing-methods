# Лекция 3. Большие данные робототехнических и IoT-систем

**Дисциплина:** «Методы обработки больших данных»  
**Направление:** 44.03.05 Педагогическое образование  
**Профиль:** «Информатика и дополнительное образование (робототехника)»  
**Этапы пайплайна:** `Sense -> Collect -> Store -> Process`

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

1. Сенсорные потоки робота: IMU, LiDAR, камеры, одометрия и оценка объёма данных.
2. ROS 2: `topic`, типы сообщений и журналирование в rosbag2; MCAP/SQLite3 и CDR-сериализация.
3. Извлечение данных ROS 2 через Python без развёртывания полного ROS 2-окружения.
4. Периферийные вычисления (`Edge Computing`), прореживание (`downsampling`), извлечение признаков и базы временных рядов.
5. Статистическая обработка: RMS, Z-score, энтропия окна и обнаружение аномалий.

После лекции студент должен уметь оценивать скорость и объём сенсорных данных, различать ROS 2 message/topic и формат хранения rosbag2, извлекать сообщения из MCAP/SQLite3 и проектировать конвейер `raw sensors -> edge features -> time-series analytics`.

> **Технологическая база на 31.08.2026:** актуальная ветка rosbag2 `rolling` использует MCAP как хранилище по умолчанию; для базы временных рядов учтён современный синтаксис TimescaleDB 2.20+ с `CREATE TABLE ... WITH (tsdb.hypertable, ...)`.

---

## 2. Скорость сенсорного потока

Для сенсора с частотой $f$ и размером измерения $s$:

$$
R=f\,s.
$$

Для $m$ сенсоров:

$$
R_{robot}=\sum_{i=1}^{m}f_i s_i.
$$

За время $T$:

$$
V=R_{robot}T.
$$

### Пример IMU

Пусть:

```text
200 Hz
6 float32 значений
```

Минимальный объём полезной нагрузки (`payload`):

$$
200\cdot6\cdot4=4800\;bytes/s.
$$

Реальное сообщение ROS 2 содержит также заголовок, временные метки, матрицы ковариации и служебные данные сериализации, поэтому фактическая нагрузка выше. Для оценки сетевой пропускной способности необходимо также учитывать протокольные накладные расходы и перевод байт/с в бит/с.

### Пример LiDAR

Point cloud:

$$
P=\{(x_i,y_i,z_i,I_i)\}_{i=1}^{N}.
$$

Для $N=100000$ точек, четырёх `float32`:

$$
S_{frame}\approx100000\cdot4\cdot4=1.6\;MB.
$$

При 10 Hz:

$$
R\approx16\;MB/s.
$$

Это оценка только полезной нагрузки без метаданных, выравнивания и возможных дополнительных полей `PointCloud2`.

---

## 3. Основные сенсорные представления

### 3.1. IMU

Ускорение:

$$
\mathbf{a}=[a_x,a_y,a_z]^T.
$$

Угловая скорость:

$$
\boldsymbol{\omega}=[\omega_x,\omega_y,\omega_z]^T.
$$

Норма ускорения:

$$
\|\mathbf{a}\|_2=
\sqrt{a_x^2+a_y^2+a_z^2}.
$$

Для неподвижного IMU модуль близок к $g$, но ориентация распределяет компоненты по осям.

### 3.2. LiDAR

```text
PointCloud2
|
+-- x
+-- y
+-- z
+-- intensity
+-- ring (optional)
+-- time/range (optional)
```

Задача больших данных возникает из сочетания:

- большого числа точек;
- высокой частоты;
- нескольких роботов;
- длительного времени записи.

### 3.3. Одометрия

Положение:

$$
\mathbf{p}_t=[x_t,y_t,z_t]^T.
$$

Пройденный путь:

$$
D=
\sum_{t=1}^{n-1}
\|\mathbf{p}_{t+1}-\mathbf{p}_t\|_2.
$$

---

## 4. ROS 2 как коммуникационная среда

```text
+-------------+    topic /imu/data    +-------------+
| IMU driver  |---------------------->| estimator   |
+------+------+                       +-------------+
       |
       | same topic
       v
+-------------+
| rosbag2     |
| recorder    |
+------+------+ 
       |
       v
 MCAP / SQLite
```

`Topic` — именованный логический канал обмена сообщениями. Издатели и подписчики согласуют тип сообщения; непосредственный обмен в ROS 2 обычно реализуется через RMW и DDS/RTPS либо другой поддерживаемый middleware.

Примеры:

```text
/sensors/imu       sensor_msgs/msg/Imu
/lidar/points      sensor_msgs/msg/PointCloud2
/odom              nav_msgs/msg/Odometry
```

---

## 5. rosbag2

В rosbag2 необходимо различать:

- плагин хранения (`storage plugin`);
- формат сериализации;
- схему/тип сообщения;
- сообщения с временными метками и служебными метаданными.

В актуальной ветке `rolling` репозитория rosbag2 поставляются плагины хранения `mcap` и `sqlite3`; `mcap` является форматом хранения по умолчанию. Для воспроизводимого курса версию ROS 2/rosbag2 следует фиксировать, поскольку поведение ветки `rolling` изменяется.

```text
ROS 2 topics
      |
      v
+----------------+
| rosbag2 writer |
+-------+--------+
        |
        +--------------------+
        |                    |
        v                    v
      MCAP                 SQLite3
```

### MCAP

Логические сущности:

```text
Schema
Channel
Message
Metadata
Indexes
Chunks
```

Пример:

```text
Schema:
  sensor_msgs/Imu

Channel:
  /imu/data
  message_encoding = cdr

Message:
  log_time
  publish_time
  raw CDR payload
```

---

## 6. CDR serialization

Для стандартного DDS/RMW-стека ROS 2 сообщения обычно сериализуются в CDR (`Common Data Representation`). В MCAP для ROS 2 канал может указывать `message_encoding = cdr`.

```text
message object
     |
serialize
     v
 CDR bytes
     |
network / rosbag
     |
deserialize
     v
message object
```

Нужно различать:

- схему;
- сериализованный payload;
- timestamp;
- topic/channel metadata.

Это критично при анализе bag-файла вне ROS 2-окружения: одних байтов payload недостаточно без типа сообщения и схемы декодирования.

---

## 7. Чтение rosbag без ROS

`rosbags` — pure Python библиотека для rosbag1/rosbag2, не требующая установленного ROS software stack.

```python
!pip -q install rosbags pandas pyarrow
```

### `AnyReader`

```python
from pathlib import Path

from rosbags.highlevel import AnyReader
import pandas as pd

bag_path = Path("/content/my_rosbag")

rows = []

with AnyReader([bag_path]) as reader:
    connections = [
        c for c in reader.connections
        if c.topic == "/imu/data"
    ]

    for connection, timestamp, rawdata in reader.messages(
        connections=connections
    ):
        msg = reader.deserialize(
            rawdata,
            connection.msgtype
        )

        rows.append({
            "timestamp_ns": timestamp,
            "ax": msg.linear_acceleration.x,
            "ay": msg.linear_acceleration.y,
            "az": msg.linear_acceleration.z,
            "wx": msg.angular_velocity.x,
            "wy": msg.angular_velocity.y,
            "wz": msg.angular_velocity.z,
        })

imu = pd.DataFrame(rows)

imu["accel_norm"] = (
    imu["ax"] ** 2 +
    imu["ay"] ** 2 +
    imu["az"] ** 2
) ** 0.5

imu.head()
```

Для пользовательского типа сообщения требуется зарегистрировать соответствующее `.msg`/IDL-описание в `typestore`; иначе библиотека не сможет корректно десериализовать CDR-payload.

---

## 8. MCAP Python tooling без ROS

```python
!pip -q install mcap mcap-ros2-support
```

Foxglove MCAP Python tooling поддерживает ROS 2 schema/encoding вне ROS 2 environment.

```text
MCAP
 |
 +-- Schema
 |
 +-- Channel /imu/ax
 |
 +-- Channel /imu/ay
 |
 +-- Channel /imu/az
 |
 +-- timestamped CDR messages
```

Для Google Colab это полезный архитектурный приём: bag-файл рассматривается как входной артефакт данных, а не как причина разворачивать полноценную ROS 2 middleware-инфраструктуру.

---

## 9. Синхронизация сенсоров

Пусть IMU измеряется в моменты:

$$
t_i^{IMU},
$$

а LiDAR:

$$
t_j^{LIDAR}.
$$

Простейшее nearest-neighbor сопоставление:

$$
j^*=
\arg\min_j
|t_i^{IMU}-t_j^{LIDAR}|.
$$

Допустимая ошибка синхронизации:

$$
|t_i^{IMU}-t_{j^*}^{LIDAR}|\le\varepsilon.
$$

Если $\varepsilon$ слишком велико, объединённый sample может описывать физически разные состояния робота. Для строгой синхронизации необходимо также определить политику повторного использования кадров, интерполяции и обработки пропусков; простое ближайшее соседство не гарантирует взаимно-однозначного сопоставления.

---

## 10. Edge Computing

Передавать все необработанные (`raw`) сенсорные данные в центральную систему не всегда возможно или экономически оправданно.

```text
SENSORS
  |
  v
+-----------------------+
| Edge computer         |
| filter                |
| synchronize           |
| downsample            |
| compress              |
| feature extraction    |
+-----------+-----------+
            |
      reduced stream
            |
            v
     Kafka / storage
```

### 10.1. Снижение трафика

$$
K_{edge}=
\frac{R_{raw}}{R_{out}}.
$$

Если исходный поток равен 80 MB/s, а после обработки на периферийном узле — 2 MB/s:

$$
K_{edge}=40.
$$

### 10.2. Downsampling

Для коэффициента $M$:

$$
f_{out}=\frac{f_{in}}{M}.
$$

Перед децимацией требуется НЧ-фильтрация (`low-pass filtering`), иначе высокочастотные компоненты могут отразиться в рабочий диапазон и вызвать алиасинг.

Критерий Найквиста:

$$
f_s>2f_{max}.
$$

Инженерный вывод: частоту дискретизации нельзя уменьшать только ради экономии места — сначала необходимо определить полезный частотный диапазон сигнала и применить антиалиасинговую фильтрацию.

---

## 11. База данных временных рядов

Типичная схема:

```text
timestamp
robot_id
sensor
value
quality
```

Типичная нагрузка временных рядов характеризуется:

- интенсивной вставкой новых измерений;
- диапазонными запросами по времени;
- агрегированием по временным интервалам (`time buckets`);
- политиками хранения (`retention`) и понижением детализации (`downsampling`).

### TimescaleDB

Для TimescaleDB 2.20+ таблицу временных рядов можно сразу создать как hypertable:

```sql
CREATE TABLE telemetry (
    ts          TIMESTAMPTZ NOT NULL,
    robot_id    TEXT        NOT NULL,
    motor_temp  DOUBLE PRECISION,
    vibration   DOUBLE PRECISION
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='ts',
    tsdb.segmentby='robot_id',
    tsdb.orderby='ts DESC'
);
```

Для TimescaleDB 2.19.3 и старше используется двухшаговая схема: обычная таблица PostgreSQL и последующий вызов `create_hypertable()`.

Агрегирование:

```sql
SELECT
    time_bucket('1 minute', ts) AS bucket,
    robot_id,
    AVG(motor_temp) AS avg_temp,
    MAX(motor_temp) AS max_temp
FROM telemetry
GROUP BY bucket, robot_id
ORDER BY bucket, robot_id;
```

`Continuous aggregate` материализует агрегаты по временным интервалам и обновляется по заданной политике; это позволяет не пересчитывать всю историю при каждом запросе.

```text
raw second-level data
        |
        v
1-minute aggregate
        |
        v
1-hour aggregate
```

---

## 12. RMS

Для вибрации:

$$
RMS=
\sqrt{
\frac{1}{N}
\sum_{i=1}^{N}x_i^2
}.
$$

RMS измеряет среднеквадратичный уровень и не уничтожает энергию сигнала из-за смены знака.

```python
import numpy as np

window = np.array([0.10, 0.12, 0.11, 0.75, 0.70])

rms = np.sqrt(np.mean(window ** 2))
print(rms)
```

---

## 13. Z-score

$$
z_i=
\frac{x_i-\mu}{\sigma}.
$$

Учебное правило:

$$
|z_i|\ge3
\Rightarrow
\text{candidate anomaly}.
$$

Ограничения:

- drift;
- нестационарность;
- multimodal distribution;
- выбросы искажают $\mu$ и $\sigma$.

Z-score не является «детектором неисправности» сам по себе; он выделяет статистически необычные точки относительно выбранного базового распределения (`baseline`). При дрейфе режима baseline необходимо пересматривать или вычислять адаптивно.

---

## 14. Энтропия сенсорного окна

Для дискретизированных состояний:

$$
H(X)=-\sum_i p_i\log_2p_i.
$$

Feature vector окна:

$$
\mathbf{z}
=
[
mean,\ std,\ RMS,\ min,\ max,\ entropy
]^T.
$$

Энтропия может изменяться при смене динамического режима, но её следует интерпретировать совместно с физически осмысленными признаками и выбранным способом дискретизации.

---

## 15. Скользящая статистика

Среднее на последних $N$ измерениях:

$$
\bar{x}_t=
\frac{1}{N}
\sum_{i=t-N+1}^{t}x_i.
$$

RMS:

$$
RMS_t=
\sqrt{
\frac{1}{N}
\sum_{i=t-N+1}^{t}x_i^2
}.
$$

Rolling Z-score:

$$
z_t=
\frac{x_t-\mu_t}{\sigma_t}.
$$

---

## 16. PySpark-анализ временного ряда

```python
!pip -q install "pyspark==4.2.0"

from pyspark.sql import SparkSession, functions as F, Window

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lecture03")
    .getOrCreate()
)

data = [
    ("robot-01", 1, 45.0, 0.20),
    ("robot-01", 2, 45.2, 0.21),
    ("robot-01", 3, 45.1, 0.19),
    ("robot-01", 4, 58.0, 0.80),
    ("robot-01", 5, 59.5, 0.84),
]

df = spark.createDataFrame(
    data,
    ["robot_id", "t", "temperature", "vibration"]
)

w = (
    Window
    .partitionBy("robot_id")
    .orderBy("t")
    .rowsBetween(-2, 0)
)

result = (
    df
    .withColumn(
        "vibration_rms",
        F.sqrt(
            F.avg(F.pow("vibration", 2)).over(w)
        )
    )
    .withColumn(
        "temp_mean_3",
        F.avg("temperature").over(w)
    )
)

result.show()
```

---

## 17. Архитектура мониторинга

```text
             +----------------+
             | IMU / LiDAR    |
             | motor / battery|
             +-------+--------+
                     |
                     v
             +----------------+
             | ROS 2 topics   |
             +-------+--------+
                     |
          +----------+-----------+
          |                      |
          v                      v
    +-----------+          +-----------+
    | rosbag2   |          | Edge      |
    | MCAP      |          | features  |
    +-----+-----+          +-----+-----+
          |                      |
          +----------+-----------+
                     |
                     v
             +----------------+
             | Spark / Flink  |
             +-------+--------+
                     |
          +----------+-----------+
          |                      |
          v                      v
   +-------------+       +-------------+
   | Time-Series |       | Data Lake   |
   | DB          |       | Parquet     |
   +------+------+       +-------------+
          |
          v
      dashboard
      + alerts
```

---

## 18. Teach: педагогический мост

### Как объяснять сенсорные данные

Использовать смартфон как знакомый объект:

```text
физическое явление
      |
датчик
      |
число + timestamp
      |
последовательность
      |
алгоритм
      |
решение
```

### Типичные ошибки

1. «Чем выше частота дискретизации, тем всегда лучше».
2. «Временные метки разных сенсоров автоматически синхронизированы и одинаково точны».
3. «Любой статистический выброс означает неисправность».
4. «RMS — это обычное арифметическое среднее».
5. «Edge Computing полностью заменяет централизованное хранилище или облачную обработку».

### Школьный микрокейс

**Проект:** «Определение удара мобильного робота по акселерометру».

Дано:

```text
timestamp, ax, ay, az
```

Рассчитать:

$$
a=
\sqrt{a_x^2+a_y^2+a_z^2},
$$

затем rolling RMS и threshold.

Необходимо:

1. выделить интервалы удара;
2. построить график;
3. объяснить false positive;
4. предложить дополнительный сенсорный признак (`feature`).

**НТО-усложнение:** объединить IMU и ток двигателя; тревогу считать достоверной только при согласованном изменении нескольких признаков.

---

## 19. Контрольные вопросы

1. Почему частота датчика одновременно влияет на требования к хранилищу, сети и вычислениям?
2. Чем объект сообщения ROS 2 отличается от CDR-payload, `topic` и формата хранения rosbag2?
3. Какие преимущества и ограничения даёт анализ rosbag2 без установленного ROS 2?
4. Почему `downsampling` без антиалиасинговой фильтрации может изменить физический смысл сигнала?
5. Какие разные свойства сенсорного временного ряда описывают RMS, Z-score и энтропия?

---

## 20. Основные источники

1. ROS 2. **rosbag2**.  
   https://github.com/ros2/rosbag2
2. ROS 2. **MCAP storage plugin**.  
   https://github.com/ros2/rosbag2/tree/rolling/rosbag2_storage_mcap
3. Ternaris. **Rosbags documentation**.  
   https://ternaris.gitlab.io/rosbags/
4. Foxglove. **MCAP**.  
   https://github.com/foxglove/mcap
5. Timescale. **CREATE TABLE: hypertable (TimescaleDB 2.20+)**.  
   https://docs.timescale.com/api/latest/hypertable/create_table/
6. Timescale. **Continuous aggregates**.  
   https://docs.timescale.com/use-timescale/latest/continuous-aggregates/about-continuous-aggregates/
