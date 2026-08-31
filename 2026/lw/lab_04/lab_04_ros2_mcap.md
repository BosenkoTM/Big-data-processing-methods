# Лабораторная работа № 4. Извлечение, декодирование и трансформация журналов ROS 2 без установленного ROS

* **Оригинальные репозитории:**
  * Ternaris / GitHub mirror — `rpng/rosbags`: https://github.com/rpng/rosbags
  * Foxglove — `foxglove/mcap`: https://github.com/foxglove/mcap
  * Официальный пример записи ROS 2 MCAP без ROS 2: https://github.com/foxglove/mcap/blob/main/python/examples/ros2-noenv/writer.py
  * Официальный пример чтения ROS 2 MCAP без ROS 2: https://github.com/foxglove/mcap/blob/main/python/examples/ros2-noenv/reader.py
  * Vikram Nagashoka — `rosbag-resurrector`: https://github.com/vikramnagashoka/rosbag-resurrector
* **Цель работы:** научиться формировать и декодировать ROS 2 MCAP непосредственно в Python без установки ROS 2, преобразовывать CDR-сообщения в табличную форму и Parquet.
* **Инструменты:** `Google Colab`, `mcap`, `mcap-ros2-support`, `rosbags 0.11.5`, `Pandas`, `PyArrow`, `Plotly`.
* **Этап пайплайна:** `Sense -> Collect -> Store -> Process`.
* **Входные данные:** зачётный MCAP генерируется самим ноутбуком. Для реальных логов разрешено подставить собственный `.mcap`/rosbag2; базовые форматы и примеры доступны в `foxglove/mcap` и `rpng/rosbags`.

### 1. Минимальные теоретические сведения

ROS 2 обычно сериализует сообщения через CDR (Common Data Representation). MCAP хранит schema, channel metadata, timestamps и сериализованный payload. Библиотека `mcap-ros2-support` может динамически кодировать/декодировать ROS 2 message definitions без `rclpy` и без установленного ROS. `rosbags` решает близкую задачу для rosbag1/rosbag2 и также не требует ROS stack.

Для трёх компонент ускорения модуль определяется как

$$
\lVert\mathbf{a}\rVert_2=
\sqrt{a_x^2+a_y^2+a_z^2}.
$$

Преобразование журнала в аналитический слой:

$$
MCAP/CDR\rightarrow
(topic,timestamp,payload)\rightarrow
DataFrame\rightarrow Parquet.
$$

### 2. Исходный код (Google Colab)

```python
!pip -q install "mcap==1.4.0" "mcap-ros2-support==0.5.7" "rosbags==0.11.5" pandas pyarrow plotly

from pathlib import Path
import math
import pandas as pd
import plotly.express as px

from mcap_ros2.writer import Writer as McapWriter
from mcap_ros2.decoder import DecoderFactory
from mcap.reader import make_reader

OUT = Path("/content/robot_imu_ros2.mcap")

# Валидное ROS 2 message definition без установленного ROS 2.
SCHEMA_NAME = "std_msgs/Float64"
SCHEMA_TEXT = "float64 data"

# 1. Создание ROS 2 MCAP: CDR кодируется mcap-ros2-support.
with OUT.open("wb") as f:
    writer = McapWriter(f)
    schema = writer.register_msgdef(SCHEMA_NAME, SCHEMA_TEXT)

    frequency_hz = 50
    n = 1000
    t0_ns = 1_720_000_000_000_000_000

    for i in range(n):
        t = i / frequency_hz
        ts_ns = t0_ns + int(t * 1e9)

        ax = 0.20 * math.sin(2 * math.pi * 1.2 * t)
        ay = 0.12 * math.cos(2 * math.pi * 0.8 * t)
        az = 9.81 + 0.05 * math.sin(2 * math.pi * 2.0 * t)

        # Искусственная ударная аномалия.
        if 8.0 <= t < 8.3:
            ax += 3.0
            ay -= 2.0

        for topic, value in [
            ("/imu/ax", ax),
            ("/imu/ay", ay),
            ("/imu/az", az),
        ]:
            writer.write_message(
                topic=topic,
                schema=schema,
                message={"data": float(value)},
                log_time=ts_ns,
                publish_time=ts_ns,
                sequence=i,
            )

    writer.finish()

print("MCAP:", OUT, "size =", OUT.stat().st_size, "bytes")

# 2. Декодирование CDR без ROS 2.
rows = {}

with OUT.open("rb") as f:
    reader = make_reader(f, decoder_factories=[DecoderFactory()])
    for schema, channel, message, ros_msg in reader.iter_decoded_messages():
        ts = message.log_time
        rows.setdefault(ts, {"timestamp_ns": ts})
        rows[ts][channel.topic.split("/")[-1]] = float(ros_msg.data)

imu = pd.DataFrame(rows.values()).sort_values("timestamp_ns").reset_index(drop=True)
imu["time_s"] = (imu["timestamp_ns"] - imu["timestamp_ns"].min()) / 1e9
imu["accel_norm"] = (imu["ax"]**2 + imu["ay"]**2 + imu["az"]**2) ** 0.5

display(imu.head())
print(imu.describe())

# 3. Трансформация в аналитический колоночный формат.
PARQUET = Path("/content/robot_imu.parquet")
imu.to_parquet(PARQUET, index=False)
print("Parquet:", PARQUET, "size =", PARQUET.stat().st_size, "bytes")

# 4. Визуализация извлечённой телеметрии.
long = imu.melt(
    id_vars=["time_s"],
    value_vars=["ax", "ay", "az"],
    var_name="axis",
    value_name="acceleration"
)

fig = px.line(long, x="time_s", y="acceleration", color="axis",
              title="Декодированная ROS 2 IMU-телеметрия из MCAP")
fig.show()

# 5. Проверки.
assert len(imu) == 1000
assert {"ax", "ay", "az"}.issubset(imu.columns)
assert imu["ax"].abs().max() > 2.5

# Дополнительно: rosbags 0.11.5 используется для чтения rosbag2 SQLite/MCAP
# из файлов, полученных от реального ROS 2. Полноценная установка ROS не требуется.
import rosbags
print("rosbags imported successfully")
```

### 3. Ход работы

1. Установите `mcap`, `mcap-ros2-support`, `rosbags`, `pandas`, `pyarrow`.
2. Создайте ROS 2 schema `std_msgs/Float64` и три топика `/imu/ax`, `/imu/ay`, `/imu/az`.
3. Запишите 1000 синхронных измерений в MCAP; добавьте искусственный ударный участок.
4. Прочитайте MCAP через `DecoderFactory()` и выполните CDR-десериализацию без ROS 2.
5. Соберите значения топиков по timestamp в единую таблицу и вычислите норму ускорения.
6. Сохраните результат в Parquet и визуализируйте оси ускорения.
7. Замените синтетический MCAP реальным робототехническим логом и перечислите необходимые изменения схемы/топиков.

### 4. Критерии оценки (ровно 10 баллов)

* **2 балла** — валидный ROS 2 MCAP создаётся непосредственно в Colab без ROS 2.
* **2 балла** — сообщения корректно CDR-декодируются и синхронизируются по timestamp.
* **2 балла** — построен DataFrame, вычислена норма ускорения и обнаруживается искусственная аномалия.
* **2 балла** — данные экспортированы в Parquet и построена корректная визуализация.
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

