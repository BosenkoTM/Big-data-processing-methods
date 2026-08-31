# Лекция 2. Потоковые данные и распределённая обработка событий

**Дисциплина:** «Методы обработки больших данных»  
**Направление:** 44.03.05 Педагогическое образование  
**Профиль:** «Информатика и дополнительное образование (робототехника)»  
**Этапы пайплайна:** `Stream -> Process`

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

1. Модель неограниченного потока: пропускная способность (`throughput`), задержка (`latency`), очередь (`backlog`) и обратное давление (`backpressure`).
2. Apache Kafka: `topic`, `partition`, `offset`, репликация, `consumer group`; кворум метаданных KRaft.
3. Spark Structured Streaming: неограниченная таблица, микропакетная обработка (`micro-batch`), состояние и контрольные точки (`checkpoint`).
4. Время события (`Event Time`), время обработки (`Processing Time`), окна и водяные знаки (`watermark`).
5. Apache Flink DataStream: обработка с состоянием и отказоустойчивость.

После лекции студент должен уметь проектировать поток обработки телеметрии нескольких роботов, выбирать ключ партиционирования Kafka, рассчитывать параметры окна, объяснять роль `watermark` и восстанавливать состояние потокового приложения после сбоя.

> **Технологическая база на 31.08.2026:** Apache Kafka 4.3.1 (только KRaft), Apache Spark 4.2.0, Apache Flink 2.3. Для лабораторных рекомендуется фиксировать версии в `requirements.txt`/контейнере, а не полагаться на `latest`.

---

## 2. Модель потока

Пусть события поступают со средней интенсивностью $\lambda$ событий/с, система обрабатывает $\mu$ событий/с.

Для устойчивой системы:

$$
\lambda<\mu.
$$

Коэффициент загрузки:

$$
\rho=\frac{\lambda}{\mu}.
$$

При $\rho\rightarrow1$ даже кратковременный всплеск (`burst`) создаёт очередь и увеличивает задержку. Условие $\lambda<\mu$ описывает упрощённую стационарную модель; в реальной системе требуется запас производительности из-за неравномерности потока, перекоса ключей (`skew`) и пауз на обслуживание.

### 2.1. Пропускная способность (Throughput)

$$
Throughput=\frac{N_{events}}{T}.
$$

### 2.2. Сквозная задержка (End-to-end latency)

$$
L=t_{result}-t_{event}.
$$

Декомпозиция:

$$
L=
L_{network}+
L_{queue}+
L_{compute}+
L_{sink}.
$$

### 2.3. Закон Литтла

Для стабильной системы:

$$
N=\lambda W,
$$

где $N$ — среднее число событий внутри конвейера, $W$ — среднее время пребывания события в системе.

Если одновременно находятся 20 000 событий при 5 000 events/s:

$$
W=\frac{20000}{5000}=4\;s.
$$

---

## 3. Apache Kafka

Kafka хранит события в виде дописываемого журнала (`append-only log`) внутри каждой партиции (`partition`).

```text
Topic: robot.telemetry

Partition 0:
offset 0 -> robot-00
offset 1 -> robot-04
offset 2 -> robot-00

Partition 1:
offset 0 -> robot-01
offset 1 -> robot-05

Partition 2:
offset 0 -> robot-02
```

Идентификатор позиции:

$$
(topic,\ partition,\ offset).
$$

`Offset` — позиция записи внутри конкретной партиции, а не временная метка (`timestamp`).

### 3.1. Key и ordering

Если producer использует:

```text
key = robot_id
```

события одного робота маршрутизируются в одну и ту же партицию при неизменных числе партиций и алгоритме партиционирования. Изменение числа партиций может изменить соответствие `key -> partition`.

Kafka гарантирует порядок записей внутри одной партиции, но не задаёт глобальный порядок между разными партициями.

### 3.2. Consumer group

```text
Partitions       Consumer group

P0 -----------> Consumer A
P1 -----------> Consumer B
P2 -----------> Consumer C
P3 -----------> Consumer A
```

Максимальное число одновременно активных потребителей одной `consumer group` ограничено числом партиций:

$$
P_{active}\le P_{partition}.
$$

Если экземпляров `consumer` больше, чем партиций, часть из них не получает назначений и простаивает.

### 3.3. Replication

```text
Partition P0
|
+-- Leader   Broker 1
+-- Replica  Broker 2
+-- Replica  Broker 3
```

По умолчанию чтение и запись выполняются через лидера партиции, а реплики поддерживают согласованные копии. Реальная долговечность определяется `replication.factor`, составом ISR (`in-sync replicas`) и политикой подтверждений производителя (`acks`).

---

## 4. KRaft

Начиная с Apache Kafka 4.0 режим ZooKeeper удалён: Kafka 4.x работает в режиме KRaft, где метаданные кластера хранятся и реплицируются кворумом контроллеров.

```text
                       +-------------------+
                       | Active Controller |
                       +---------+---------+
                                 |
                       replicated metadata
                 +---------------+---------------+
                 |               |               |
                 v               v               v
          +------------+  +------------+  +------------+
          | Controller |  | Controller |  | Controller |
          | voter      |  | voter      |  | voter      |
          +------------+  +------------+  +------------+

          +------------+  +------------+  +------------+
          | Broker 1   |  | Broker 2   |  | Broker 3   |
          +------------+  +------------+  +------------+
```

Минимальный нечётный кворум, способный сохранить большинство при $f$ одновременных отказах контроллеров:

$$
N_{controllers}=2f+1.
$$

Три контроллера позволяют пережить отказ одного контроллера при сохранении большинства.

В учебном одноузловом стенде роли `broker` и `controller` могут совмещаться. В производственных кластерах роли обычно разделяют, чтобы отказ или перегрузка брокера не влияли одновременно на кворум метаданных.

---

## 5. Потоковый конвейер робототехнической системы

```text
+---------+       +---------+       +------------------+
| IMU     |       | LiDAR   |       | Motor controller |
+----+----+       +----+----+       +---------+--------+
     |                 |                      |
     +-----------------+----------------------+
                       |
                       v
                +--------------+
                | Edge Gateway |
                | ROS 2/Python |
                +------+-------+
                       |
                       v
                +--------------+
                | Kafka topic  |
                | telemetry    |
                +------+-------+
                       |
             +---------+----------+
             |                    |
             v                    v
   +-------------------+   +------------------+
   | Spark Structured  |   | archival sink    |
   | Streaming / Flink |   | Parquet/Object   |
   +---------+---------+   +------------------+
             |
             v
        TSDB / alerts
```

---

## 6. Event Time и Processing Time

Для сенсорного события:

- `event_time` — момент физического измерения;
- `ingestion_time` — момент поступления события в движок потоковой обработки;
- `processing_time` — момент, когда оператор фактически обрабатывает событие.

Пример:

```text
12:00:01.100  IMU измерил ускорение
12:00:01.120  edge сериализовал пакет
12:00:04.800  пакет пришёл по Wi-Fi
12:00:05.020  Spark обработал пакет
```

Задержка:

$$
L=t_{processing}-t_{event}.
$$

Для воспроизводимой аналитики сенсорных данных основной временной осью обычно выбирают `event_time`, полученный как можно ближе к источнику измерения.

---

## 7. Оконные алгоритмы

### 7.1. Tumbling window

Для размера $W$:

$$
Window_k=[kW,(k+1)W).
$$

```text
|------ W0 ------|------ W1 ------|------ W2 ------|
```

Окна не перекрываются.

### 7.2. Sliding window

Размер $W$, шаг $S$:

$$
Window_k=[kS,kS+W).
$$

```text
|---------- W0 ----------|
      |---------- W1 ----------|
            |---------- W2 ----------|
```

При $S<W$ запись участвует приблизительно в

$$
M\approx\left\lceil\frac{W}{S}\right\rceil
$$

окнах.

### 7.3. Session window

События объединяются, пока gap между ними меньше $G$:

```text
x x x      x x                 x x x
<----->    <->                 <----->
session 1  session 2           session 3
```

Подходит для эпизодов управления, пользовательских действий и серий команд робота.

---

## 8. Watermark

Для bounded out-of-orderness упрощённая модель:

$$
WM=\max(t_{event\ observed})-\Delta,
$$

где $\Delta$ — допустимое запаздывание.

Если максимальный observed event time равен 12:00:40, а $\Delta=10$ s:

```text
watermark ≈ 12:00:30
```

### Latency vs completeness

Большая $\Delta$:

- больше late events;
- больше state;
- более поздняя финализация.

Малая $\Delta$:

- меньше задержка;
- больше риск потерять late data.

`Watermark` не удаляет записи из Kafka. Это оценка прогресса времени событий, используемая потоковым движком для управления состоянием окон и обработки опоздавших данных.

---

## 9. Spark Structured Streaming

Spark Structured Streaming рассматривает входной поток как неограниченную таблицу, к которой применяется инкрементально исполняемый запрос.

```text
new event
    |
    v
+-------------------+
| Unbounded table   |
+-------------------+
    |
SQL/DataFrame query
    |
incremental result
```

### Colab-safe пример

```python
!pip -q install "pyspark==4.2.0"

from pyspark.sql import SparkSession, functions as F
import time

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lecture02")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

source = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
)

telemetry = (
    source
    .withColumn(
        "robot_id",
        F.concat(F.lit("robot-"), (F.col("value") % 4).cast("string"))
    )
    .withColumnRenamed("timestamp", "event_time")
    .withColumn(
        "motor_temp",
        F.lit(45.0) + (F.col("value") % 20) * 0.5
    )
)

stats = (
    telemetry
    .withWatermark("event_time", "10 seconds")
    .groupBy(
        F.window("event_time", "20 seconds", "5 seconds"),
        "robot_id"
    )
    .agg(
        F.count("*").alias("events"),
        F.avg("motor_temp").alias("avg_temp"),
        F.max("motor_temp").alias("max_temp")
    )
)

query = (
    stats.writeStream
    .format("memory")
    .queryName("robot_stats")
    .outputMode("update")
    .trigger(processingTime="1 second")
    .start()
)

time.sleep(8)
query.processAllAvailable()
query.stop()

spark.sql("""
SELECT
    window.start,
    window.end,
    robot_id,
    events,
    ROUND(avg_temp, 2) AS avg_temp,
    max_temp
FROM robot_stats
ORDER BY start DESC, robot_id
""").show(30, truncate=False)
```

### Kafka source

```python
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "robot.telemetry")
    .load()
)
```

Далее `value` декодируется из JSON/Avro/Protobuf и передаётся в тот же DataFrame pipeline.

---

## 10. Обработка с состоянием и контрольные точки

Оконный оператор хранит состояние:

```text
(robot-01, window 12:00:00-12:00:20)
count = 318
sum_temp = ...
max_temp = ...
```

После сбоя система должна согласованно восстановить:

1. позицию чтения (`offset`) источника;
2. состояние операторов;
3. состояние или семантику записи в приёмник (`sink`).

```text
Source offsets
      |
      v
+-------------+
| Operators   |---- state ----+
+-------------+               |
      |                       v
      |                +-------------+
      +--------------->| Checkpoint  |
                       +-------------+
```

`Exactly-once` нельзя приписывать одному оператору или одному фреймворку без оговорок. Контрольные точки обеспечивают согласованное восстановление состояния и позиции источника, но сквозная (`end-to-end`) семантика зависит также от `sink`. Если внешний приёмник не поддерживает транзакции или идемпотентную запись, повторное воспроизведение (`replay`) может создать дубликаты.

Идемпотентный ключ:

```text
PRIMARY KEY (robot_id, event_id)
```

позволяет безопасный `UPSERT`.

---

## 11. Apache Flink DataStream (Flink 2.3)

```text
Source
  |
map/filter
  |
assign timestamps + watermarks
  |
keyBy(robot_id)
  |
window
  |
aggregate/process
  |
Sink
```

### PyFlink: bounded out-of-orderness

```python
from pyflink.common import Duration
from pyflink.common.watermark_strategy import WatermarkStrategy

wm = WatermarkStrategy.for_bounded_out_of_orderness(
    Duration.of_seconds(10)
)
```

Ключевые понятия, общие для Spark Structured Streaming и Flink DataStream:

```text
timestamp
key
state
window
watermark
checkpoint
sink semantics
```

---

## 12. Fault tolerance

### Broker failure

Replica может стать leader при корректной replication/ISR configuration.

### Отказ потокового worker

Состояние восстанавливается из контрольной точки, а чтение источника продолжается с согласованной позиции. Для Flink checkpoint должен храниться в надёжном хранилище; по умолчанию checkpointing необходимо явно включить.

### Duplicate external effect

```text
event E
 -> INSERT
 -> worker crash
 -> replay E
 -> duplicate INSERT
```

Решения:

- идемпотентный `sink`;
- транзакционный `sink`;
- детерминированный идентификатор события;
- `UPSERT` / `MERGE` вместо безусловного `INSERT`.

---

## 13. Teach: педагогический мост

### Как объяснять потоковую обработку

Модель «турникет»:

```text
ученики входят
     |
события появляются непрерывно
     |
система считает входы каждую минуту
```

CSV-файл можно представить как «фотографию» накопленных данных, а поток — как непрерывно поступающие события, которые приходится обрабатывать по мере появления.

### Event time

> Фото сделано в 10:01, но отправлено в 10:05. Когда произошло событие?

Так понятие `event_time` вводится без преждевременной привязки к терминологии Kafka.

### Типичные ошибки

1. «Kafka сама выполняет аналитическую обработку данных».
2. «`Offset` — это временная метка».
3. «`Watermark` удаляет исходные события из Kafka».
4. «Размер окна (`window size`) равен интервалу запуска (`trigger interval`)».
5. «`Exactly-once` автоматически распространяется на любую внешнюю БД».

### Микрокейс НТО

Дан поток:

```text
timestamp, robot_id, motor_temp
```

События задерживаются до 7 секунд. Каждые 5 секунд требуется максимум за последние 20 секунд.

При:

$$
W=20,\quad S=5
$$

одна запись участвует примерно в

$$
\frac{20}{5}=4
$$

окнах.

Учащийся должен выбрать watermark и объяснить судьбу late event.

---

## 14. Контрольные вопросы

1. Почему Kafka гарантирует порядок только внутри одной партиции?
2. Как `key=robot_id` влияет на порядок, параллелизм и перекос нагрузки (`skew`)?
3. Почему `event_time` важнее `processing_time` для воспроизводимой аналитики сенсорных данных?
4. Что может произойти с состоянием окон при бесконечном потоке без корректной политики `watermark`?
5. Почему сквозную семантику `exactly-once` нельзя утверждать без анализа внешнего `sink`?

---

## 15. Основные источники

1. Apache Kafka. **KRaft**.  
   https://kafka.apache.org/43/operations/kraft/
2. Apache Spark. **Structured Streaming Programming Guide**.  
   https://spark.apache.org/docs/latest/streaming/
3. Apache Flink 2.3. **Generating Watermarks**.  
   https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/event-time/generating_watermarks/
4. Apache Flink 2.3. **Checkpointing**.  
   https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/fault-tolerance/checkpointing/
5. Apache Flink 2.3. **Windows**.  
   https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows/
