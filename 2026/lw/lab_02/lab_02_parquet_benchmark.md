# Лабораторная работа № 2. Колоночное хранение, партиционирование и бенчмаркинг Parquet

* **Оригинальные репозитории:**
  * Balapriya C — `balapriyac/data-science-tutorials`: https://github.com/balapriyac/data-science-tutorials
  * PySpark/Parquet notebook: https://github.com/balapriyac/data-science-tutorials/blob/main/pyspark/pyspark_write_parquet.ipynb
  * Piotr Szul — `piotrszul/spark-tutorial`: https://github.com/piotrszul/spark-tutorial
* **Цель работы:** освоить колоночное хранение телеметрии в Parquet, физическое партиционирование набора и экспериментальное сравнение объёма и времени чтения CSV/Parquet.
* **Инструменты:** `Google Colab`, `PySpark 4.2.0 local[*]`, `Parquet`, `Snappy`, `PyArrow`.
* **Этап пайплайна:** `Collect -> Store -> Process`.
* **Входные данные:** синтетическая телеметрия генерируется в ноутбуке.

### 1. Минимальные теоретические сведения

Parquet хранит значения колонок совместно, что повышает эффективность компрессии и позволяет читать только необходимые столбцы (**column pruning**). Spark по умолчанию поддерживает компрессию Parquet; для учебного эксперимента используется Snappy. Партиционирование каталогов по `robot_id` позволяет отбрасывать не относящиеся к запросу каталоги (**partition pruning**).

Коэффициент уменьшения объёма:

$$
C=\frac{S_{CSV}}{S_{Parquet}}.
$$

Ускорение чтения:

$$
Speedup=\frac{T_{CSV}}{T_{Parquet}}.
$$

Результаты зависят от runtime Colab и файлового кэша, поэтому измерение повторяется несколько раз, а итогом берётся медиана.

### 2. Исходный код (Google Colab)

```python
!pip -q install "pyspark==4.2.0" pandas pyarrow

from pyspark.sql import SparkSession, functions as F
from pathlib import Path
import shutil
import time
import statistics

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Lab02_ParquetBenchmark")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

ROOT = Path("/content/lab02_storage")
CSV_DIR = ROOT / "csv"
PARQUET_DIR = ROOT / "parquet"
PARTITIONED_DIR = ROOT / "parquet_partitioned"

shutil.rmtree(ROOT, ignore_errors=True)
ROOT.mkdir(parents=True, exist_ok=True)

N = 350_000

df = (
    spark.range(N)
    .withColumn("robot_id", F.concat(F.lit("robot-"), (F.col("id") % 12).cast("string")))
    .withColumn("sensor", F.when(F.col("id") % 2 == 0, F.lit("imu")).otherwise(F.lit("motor")))
    .withColumn("ts_ms", (F.lit(1_720_000_000_000) + F.col("id") * 100).cast("long"))
    .withColumn("temperature", F.lit(40.0) + (F.col("id") % 150) * 0.08)
    .withColumn("vibration", F.abs(F.sin(F.col("id") / 15.0)) + (F.col("id") % 11) * 0.002)
    .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
    .drop("id")
)

df.write.mode("overwrite").option("header", True).csv(str(CSV_DIR))
df.write.mode("overwrite").option("compression", "snappy").parquet(str(PARQUET_DIR))
df.write.mode("overwrite").partitionBy("robot_id").parquet(str(PARTITIONED_DIR))

def dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())

sizes = {
    "CSV": dir_size(CSV_DIR),
    "Parquet": dir_size(PARQUET_DIR),
    "Partitioned Parquet": dir_size(PARTITIONED_DIR),
}

for name, size in sizes.items():
    print(f"{name:22s}: {size / 1024 / 1024:.2f} MiB")

compression_ratio = sizes["CSV"] / sizes["Parquet"]
print(f"CSV/Parquet size ratio: {compression_ratio:.2f}x")

def benchmark_read(fmt, path, predicate=None, repeats=3):
    times = []
    counts = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        if fmt == "csv":
            x = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
        else:
            x = spark.read.parquet(str(path))
        if predicate is not None:
            x = x.filter(predicate)
        counts.append(x.count())
        times.append(time.perf_counter() - t0)
    return statistics.median(times), counts[-1]

csv_t, csv_n = benchmark_read("csv", CSV_DIR, F.col("robot_id") == "robot-7")
pq_t, pq_n = benchmark_read("parquet", PARQUET_DIR, F.col("robot_id") == "robot-7")
part_t, part_n = benchmark_read("parquet", PARTITIONED_DIR, F.col("robot_id") == "robot-7")

print(f"CSV                : {csv_t:.3f}s, rows={csv_n}")
print(f"Parquet            : {pq_t:.3f}s, rows={pq_n}")
print(f"Partitioned Parquet: {part_t:.3f}s, rows={part_n}")

print("Speedup CSV -> Parquet:", round(csv_t / pq_t, 2), "x")
print("Speedup CSV -> partitioned:", round(csv_t / part_t, 2), "x")

# Проверяем predicate/partition pruning в плане.
(
    spark.read.parquet(str(PARTITIONED_DIR))
    .filter(F.col("robot_id") == "robot-7")
    .select("robot_id", "temperature")
    .explain(mode="formatted")
)

assert csv_n == pq_n == part_n
assert sizes["Parquet"] < sizes["CSV"]
```

### 3. Ход работы

1. Создайте телеметрический DataFrame не менее чем из 300 000 строк.
2. Запишите один и тот же набор в CSV, Parquet/Snappy и Parquet с `partitionBy("robot_id")`.
3. Рассчитайте суммарный физический размер каждого представления.
4. Выполните одинаковый фильтр `robot_id == "robot-7"` не менее трёх раз для каждого формата и вычислите медианное время.
5. Рассчитайте коэффициенты `CSV/Parquet` по объёму и времени.
6. Исследуйте physical plan запроса к partitioned Parquet и найдите `PartitionFilters`.
7. Объясните, когда слишком большое число мелких partition ухудшает эффективность хранения.

### 4. Критерии оценки (ровно 10 баллов)

* **2 балла** — корректно созданы три физические версии набора: CSV, Parquet, partitioned Parquet.
* **2 балла** — рассчитаны размеры и коэффициент сжатия/уменьшения объёма.
* **2 балла** — выполнен воспроизводимый benchmark с минимум тремя повторениями каждого чтения.
* **2 балла** — исследованы column/partition pruning и сделан технический вывод по результатам.
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

