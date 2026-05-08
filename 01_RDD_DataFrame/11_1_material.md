# Module 11.1: PySpark Basics — RDD dan DataFrame

**Date:** May 7, 2026  
**Learner:** Alma  
**Status:** Learning Material

---

## 📚 Daftar Isi

1. [Core Concept](#core-concept)
2. [Bagaimana Cara Kerjanya](#bagaimana-cara-kerjanya)
3. [Kapan dan Kenapa Gunakan](#kapan-dan-kenapa-gunakan)
4. [Lazy Evaluation](#lazy-evaluation)
5. [Membaca Data dengan Spark](#membaca-data-dengan-spark)
6. [Glossary](#glossary)
7. [Best Practice](#best-practice)
8. [Industry Insight](#industry-insight)
9. [Resources](#resources)

---

## Core Concept

### Apa itu RDD (Resilient Distributed Dataset)?

**RDD** adalah abstraksi data paling fundamental di Apache Spark. Bayangkan RDD seperti koleksi besar data yang tersebar di banyak komputer (distributed), dan Spark tahu caranya mengelola distribusi itu secara otomatis.

**Karakteristik RDD:**
- **Immutable** — Setelah dibuat, tidak bisa diubah
- **Distributed** — Data tersebar di berbagai node/komputer
- **Resilient** — Tahan terhadap node failure (recovery otomatis)
- **Unstructured** — Bisa berisi data apa saja (text, numbers, objects)

### Apa itu DataFrame?

**DataFrame** adalah lapisan abstraksi yang lebih tinggi di atas RDD. Kalau RDD adalah raw data mentah yang bisa berisi apa saja, DataFrame adalah data yang sudah terstruktur — seperti tabel database dengan kolom, tipe data, dan schema.

**Karakteristik DataFrame:**
- **Structured** — Punya schema (kolom names & types)
- **SQL-compatible** — Bisa pakai SQL queries
- **Optimized** — Engine bisa optimize performance
- **Familiar** — Mirip pandas/Excel

### Analogi Sederhana

```
RDD = Buku catatan yang berisi teks acakan
      Bisa berisi apa saja (kata, angka, simbol)
      Flexible tapi sulit untuk query/analisis

DataFrame = Excel spreadsheet yang sudah rapi
            Dengan kolom header, tipe data jelas
            Mudah untuk query dan analisis
```

---

## Bagaimana Cara Kerjanya

### RDD — Transformation & Action

RDD bekerja dengan dua konsep utama:

#### 1. Transformation (Lazy)

Transformation adalah operasi yang mengubah satu RDD menjadi RDD baru. **PENTING:** Transformation adalah **lazy** — artinya tidak langsung dijalankan. Spark hanya membuat "execution plan" dan tunggu ada action.

**Contoh transformations:**
- `map()` — transform setiap element
- `filter()` — select elements yang memenuhi kondisi
- `flatMap()` — map kemudian flatten hasil
- `join()` — combine dua RDD
- `reduceByKey()` — aggregate by key

```python
# Contoh RDD Transformation
from pyspark import SparkContext

sc = SparkContext("local", "RDD Example")

# Create RDD dari list
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Transformation 1: map (lazy — belum jalan)
rdd_doubled = rdd.map(lambda x: x * 2)

# Transformation 2: filter (lazy — belum jalan)
rdd_filtered = rdd_doubled.filter(lambda x: x > 5)
```

Di titik ini, **tidak ada komputasi yang terjadi**. Spark hanya catat: "Oh, kamu mau double dulu, terus filter > 5".

#### 2. Action (Eager)

Action adalah operasi yang **trigger eksekusi sebenarnya** dan mengembalikan hasil ke driver (komputer kamu).

**Contoh actions:**
- `collect()` — return semua data ke driver
- `count()` — return jumlah elements
- `first()` — return element pertama
- `take(n)` — return n elements pertama
- `show()` — print DataFrame (hanya untuk DataFrame)
- `write()` — save ke storage

```python
# Action: collect() — SEKARANG eksekusi!
result = rdd_filtered.collect()
# Output: [6, 8, 10]
```

**Baru action dipanggil, baru Spark jalankan semua transformations sebelumnya.**

---

### DataFrame — SQL-like Operations

DataFrame bekerja mirip seperti database. Kamu bisa menulis SQL queries atau method chaining.

```python
from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create DataFrame dari list of tuples
data = [("Alice", 25, "Engineer"), 
        ("Bob", 30, "Manager"), 
        ("Charlie", 35, "Director")]
columns = ["name", "age", "role"]

df = spark.createDataFrame(data, columns)

# Transformation 1: filter (lazy)
df_senior = df.filter(df.age > 28)

# Transformation 2: select (lazy)
df_result = df_senior.select("name", "role")

# Action: show() — SEKARANG eksekusi!
df_result.show()
```

**Output:**
```
+-------+-------+
|   name|   role|
+-------+-------+
|    Bob|Manager|
|Charlie|Director|
+-------+-------+
```

**Perbedaan DataFrame vs RDD:**
- RDD lebih fleksibel tapi umum dipakai untuk unstructured data
- DataFrame lebih cepat karena punya optimizer built-in
- DataFrame mudah dipakai untuk data yang terstruktur

---

## Kapan dan Kenapa Gunakan

| Aspek | RDD | DataFrame |
|-------|-----|-----------|
| **Fleksibilitas** | Tinggi — handle any data type | Terbatas — need structured data |
| **Performance** | Lebih lambat | Lebih cepat — optimized |
| **Ease of Use** | Sulit — functional programming | Mudah — mirip SQL |
| **Schema** | Tidak punya | Punya — kolom & types |
| **Optimization** | Manual | Automatic (Catalyst optimizer) |
| **SQL Support** | Tidak | Ya |
| **Use Case** | Unstructured, complex logic | Structured, analytics, reporting |

### Untuk Module 11 (Skill Extraction):

**Kita akan pakai DataFrame sebagai primary tool** karena:
- Data dari CV akan di-parse menjadi structured format (nama, skill, experience, dll)
- Kita butuh query dan filter cepat
- SQL operations sangat berguna untuk aggregasi dan transformasi
- Performance lebih baik untuk data processing di scale

**RDD knowledge tetap penting** karena:
- DataFrame dibangun di atas RDD secara internal
- Debugging — kadang perlu turun ke RDD level
- Understanding architecture — tahu kenapa DataFrame cepat

---

## Lazy Evaluation

Ini konsep **paling penting** di Spark. Pahami ini dengan baik.

### Apa itu Lazy Evaluation?

**Lazy evaluation** berarti transformations **tidak langsung dijalankan**. Spark membuat "execution plan" dan tunggu ada action.

### Mengapa Lazy Evaluation?

Lazy evaluation memungkinkan Spark untuk **optimize keseluruhan query sebelum eksekusi**, bukan operasi per operasi. Hasilnya bisa **10x lebih cepat**!

**Contoh:**

```python
# Scenario 1: Tanpa optimization (jika dijalankan sekaligus)
# 1. Read 1 juta rows
# 2. Map (double setiap number) → 1 juta operations
# 3. Filter > 5 → hanya 500ribu yang lolos

# Dengan lazy evaluation & optimization:
# Spark bisa lihat: "Filter > 5 ini akan eliminate banyak data"
# Jadi Spark bisa optimize: 
# 1. Push filter ke awal (filter while reading)
# 2. Hanya process 500ribu rows dari awal
# → Jauh lebih cepat!
```

### Contoh Lazy Evaluation di Code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lazyDemo").getOrCreate()

# Create DataFrame
df = spark.read.csv("large_file.csv", header=True)
print("Ini tidak membaca file sama sekali!")

# Transformation 1: filter (lazy)
df = df.filter(df.salary > 50000)
print("Masih tidak membaca file!")

# Transformation 2: select (lazy)
df = df.select("name", "salary")
print("Masih belum baca file!")

# Action: show() — SEKARANG file dibaca & processed!
df.show()
print("Baru di sini file dibaca dan diproses")

# Action lain: count() — trigger eksekusi lagi
count = df.count()
print(f"Total rows: {count}")
```

**Output console:**
```
Ini tidak membaca file sama sekali!
Masih tidak membaca file!
Masih belum baca file!
+-----+------+
| name|salary|
+-----+------+
...
Baru di sini file dibaca dan diproses
Total rows: 5000
```

---

## Membaca Data dengan Spark

Spark bisa membaca dari berbagai sumber: CSV, Parquet, JSON, database, ADLS (untuk Phase 7 nanti).

### Membaca CSV

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("readData").getOrCreate()

# Read CSV dengan header
df = spark.read.option("header", True).csv("path/to/file.csv")

# Atau dengan inferSchema (auto-detect types)
df = spark.read.option("header", True) \
                .option("inferSchema", True) \
                .csv("path/to/file.csv")

# Lihat schema (structure)
df.printSchema()

# Lihat data (first 5 rows)
df.show(5)

# Lihat jumlah rows
print(f"Total rows: {df.count()}")
```

### Membaca Parquet (lebih efficient)

```python
# Parquet adalah format columnar yang efficient
df = spark.read.parquet("path/to/file.parquet")
df.show()
```

### Membaca JSON

```python
df = spark.read.json("path/to/file.json")
df.show()
```

---

## Glossary

| Term | Definition |
|------|-----------|
| **RDD** | Resilient Distributed Dataset; abstraksi data fundamental di Spark, immutable dan distributed |
| **DataFrame** | Abstraksi data structured di atas RDD; mirip table dengan schema |
| **Transformation** | Operasi yang menghasilkan RDD/DataFrame baru (lazy — tidak langsung eksekusi) |
| **Action** | Operasi yang trigger eksekusi dan return hasil ke driver (eager — langsung jalan) |
| **Lazy Evaluation** | Spark tidak eksekusi transformations sampai ada action |
| **Schema** | Definisi struktur data (kolom names, tipe data, nullable flags) |
| **Parallelize** | Mendistribusikan data ke berbagai partitions di cluster |
| **Partition** | Subset dari data yang diproses oleh satu executor |
| **Executor** | Worker process yang jalankan tasks di node |
| **Driver** | Master process yang coordinate Spark application |
| **Catalyst Optimizer** | Query optimizer di Spark yang optimize execution plan |
| **Immutable** | Data tidak bisa diubah setelah dibuat (create baru jika butuh modify) |
| **Distributed** | Data tersebar di berbagai komputer dalam cluster |

---

## Best Practice

### 1. Gunakan DataFrame daripada RDD untuk Structured Data

```python
# ❌ BAD - Using RDD for structured data
rdd = sc.parallelize([("Alice", 25), ("Bob", 30)])
result = rdd.map(lambda x: (x[0], x[1] + 5)).collect()

# ✅ GOOD - Using DataFrame
df = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
result = df.withColumn("age_plus_5", df.age + 5).show()
```

### 2. Hindari `.collect()` pada Data Besar

```python
# ❌ BAD - collect large dataset
big_data = df.collect()  # Pull semua data to driver memory

# ✅ GOOD - use actions yang lebih efficient
df.write.parquet("output_path")  # Write directly
df.show(10)  # Show first 10 rows
df.count()  # Count (distributed)
```

### 3. Chain Transformations Sebelum Action

```python
# ❌ BAD - Multiple actions (inefficient)
df.filter(df.age > 25).show()
df.filter(df.age > 25).count()

# ✅ GOOD - Chain sebelum action
df_filtered = df.filter(df.age > 25)
df_filtered.show()
df_filtered.count()
```

### 4. Gunakan `.repartition()` atau `.coalesce()` Dengan Hati-hati

```python
# .repartition() - shuffle data (expensive but flexible)
df_repartitioned = df.repartition(100)  # 100 partitions

# .coalesce() - merge partitions (no shuffle)
df_coalesced = df.coalesce(10)  # 10 partitions dari existing
```

---

## Industry Insight

Di perusahaan data engineering:

1. **RDD hampir tidak pernah dipakai langsung** — semua orang pakai DataFrame/SQL sekarang
2. **DataFrame dan SQL adalah standard** — expected skill di job interview
3. **RDD knowledge still important** untuk:
   - Debugging ketika ada issue
   - Understanding architecture
   - Legacy systems yang masih pakai RDD

4. **Synapse (Phase 7) juga berbasis PySpark** — jadi semua konsep ini langsung applicable
5. **Performance adalah critical** — lazy evaluation & optimization adalah reason kenapa Spark powerful

---

## Resources

📎 **Official Documentation:**
- [Apache Spark RDD Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [Spark DataFrame Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

📖 **Key Concepts to Remember:**
1. RDD = Unstructured, flexible, slower
2. DataFrame = Structured, optimized, faster
3. Lazy evaluation = Spark waits for action
4. Transformations = Lazy (don't execute)
5. Actions = Eager (execute immediately)

---

**Ready untuk Exercise 1 (Easy)?** Let's go! 🚀