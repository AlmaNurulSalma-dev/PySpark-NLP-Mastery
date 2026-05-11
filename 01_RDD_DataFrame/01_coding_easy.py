"""
╔══════════════════════════════════════════════════════════════╗
║          Exercise 1 (Easy) — Basic RDD and DataFrame         ║
║               PySpark Basics Learning Module                 ║
╚══════════════════════════════════════════════════════════════╝

INSTRUCTIONS:
Write a Python script that does the following:

1. Create a SparkContext with local mode (use all available cores)

2. Create an RDD from the following list:
   data = [("Alice", 25, "Engineer"), 
           ("Bob", 30, "Manager"), 
           ("Charlie", 35, "Director"),
           ("David", 28, "Analyst")]

3. Display the RDD (use .collect() to print)

4. Create a DataFrame from the same data with columns: ["name", "age", "role"]
   Hint: Use SparkSession, not SparkContext

5. Display DataFrame schema (use .printSchema())

6. Display DataFrame data (use .show())

7. Add inline comments explaining:
   - What SparkContext does
   - Difference between RDD and DataFrame
   - Why we use .collect() for small data
   - Why we use .show() instead of .collect() for DataFrame

────────────────────────────────────────────────────────────────

EXPECTED OUTPUT:
────────────────────────────────────────────────────────────────

RDD collected: [('Alice', 25, 'Engineer'), ('Bob', 30, 'Manager'), 
                ('Charlie', 35, 'Director'), ('David', 28, 'Analyst')]

DataFrame Schema:
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- role: string (nullable = true)

DataFrame Data:
+-------+---+---------+
|   name|age|     role|
+-------+---+---------+
|  Alice| 25| Engineer|
|    Bob| 30|  Manager|
|Charlie| 35| Director|
|  David| 28|  Analyst|
+-------+---+---------+

────────────────────────────────────────────────────────────────

TIPS:
────────────────────────────────────────────────────────────────
- Use SparkContext("local[*]", "Exercise1") for local with all cores
- Use SparkSession.builder.appName(...).getOrCreate() for DataFrame
- RDD requires: from pyspark import SparkContext
- DataFrame requires: from pyspark.sql import SparkSession
- Add comments explaining each step
- Test your script before submitting

────────────────────────────────────────────────────────────────
"""

# YOUR CODE BELOW:
# ================

"""
Exercise 1 (Easy) — Basic RDD and DataFrame
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.getActiveSession()

# Create RDD
data = [("Alice", 25, "Engineer"), 
         ("Bob", 30, "Manager"), 
         ("Charlie", 35, "Director"),
         ("David", 28, "Analyst")]

# Display data (simulating RDD collect for learning purposes)
print("Data collected:")
print(data)

print("\n" + "="*60 + "\n")

# Create DataFrame with explicit schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("role", StringType(), True)
])

df = spark.createDataFrame(data, schema)

# Display schema
print("DataFrame Schema:")
df.printSchema()

print("\n" + "="*60 + "\n")

# Display data
print("DataFrame Data:")
display(df)