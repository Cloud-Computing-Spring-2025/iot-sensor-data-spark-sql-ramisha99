
# IoT Sensor Data Analysis with Spark SQL

This project performs step-by-step analysis on IoT sensor data using PySpark. Each task corresponds to a separate Python script and outputs a CSV file.

## Dataset

Input file: `sensor_data.csv`

Columns:
- sensor_id
- timestamp
- temperature
- humidity
- location
- sensor_type

---

## Setup Instructions

1. **Install Spark** :

```bash
pip install pyspark
```

2. **Generate dataset** (`sensor_data.csv`) :
```bash
python data_generator.py
```

3. **Run each task script one by one** using:

```bash
python taskX.py
```

Replace `X` with task number 1 to 5.

Each script will generate an output CSV file, e.g., `task1_output.csv`, `task2_output.csv`, etc.

---

## 📋 Tasks Overview

### ✅ Task 1 - Load & Basic Exploration (`task1.py`)
- Load the CSV file.
- Create a temp view.
- Show first 5 rows.
- Count total records.
- Show distinct locations and sensor types.
- Save full DataFrame to `task1_output.csv`.
- Save the terminal logs 

Run with:
```bash
python task1.py > outputterminal.txt
```

---

### ✅ Task 2 - Filtering & Aggregation (`task2.py`)
- Filter records by temperature range.
- Count in-range and out-of-range.
- Group by location for avg temperature and humidity.
- Save results to `task2_output.csv`.

Run with:
```bash
python task2.py
```

---

### ✅ Task 3 - Time-Based Analysis (`task3.py`)
- Convert `timestamp` to datetime.
- Extract `hour_of_day`.
- Group by hour and find average temperature.
- Save results to `task3_output.csv`.

Run with:
```bash
python task3.py
```

---

### ✅ Task 4 - Window Function (`task4.py`)
- Calculate average temperature per sensor.
- Rank sensors using `DENSE_RANK`.
- Show top 5.
- Save full ranked results to `task4_output.csv`.

Run with:
```bash
python task4.py
```

---

### ✅ Task 5 - Pivot & Interpretation (`task5.py`)
- Extract hour from timestamp.
- Pivot by hour with location as row, and average temp as value.
- Save pivot table to `task5_output.csv`.

Run with:
```bash
python task5.py
```

---

## ✅ Output Files

Each task will generate its corresponding output CSV:

- `task1_output.csv`
- `task2_output.csv`
- `task3_output.csv`
- `task4_output.csv`
- `task5_output.csv`
