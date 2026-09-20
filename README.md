# Data Quality Checks for Data Engineers

A practical reference of **25 data quality checks** for data engineers, with SQL and PySpark patterns, production tips, and interview-ready talking points. Examples use a **healthcare patient table** moving from the Bronze to the **Silver layer** of a medallion architecture.

![SQL](https://img.shields.io/badge/SQL-ready-blue)
![PySpark](https://img.shields.io/badge/PySpark-ready-orange)
![Medallion](https://img.shields.io/badge/Architecture-Medallion-lightgrey)

---

## Table of Contents

- [Overview](#overview)
- [The 25 Checks](#the-25-checks)
- [How Each Chapter Is Structured](#how-each-chapter-is-structured)
- [Quick Start Examples](#quick-start-examples)
- [Quarantine Pattern (Best Practice)](#quarantine-pattern-best-practice)
- [Monitoring Metrics](#monitoring-metrics)
- [Repository Structure](#repository-structure)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

Bad data breaks dashboards, models, and trust. This guide covers the validation rules a data engineer should apply **before data is consumed downstream**, so failures are caught early, logged with a clear reason, and never silently propagate.

Each check answers three questions:

1. **What rule is being validated?**
2. **How do I implement it in SQL and PySpark?**
3. **What happens to records that fail?** (Quarantine, don't drop.)

---

## The 25 Checks

| #  | Check                                | What it catches                                                       |
|----|--------------------------------------|------------------------------------------------------------------------|
| 1  | Null or Missing Values               | Required fields that are empty                                         |
| 2  | Primary Key Uniqueness               | Repeated or missing business keys                                      |
| 3  | Duplicate Record Detection           | Fully or partially duplicated rows                                     |
| 4  | Referential Integrity Validation     | Child records with no matching parent                                  |
| 5  | Data Type Validation                 | Values that can't be cast to the expected type                         |
| 6  | Numeric Range Checks                 | Values outside acceptable min/max bounds                               |
| 7  | String Length Validation             | Values too short or too long for the target column                     |
| 8  | Regex Pattern Validation             | Malformed emails, phone numbers, IDs, postal codes                     |
| 9  | Allowed Values / Domain Validation   | Values outside a defined list (e.g., gender, status codes)             |
| 10 | Business Rule Consistency            | Records that violate domain rules                                      |
| 11 | Cross-Column Consistency Checks      | Conflicting values across columns in the same row                      |
| 12 | Timeliness / Freshness Check         | Stale or late-arriving data                                            |
| 13 | Completeness Check                   | Missing records or missing partitions/days                             |
| 14 | Volume Checks                        | Unexpected spikes or drops in row counts                               |
| 15 | Distribution Checks                  | Shifts in value distributions vs. history                              |
| 16 | Outlier Detection                    | Statistically extreme values                                           |
| 17 | Schema Drift Detection               | Added, removed, renamed, or retyped columns                            |
| 18 | Duplicate File Ingestion             | The same source file loaded more than once                             |
| 19 | Negative Value Checks                | Negatives where only positives are valid                               |
| 20 | Percentage / Total Consistency       | Parts that don't add up to the whole; percentages outside 0–100        |
| 21 | Hierarchy Validation                 | Broken parent-child chains, cycles, orphaned nodes                     |
| 22 | Audit Column Consistency             | Bad `created_at` / `updated_at` / `load_ts` values                     |
| 23 | Foreign Key Existence Check          | Foreign keys not present in the reference table                        |
| 24 | Date Validity Check                  | Impossible, future, or out-of-order dates                              |
| 25 | PII Masking Validation               | Sensitive fields left unmasked in downstream layers                    |

---

## How Each Chapter Is Structured

Every chapter follows the same template:

- **Purpose**: why the check exists
- **Example**: a healthcare patient table scenario (validated before loading into Silver)
- **Sample SQL**: the check expressed as a query
- **Sample PySpark**: the same check as a DataFrame filter
- **Best Practice**: log failures to a quarantine table with a rejection reason and audit metadata
- **Discussion**: business rule, expected output, production considerations, interview tips, and monitoring metrics

---

## Quick Start Examples

Below are working examples for a sample `bronze.patients` table (`patient_id`, `first_name`, `dob`, `gender`, `email`, `admission_date`, `discharge_date`, `age`). Adapt table and column names to your project.

### 1. Null or Missing Values

```sql
SELECT *
FROM bronze.patients
WHERE patient_id IS NULL
   OR dob IS NULL;
```

```python
from pyspark.sql import functions as F

failed = df.filter(F.col("patient_id").isNull() | F.col("dob").isNull())
```

### 2. Primary Key Uniqueness

```sql
SELECT patient_id, COUNT(*) AS cnt
FROM bronze.patients
GROUP BY patient_id
HAVING COUNT(*) > 1;
```

```python
dupes = (df.groupBy("patient_id")
           .count()
           .filter(F.col("count") > 1))
```

### 3. Duplicate Record Detection

```sql
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY updated_at DESC) AS rn
  FROM bronze.patients
) t
WHERE rn > 1;
```

```python
from pyspark.sql import Window

w = Window.partitionBy("patient_id").orderBy(F.col("updated_at").desc())
dupes = (df.withColumn("rn", F.row_number().over(w))
           .filter(F.col("rn") > 1))
```

### 4 & 23. Referential / Foreign Key Integrity

```sql
SELECT a.*
FROM bronze.admissions a
LEFT JOIN silver.patients p
       ON a.patient_id = p.patient_id
WHERE p.patient_id IS NULL;
```

```python
orphans = admissions.join(patients, "patient_id", "left_anti")
```

### 5. Data Type Validation

```sql
SELECT *
FROM bronze.patients
WHERE TRY_CAST(age AS INT) IS NULL
  AND age IS NOT NULL;
```

```python
failed = (df.withColumn("age_int", F.col("age").cast("int"))
            .filter(F.col("age").isNotNull() & F.col("age_int").isNull()))
```

### 6 & 19. Numeric Range and Negative Value Checks

```sql
SELECT *
FROM bronze.patients
WHERE age < 0 OR age > 120;
```

```python
failed = df.filter((F.col("age") < 0) | (F.col("age") > 120))
```

### 8. Regex Pattern Validation

```sql
SELECT *
FROM bronze.patients
WHERE email IS NOT NULL
  AND NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$';
```

```python
pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
failed = df.filter(F.col("email").isNotNull() & ~F.col("email").rlike(pattern))
```

### 9. Allowed Values / Domain Validation

```sql
SELECT *
FROM bronze.patients
WHERE gender NOT IN ('M', 'F', 'O', 'U');
```

```python
failed = df.filter(~F.col("gender").isin("M", "F", "O", "U"))
```

### 11 & 24. Cross-Column and Date Consistency

```sql
SELECT *
FROM bronze.patients
WHERE discharge_date < admission_date
   OR admission_date > CURRENT_DATE;
```

```python
failed = df.filter(
    (F.col("discharge_date") < F.col("admission_date")) |
    (F.col("admission_date") > F.current_date())
)
```

### 12. Timeliness / Freshness

```sql
SELECT MAX(load_ts) AS last_load,
       CASE WHEN MAX(load_ts) < CURRENT_TIMESTAMP - INTERVAL 24 HOURS
            THEN 'STALE' ELSE 'FRESH' END AS status
FROM bronze.patients;
```

```python
last_load = df.agg(F.max("load_ts")).first()[0]
is_stale = (F.current_timestamp() - F.lit(last_load)) > F.expr("INTERVAL 24 HOURS")
```

### 14. Volume Checks

```sql
SELECT load_date, COUNT(*) AS row_cnt
FROM bronze.patients
GROUP BY load_date
ORDER BY load_date DESC;
```

```python
today = df.filter(F.col("load_date") == F.current_date()).count()
# Compare `today` against a rolling average of previous days and alert on large deviations.
```

### 16. Outlier Detection (IQR)

```python
q1, q3 = df.approxQuantile("length_of_stay", [0.25, 0.75], 0.01)
iqr = q3 - q1
outliers = df.filter(
    (F.col("length_of_stay") < q1 - 1.5 * iqr) |
    (F.col("length_of_stay") > q3 + 1.5 * iqr)
)
```

### 17. Schema Drift Detection

```python
expected = {"patient_id", "first_name", "dob", "gender", "email"}
actual = set(df.columns)

missing = expected - actual
unexpected = actual - expected
```

---

## Quarantine Pattern (Best Practice)

Never silently drop bad records. Route them to a quarantine table with a rejection reason and audit metadata so they can be reviewed, fixed, and replayed.

```python
from pyspark.sql import functions as F

def quarantine(failed_df, rule_name, reason, target="quarantine.patients"):
    (failed_df
        .withColumn("rule_name", F.lit(rule_name))
        .withColumn("rejection_reason", F.lit(reason))
        .withColumn("source_table", F.lit("bronze.patients"))
        .withColumn("rejected_at", F.current_timestamp())
        .withColumn("batch_id", F.lit(BATCH_ID))   # your pipeline run ID
        .write.mode("append").saveAsTable(target))

# Example usage
null_failures = df.filter(F.col("patient_id").isNull())
quarantine(null_failures, "NULL_CHECK_PATIENT_ID", "patient_id is null")

clean_df = df.filter(F.col("patient_id").isNotNull())
```

**Suggested quarantine schema:** original columns + `rule_name`, `rejection_reason`, `source_table`, `batch_id`, `rejected_at`.

---

## Monitoring Metrics

Track these per rule, per run, so quality becomes measurable over time:

- **Failure count** and **failure rate** (`failed / total`)
- **Rows quarantined** per batch
- **Freshness lag** (now minus max load timestamp)
- **Row count delta** vs. previous run / rolling average
- **Schema drift events**
- **Rule execution time**

Consider pushing these to a `dq_results` table and alerting when thresholds are breached.

---

## Repository Structure

```
.
├── README.md
├── Data_Quality_Checks_for_Data_Engineers.pdf
├── sql/                    # SQL version of each check
├── pyspark/                # PySpark version of each check
└── docs/                   # Notes, diagrams, interview tips
```

*Adjust to match your repo layout.*

---

## Contributing

Contributions are welcome.

1. Fork the repo
2. Create a feature branch (`git checkout -b feature/new-check`)
3. Commit your changes (`git commit -m "Add <check name> example"`)
4. Push to your branch (`git push origin feature/new-check`)
5. Open a Pull Request

---

## License

Distributed under the MIT License. See `LICENSE` for details.
