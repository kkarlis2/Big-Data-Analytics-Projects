# Ultra Race Data Warehouse

A Spark-based data warehouse project for analyzing ultra-distance running races (50km, 50mi, 100km, 100mi) using a star schema.

## 🔧 Technologies
- Apache Spark (Scala/Python)
- Power BI

## 📊 Functionality

- ETL pipeline to transform raw race data (`raceData.txt`) into a dimensional star schema.
- Output stored as separate CSVs for use in Power BI.
- Five analytical queries:
  - Number of races per country/year.
  - Avg. finish time per age category (50km).
  - Greek runners per year.
  - Fastest race (by avg speed) per distance.
  - Cube with participation counts by country, distance, and gender.

## 📈 Power BI

- One visual per analytical report.
- A custom dashboard highlighting trends and performance across time, demographics, and geography.

## 📂 Output

- CSVs for each dimension & fact table.
- Graphs and dashboard in PDF format.
