
# ✈️ Flight Data Analysis with PySpark

## 📌 Overview
This project demonstrates how to use **PySpark** to process and analyze large datasets.  
We use a **flight dataset** containing millions of rows and perform **data cleaning, transformations, and analytics** to extract meaningful insights.

---

## 🛠️ Tech Stack
- Python
- Apache Spark (PySpark)
- Pandas (for comparison and small-scale testing)
- Jupyter Notebook

---
```plaintext
## 📂 Project Structure
flight-data-analysis/
├── data/
│ └── flights.csv # Dataset 
├── src/
│ └── flight_analysis.py # Main PySpark script
├── README.md
└── requirements.txt
```


---

## 📊 Key Tasks Performed
- **Data Loading**: Load raw CSV into PySpark DataFrame.
- **Data Cleaning**: Handle null values, remove duplicates, filter invalid rows.
- **Transformations**:
  - Extract `year`, `month`, and `day` from dates.
  - Create new columns like `delay_category`.
- **Aggregations**:
  - Average flight delay per airline.
  - Top 5 airports with most delays.
  - On-time vs delayed flight percentage.
- **Window Functions**:
  - Rank airlines by average delay.
  - Find busiest airports.
- **Output**:
  - Save results to **CSV** and **Parquet** formats.

---

## 🚀 How to Run
1. Clone the repository:
   ```bash
   git clone https://github.com/jgarij/flight-data-analysis.git
   cd flight-data-analysis

2.Install dependencies:
```bash
   pip install -r requirements.txt


3.Run the PySpark script:
```bash
spark-submit src/flight_analysis.py


4.View results in the output/ folder.

📈 Sample Insights

Airline AA had the highest average delay of 15.2 minutes.

Airport ATL was the busiest with 120K+ flights.

78% of flights arrived on time across the dataset.

🏆 Achievements

Processed 1M+ records efficiently with PySpark.

Demonstrated ETL pipeline concepts: extract, clean, transform, and load.

Showcased ability to write scalable, optimized PySpark code.

This project is end-to-end:

Dataset ingestion → cleaning → transformation → analytics → storage.