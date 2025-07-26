# 🚖 PySpark Taxi Trip Data Analysis

This project is part of a big data internship assignment where dummy taxi trip data is analyzed using PySpark. The workflow involves data cleaning, transformation, aggregation, and visualization.

---

## 📂 Project Structure

```
├── dummy_data.csv                # Input data (1000 rows of synthetic taxi trip data)
├── lava.py                       # Main PySpark script for analysis
├── monthly_summary.txt           # Cleaned and grouped output summary (text)
├── monthly_summary.png           # Graphical visualization (Average Fare & Trip Count)
├── Internship_BigData_Report.docx # Final report for submission
└── README.md                     # This file
```

---

## ⚙️ Tools Used

- **Python 3.x**
- **PySpark**
- **Pandas** (for graph generation)
- **Matplotlib** (for plotting)
- **Jupyter / VS Code** (optional for development)

---

## 📊 Features

- Reads local `.csv` file using PySpark
- Filters out invalid fare and distance entries
- Extracts year and month from pickup timestamp
- Aggregates by month to compute:
  - Average fare amount
  - Total trip count
- Outputs clean summary to terminal and `.txt`
- Generates line + bar graph using matplotlib

---

## 🚀 How to Run

1. Ensure Python and Java are installed
2. Install required packages:
```bash
pip install pandas matplotlib findspark
```

3. Run the script:
```bash
python lava.py
```

---

## 📁 Output Files

- `monthly_summary.txt` - Tabular monthly summary
- `monthly_summary.png` - Fare and trip visualization
- `Internship_BigData_Report.docx` - Final project report

---

## 🧠 Credits

Developed as part of an academic internship under **ESAIP Engineering School** for the **Big Data** subject. Supervised by **[Professor’s Name]**.

---
