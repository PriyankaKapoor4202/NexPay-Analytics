# NexPay Analytics — Payments Intelligence Platform

![Python](https://img.shields.io/badge/Python-3.9+-blue) ![Streamlit](https://img.shields.io/badge/Streamlit-Live-red) ![Groq](https://img.shields.io/badge/Groq-LLaMA3-purple) ![Transactions](https://img.shields.io/badge/Transactions-100K-brightgreen)

> Next-generation payments intelligence platform with an AI agent that answers natural language questions about transaction data, campaign performance, and customer churn in real time.

## Live Demo
**[Open NexPay Analytics](https://agentic-marketing-analytics-lq78mplj3t5qf3c5afebph.streamlit.app)**

---

## Overview

NexPay Analytics is a fintech intelligence platform that analyzes 100,000 payment transactions, 50,000 customers, and 6 campaign types. The AI agent powered by Groq LLaMA3 answers natural language business questions in under two seconds — no SQL required.

Ask it anything:
- "Which payment channel has the best ROAS?"
- "Which customer segment is at highest churn risk?"
- "Where should we increase marketing budget?"

---

## Platform Features

### Executive Dashboard
- KPI cards — total volume, net revenue, approval rate, avg transaction
- Transaction volume by payment channel
- Monthly revenue trend
- Customer segment risk matrix
- Top merchants by volume

### AI Analytics Agent
- Full screen chat interface
- Natural language queries answered in real time
- Suggested questions for quick insights
- Every answer grounded in live data

### Transaction Explorer
- Filter 100,000 transactions by channel, merchant, and status
- Real time metric recalculation on filter
- Raw data table with export capability

### Campaign Intelligence
- ROAS ranking by campaign type
- Conversion vs revenue scatter analysis
- Full campaign performance summary table

---

## How The AI Agent Works

1. User asks a natural language question
2. System computes live metrics from the dataset — revenue by channel, top merchants, churn risk by segment, ROAS by campaign
3. Metrics are injected as structured context into a prompt
4. Prompt sent to Groq running LLaMA3 on custom LPU hardware
5. Model responds like a senior analyst in under two seconds
6. Every answer includes a specific recommendation

---
## Data Pipeline (Apache Airflow)

The metrics behind the dashboard are produced by an Airflow DAG, `nexpay_daily_pipeline` (see `dags/nexpay_pipeline.py`), scheduled to run daily.

extract → validate_and_clean → compute_metrics → publish_summary

- **extract**: loads transaction, campaign, and customer data and fails fast if any source is empty
- **validate_and_clean**: removes duplicates, missing values, and invalid amounts; fails the run if more than 5% of rows are dropped, so bad source data never reaches the metrics
- **compute_metrics**: revenue and approval rate by payment channel, ROAS by campaign, churn risk by customer segment
- **publish_summary**: verifies every output exists and writes a run summary of the key findings

**Reliability:** each task retries twice with a 1-minute delay, and a failure callback logs an alert when retries are exhausted. Downstream tasks only run when upstream tasks succeed.

Run it locally:

    pip install -r requirements-pipeline.txt
    airflow dags test nexpay_daily_pipeline

## Tech Stack

- **Python** — core language
- **Streamlit** — web application and deployment
- **Groq API** — LLaMA3 inference on LPU hardware
- **Pandas / NumPy** — data processing and aggregation
- **Plotly** — interactive charts and visualizations
- **python-dotenv** — environment variable management
- **Apache Airflow**: pipeline orchestration, scheduling, retries, and failure alerts

---

## Dataset

Synthetic fintech data generated to simulate realistic payment patterns:

| Dataset | Records | Description |
|---------|---------|-------------|
| transaction_data.csv | 100,000 | Payment transactions across 5 channels and 18 merchants |
| campaign_data.csv | 72 | Monthly performance for 6 campaign types |
| customer_data.csv | 50,000 | Customer segments, spend, and churn risk |

Transaction amounts follow log-normal distribution to simulate realistic payment behavior. ROAS values are calibrated to realistic fintech ranges of 2x to 15x.

---

## Why This Project

Payments analytics teams spend hours writing SQL queries to answer recurring business questions. The insight exists in the data but extracting it requires technical expertise most business stakeholders do not have. NexPay Analytics removes that barrier — any stakeholder can ask a question in plain English and get a data-grounded answer instantly.

---

## Built By

**Priyanka Kapoor** — MS Business Analytics, Montclair State University 2026

[LinkedIn](https://www.linkedin.com/in/priyanka-kapoor-analytics) • [GitHub](https://github.com/PriyankaKapoor4202) • [Research Paper](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=6669899) • [MedScan AI](https://breast-cancer-detection-ai-wnn9lf9wtmywjaeuhckuv9.streamlit.app)
