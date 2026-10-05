from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_DIR = "/workspaces/NexPay-Analytics"
OUT_DIR = os.path.join(PROJECT_DIR, "pipeline_output")
STAGING = os.path.join(OUT_DIR, "staging")
METRICS = os.path.join(OUT_DIR, "metrics")


def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    msg = f"ALERT: task '{task_id}' failed in run {context['run_id']}"
    print(msg)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "alerts.log"), "a") as f:
        f.write(f"{datetime.now()} {msg}\n")


def extract():
    os.makedirs(STAGING, exist_ok=True)
    for name in ["transaction_data", "campaign_data", "customer_data"]:
        df = pd.read_csv(os.path.join(PROJECT_DIR, f"{name}.csv"))
        if df.empty:
            raise ValueError(f"{name}.csv is empty")
        df.to_csv(os.path.join(STAGING, f"{name}_raw.csv"), index=False)
        print(f"Extracted {len(df):,} rows from {name}.csv")


def validate_and_clean():
    tx = pd.read_csv(os.path.join(STAGING, "transaction_data_raw.csv"))
    before = len(tx)
    tx = tx.drop_duplicates()
    tx = tx.dropna(subset=["transaction_amount", "net_revenue"])
    tx = tx[tx["transaction_amount"] > 0]
    print(f"Transactions: {before:,} rows in, {len(tx):,} rows after cleaning")
    if len(tx) < 0.95 * before:
        raise ValueError("Over 5% of rows removed. Check the source data.")
    tx.to_csv(os.path.join(STAGING, "transactions_clean.csv"), index=False)
    for name in ["campaign_data", "customer_data"]:
        df = pd.read_csv(os.path.join(STAGING, f"{name}_raw.csv")).drop_duplicates()
        df.to_csv(os.path.join(STAGING, f"{name}_clean.csv"), index=False)


def compute_metrics():
    os.makedirs(METRICS, exist_ok=True)
    tx = pd.read_csv(os.path.join(STAGING, "transactions_clean.csv"))
    camp = pd.read_csv(os.path.join(STAGING, "campaign_data_clean.csv"))
    cust = pd.read_csv(os.path.join(STAGING, "customer_data_clean.csv"))

    channel_cols = [c for c in tx.columns if "channel" in c.lower() or "method" in c.lower()]
    if not channel_cols:
        raise ValueError(f"No payment channel column found in: {list(tx.columns)}")
    channel = channel_cols[0]

    by_channel = (
        tx.groupby(channel)
        .agg(transactions=("transaction_amount", "count"),
             volume=("transaction_amount", "sum"),
             net_revenue=("net_revenue", "sum"),
             approval_rate=("is_approved", "mean"))
        .round(2)
        .sort_values("net_revenue", ascending=False)
    )
    by_channel.to_csv(os.path.join(METRICS, "revenue_by_channel.csv"))

    roas = camp.groupby("campaign").agg(revenue=("revenue", "sum"), cost=("cost", "sum"))
    roas["roas"] = (roas["revenue"] / roas["cost"]).round(2)
    roas.sort_values("roas", ascending=False).to_csv(os.path.join(METRICS, "roas_by_campaign.csv"))

    churn = (
        cust.groupby("segment")
        .agg(customers=("customer_id", "count"),
             avg_churn_risk=("churn_risk", "mean"),
             avg_spend=("total_spend", "mean"))
        .round(3)
        .sort_values("avg_churn_risk", ascending=False)
    )
    churn.to_csv(os.path.join(METRICS, "churn_by_segment.csv"))
    print("Metrics written: revenue by channel, ROAS by campaign, churn by segment")


def publish_summary():
    files = ["revenue_by_channel.csv", "roas_by_campaign.csv", "churn_by_segment.csv"]
    for f in files:
        if not os.path.exists(os.path.join(METRICS, f)):
            raise FileNotFoundError(f"Missing output: {f}")
    top_channel = pd.read_csv(os.path.join(METRICS, files[0])).iloc[0, 0]
    top_campaign = pd.read_csv(os.path.join(METRICS, files[1])).iloc[0, 0]
    riskiest = pd.read_csv(os.path.join(METRICS, files[2])).iloc[0, 0]
    summary = (f"Run at {datetime.now():%Y-%m-%d %H:%M}\n"
               f"Top revenue channel: {top_channel}\n"
               f"Best ROAS campaign: {top_campaign}\n"
               f"Highest churn risk segment: {riskiest}\n")
    with open(os.path.join(OUT_DIR, "run_summary.txt"), "w") as f:
        f.write(summary)
    print(summary)


default_args = {
    "owner": "priyanka",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="nexpay_daily_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["nexpay"],
) as dag:
    t_extract = PythonOperator(task_id="extract", python_callable=extract)
    t_clean = PythonOperator(task_id="validate_and_clean", python_callable=validate_and_clean)
    t_metrics = PythonOperator(task_id="compute_metrics", python_callable=compute_metrics)
    t_publish = PythonOperator(task_id="publish_summary", python_callable=publish_summary)

    t_extract >> t_clean >> t_metrics >> t_publish