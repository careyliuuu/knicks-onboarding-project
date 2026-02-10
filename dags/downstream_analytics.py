from airflow.decorators import dag, task
from airflow.sdk import Asset
from datetime import datetime

# 1. Define the SAME asset (this is the "Logical Breakpoint")
KNICKS_ASSET = Asset(name="knicks_prediction", uri="redshift://knicks_predictions")

# 2. Runs only when asset is updated
@dag(
    schedule=[KNICKS_ASSET],  # Event-driven architecture
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['downstream']
)
def knicks_analytics_dashboard():
    
    @task
    def refresh_dashboard():
        print("Knicks data updated! Refreshing Executive Dashboard...")

    refresh_dashboard()

knicks_analytics_dashboard()