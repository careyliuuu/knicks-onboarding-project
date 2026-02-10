from airflow.sdk import Asset

# Define the Asset once here. Both DAGs will import it.
KNICKS_ASSET = Asset(name="knicks_prediction", uri="redshift://knicks_predictions")