from google.cloud import bigquery
import pandas as pd
import os


def ReadAlreadyProcessedData():
    vAR_client = bigquery.Client()
    vAR_table_name = "DMV_ELP_GPT4_RECOMMENDATION"
    vAR_sql =(
        "select REQUEST_ID,REQUEST_DATE,ORDER_CONFIGURATION,ORDER_PAYMENT_DATE from `"+ os.environ["GCP_PROJECT_ID"]+"."+os.environ["GCP_BQ_SCHEMA_NAME"]+"."+vAR_table_name+"`"
    )

    vAR_df = vAR_client.query(vAR_sql).to_dataframe()
    return vAR_df