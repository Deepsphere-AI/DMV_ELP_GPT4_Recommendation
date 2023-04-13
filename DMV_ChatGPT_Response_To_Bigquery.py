"""
-----------------------------------------------------------------------------------------------------------------------------------------------------
© Copyright 2022, California, Department of Motor Vehicle, all rights reserved.
The source code and all its associated artifacts belong to the California Department of Motor Vehicle (CA, DMV), and no one has any ownership
and control over this source code and its belongings. Any attempt to copy the source code or repurpose the source code and lead to criminal
prosecution. Don't hesitate to contact DMV for further information on this copyright statement.

Release Notes and Development Platform:
The source code was developed on the Google Cloud platform using Google Cloud Functions serverless computing architecture. The Cloud
Functions gen 2 version automatically deploys the cloud function on Google Cloud Run as a service under the same name as the Cloud
Functions. The initial version of this code was created to quickly demonstrate the role of MLOps in the ELP process and to create an MVP. Later,
this code will be optimized, and Python OOP concepts will be introduced to increase the code reusability and efficiency.
____________________________________________________________________________________________________________
Development Platform                | Developer       | Reviewer   | Release  | Version  | Date
____________________________________|_________________|____________|__________|__________|__________________
Google Cloud Serverless Computing   | DMV Consultant  | Ajay Gupta | Initial  | 1.0      | 09/18/2022

-----------------------------------------------------------------------------------------------------------------------------------------------------
"""

import datetime
from google.cloud import bigquery
import os
import pandas as pd



def Insert_Response_to_Bigquery(vAR_configuration,vAR_order_date,vAR_request_id,vAR_request_date,vAR_result_df,vAR_conclusion_df):
    vAR_df = pd.DataFrame()
    created_at = []
    created_by = []
    updated_at = []
    updated_by = []
    created_at += 1 * [datetime.datetime.utcnow()]
    created_by += 1 * [os.environ['GCP_USER']]
    updated_by += 1 * [os.environ['GCP_USER']]
    updated_at += 1 * [datetime.datetime.utcnow()]
    vAR_df['CREATED_DT'] = created_at
    vAR_df['CREATED_USER'] = created_by
    vAR_df['UPDATED_DT'] = updated_at
    vAR_df['UPDATED_USER'] = updated_by

    # Load client
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])

   
    table = os.environ["GCP_BQ_SCHEMA_NAME"]+'.DMV_ELP_GPT4_RECOMMENDATION'

   #  parse_date reference link - https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements

    for index,row in vAR_result_df.iterrows():
      if row["Category"]=='Profanity':
        vAR_profanity_score = row["Probability"]
        vAR_profanity_details = row["Reason"]
      elif row["Category"]=='Obscene':
        vAR_obscene_score = row["Probability"]
        vAR_obscene_details = row["Reason"]
      elif row["Category"]=='Toxic':
        vAR_toxic_score = row["Probability"]
        vAR_toxic_details = row["Reason"]
      elif row["Category"]=='Threat':
        vAR_threat_score = row["Probability"]
        vAR_threat_details = row["Reason"]
      elif row["Category"]=='Insult':
        vAR_insult_score = row["Probability"]
        vAR_insult_details = row["Reason"]
      elif row["Category"]=='Hate':
        vAR_hate_score = row["Probability"]
        vAR_hate_details = row["Reason"]
    for index,row in vAR_conclusion_df.iterrows():
      vAR_approved_denied = row["Conclusion"]
      vAR_conclusion_reason = row["Conclusion Reason"]
      vAR_recommended_config = row["Recommended Configuration"]
      vAR_recommendation_reason = row["Recommendation Reason"]

    vAR_query = """Insert into `{}` (REQUEST_ID,REQUEST_DATE,ORDER_CONFIGURATION,ORDER_PAYMENT_DATE,TOXIC_SCORE,TOXIC_SCORE_DETAILS,PROFANITY_SCORE,PROFANITY_SCORE_DETAILS,INSULT_SCORE,INSULT_SCORE_DETAILS,HATE_SCORE,HATE_SCORE_DETAILS,THREAT_SCORE,THREAT_SCORE_DETAILS,OBSCENE_SCORE,OBSCENE_SCORE_DETAILS,APPROVED_OR_DENIED,DENIAL_REASON,RECOMMENDED_CONFIGURATION,RECOMMENDATION_REASON,CREATED_DT,CREATED_USER,UPDATED_DT,UPDATED_USER) values ({},"{}","{}",parse_date('%m/%d/%E4Y',"{}"),{},"{}",{},"{}",{},"{}",{},"{}",{},"{}",{},"{}","{}","{}","{}","{}","{}","{}","{}","{}")""".format(table,vAR_request_id,vAR_request_date,vAR_configuration,vAR_order_date,vAR_toxic_score,vAR_toxic_details,vAR_profanity_score,vAR_profanity_details,vAR_insult_score,vAR_insult_details,vAR_hate_score,vAR_hate_details,vAR_threat_score,vAR_threat_details,vAR_obscene_score,vAR_obscene_details,vAR_approved_denied,vAR_conclusion_reason,vAR_recommended_config,vAR_recommendation_reason,vAR_df['CREATED_DT'].to_string(index=False),vAR_df['CREATED_USER'].to_string(index=False),vAR_df['UPDATED_DT'].to_string(index=False),vAR_df['UPDATED_USER'].to_string(index=False))
    
    print('Insert response table query - ',vAR_query)

    vAR_job = client.query(vAR_query)
    vAR_job.result()
    vAR_num_of_inserted_row = vAR_job.num_dml_affected_rows
    print('Number of rows inserted into response table - ',vAR_num_of_inserted_row)
