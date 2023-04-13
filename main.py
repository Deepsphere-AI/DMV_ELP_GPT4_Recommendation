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

import pandas as pd
pd.set_option('display.max_colwidth', 500)
import json
import traceback
import requests
import numpy as np
from datetime import date
import datetime
import os
import time
from google.cloud import storage
import  multiprocessing as mp
from DMV_ChatGPT_Recommendation import ELP_Recommendation,ELP_Recommendation_Batch
from DMV_ChatGPT_Response_To_Bigquery import Insert_Response_to_Bigquery
from DMV_Bigquery_Utility import ReadAlreadyProcessedData

def ELP_ChatGPT_Recommendation(request):
   try:
      pool = mp.Pool(mp.cpu_count())
      vAR_timeout_start = time.time()
      vAR_timeout_secs = int(os.environ['TIMEOUT_SECS'])
      vAR_batch_elp_configuration = ReadRequestFile()
      vAR_num_of_configs = len(vAR_batch_elp_configuration)
      print('len1 - ',vAR_num_of_configs)
      # vAR_processed_data = ReadAlreadyProcessedData()
      # print('len2 - ',len(vAR_processed_data))
      # vAR_not_processed_records = vAR_batch_elp_configuration[~vAR_batch_elp_configuration.isin(vAR_processed_data)]
      # print('len3 - ',len(vAR_not_processed_records))

      # print('Processing records, which is not processed already')

      # vAR_number_of_configuration_to_process = len(vAR_not_processed_records)

      # print('No of configurations to process - ',vAR_number_of_configuration_to_process)


      vAR_output_result_objects = [pool.apply_async(ELP_Recommendation_Batch,args=(vAR_batch_elp_configuration,idx,vAR_num_of_configs)) for idx in range(0,vAR_num_of_configs,10)]
      
      for vAR_response in vAR_output_result_objects:
         try:
            print('Time taking in for loop - ',time.time()-vAR_timeout_start)
            if (time.time()-vAR_timeout_start)<vAR_timeout_secs:
               print('get - ',vAR_response.get())
               vAR_result_df = vAR_response.get()
               for index,row in vAR_result_df.iterrows():
                  vAR_response = row["MODEL_RESULT"]
                  vAR_dict1_start_index = vAR_response.index('{')
                  vAR_dict1_end_index = vAR_response.index('}')


                  vAR_dict2_start_index = vAR_response.rfind('{')
                  vAR_dict2_end_index = vAR_response.rfind('}')

                  vAR_result_dict = vAR_response[vAR_dict1_start_index:vAR_dict1_end_index+1]
                  vAR_conclusion_dict = vAR_response[vAR_dict2_start_index:vAR_dict2_end_index+1]

                  vAR_result_df = pd.DataFrame(json.loads(vAR_result_dict))
                  vAR_conclusion_df = pd.DataFrame(json.loads(vAR_conclusion_dict))

                  Insert_Response_to_Bigquery(row["ORDER_CONFIGURATION"],row["ORDER_PRINTED_DATE"],row["REQUEST_ID"],row["REQUEST_DATE"],vAR_result_df,vAR_conclusion_df)
            else:
               print('Timeout Error inside result iteration')
               return 'Timeout Error inside result iteration'
         except BaseException as e:
            print('Error in processing records - ',str(e))
            print('Error Traceback in for loop - '+str(traceback.print_exc()))
      return "Process Successfully Completed!"

   except BaseException as e:
     print('Error in main - ',str(e))
     print('Error Traceback - '+str(traceback.print_exc()))
     return "Something went wrong!"
    










def ReadRequestFile():
   vAR_utc_time = datetime.datetime.utcnow()
   vAR_gcs_request_file_df = pd.DataFrame()
   vAR_storage_client = storage.Client(project=os.environ["GCP_PROJECT_ID"])
   vAR_bucket = vAR_storage_client.get_bucket(os.environ["GCS_BUCKET_NAME"])
   # vAR_prefix = os.environ["GCP_REQUEST_PATH"]+"/"+vAR_utc_time.strftime('%Y%m%d')+"/"
   vAR_prefix = os.environ["GCP_REQUEST_PATH"]+"/"+os.environ["REQUEST_DATE"]+"/"
   
   for blob in vAR_bucket.list_blobs(prefix=vAR_prefix):
      print('BLOB - ',blob)
      print('BLOB NAME - ',blob.name)
      if blob.name.endswith('.csv') or blob.name.endswith('.CSV'):
         vAR_request_object_name = "gs://"+os.environ["GCS_BUCKET_NAME"]+"/"+blob.name
         vAR_gcs_request_file_df=  pd.read_csv(vAR_request_object_name)
         vAR_gcs_request_file_df = vAR_gcs_request_file_df[['REQUEST_ID','REQUEST_DATE','LICENSE_PLATE_CONFIG','ORDER_PRINTED_DATE']]
         vAR_gcs_request_file_df.rename(columns={'LICENSE_PLATE_CONFIG':'ORDER_CONFIGURATION'},inplace=True)
      else:
         print('No Request file found in GCS bucket with given date&filename')
   return vAR_gcs_request_file_df