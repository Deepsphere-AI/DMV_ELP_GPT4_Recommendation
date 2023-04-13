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

import openai
import os
import pandas as pd
import re

openai.api_key = os.environ["API_KEY"]



def ELP_Recommendation(vAR_batch_elp_configuration,idx):        
    vAR_input_dict = vAR_batch_elp_configuration.iloc[idx].to_dict()
    vAR_input = vAR_input_dict["ORDER_CONFIGURATION"]
    vAR_request_id = vAR_input_dict["REQUEST_ID"]
    vAR_request_date = vAR_input_dict["REQUEST_DATE"]
    vAR_order_date =vAR_input_dict["ORDER_PRINTED_DATE"]
    vAR_input_config = vAR_input.replace('/','')
    vAR_input_config = vAR_input_config.replace('*','')
    vAR_input_config = vAR_input_config.replace('#','')
    vAR_input_config = vAR_input_config.replace('$','')
    response = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[
        {"role": "user", "content": """Consider a california dmv customer applying new licese plate configuration. Perform below tasks for given word as below format:
1.Please Provide the probability value and detailed explanation for each of the categories (profanity, obscene, insult, hate, toxic, threat).
2.Deny the configuration if any one of the above categories probability value is greater than 0.2. Otherwise, accept the configuration.
3.If it's denied, recommend new configuration which must not represent/fall any of the profanity,insult,hate,threat,obscene,toxic categories and the configuration length must be less than 8 characters. Also, provide the recommended configuration reason, why it is recommended? If it's accepted no recommendation needed.

Given configuration is : 'RAC1SM'
{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.2,0.3,0.8,0.9,0.7,0.1],"Reason":["'RAC1SM' is not considered profane language.","'RAC1SM' is not considered to be an obscene expression.","'RAC1SM' can be used as an insult, depending on the context.","'RAC1SM' is a term associated with hate speech and discrimination.","'RAC1SM' can be used in a toxic manner, depending on the context.","'RAC1SM' is not typically used to express a threat."]}
{"Conclusion": ["Denied"],"Conclusion Reason":["The configuration 'RAC1SM' is DENIED as the probability value of Hate is greater than 0.2."],"Recommended Configuration":["UNITY"],"Recommendation Reason":["The configuration 'UNITY' represents a positive message of togetherness and does not represent/fall any of the profanity,insult,hate,threat,obscene,toxic categories and the configuration length is less than 8 characters."]}
Given Configuration is : 'GOOD'
{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.0,0.0,0.0,0.0,0.0,0.0],"Reason":["'GOOD' is not considered profane language.","'GOOD' is not considered to be an obscene expression.","'GOOD' is not typically used as an insult.","'GOOD' is not associated with hate speech and discrimination.","'GOOD' is not typically used in a toxic manner.","'GOOD' is not typically used to express a threat."]}
{"Conclusion": ["Accepted"],"Conclusion Reason":["N/A"],"Recommended Configuration":["N/A"],"Recommendation Reason":["N/A"]}
Given configuration is : 'AE51***/'
{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.0,0.0,0.0,0.0,0.0,0.0],"Reason":["'AE51***/' is not a profanity word.","'AE51***/' is not an obscene word.","'AE51***/' is not an insulting word.","'AE51***/' is not a word which is used to express hatred.","'AE51***/' is not a toxic word."," 'AE51***/' is not a threatening word."]}
{"Conclusion": ["Accepted"],"Conclusion Reason":["N/A"],"Recommended Configuration":["N/A"],"Recommendation Reason":["N/A"]}
Given configuration is :'"""+vAR_input_config.upper()+"'"},
    ],
    temperature=0,
    max_tokens=1000,
    top_p=1,
    frequency_penalty=0,
    presence_penalty=0.9,

)
    print(response['choices'][0]['message']['content'])
    return vAR_input,vAR_order_date,vAR_request_id,vAR_request_date,response['choices'][0]['message']['content']



def ELP_Recommendation_Batch(vAR_batch_elp_configuration,idx,vAR_num_of_configs):  
    vAR_order_config_list = []
    vAR_processed_order_config_list = []
    vAR_order_date_list = []
    vAR_request_id_list = []
    vAR_request_date_list = []
    vAR_model_result_list = []
    vAR_idx = 0
    str_idx = 0
    vAR_result_df = pd.DataFrame()
    print('idx - ',idx)
    for i in range(idx,idx+10):
       if i<vAR_num_of_configs:
         vAR_order_config_list.append(vAR_batch_elp_configuration.iloc[i].to_dict()["ORDER_CONFIGURATION"])
         vAR_processed_order_config_list.append(vAR_batch_elp_configuration.iloc[i].to_dict()["ORDER_CONFIGURATION"].replace('/','').replace('*','').replace('#','').replace('$','').upper())
         vAR_order_date_list.append(vAR_batch_elp_configuration.iloc[i].to_dict()["ORDER_PRINTED_DATE"])
         vAR_request_id_list.append(vAR_batch_elp_configuration.iloc[i].to_dict()["REQUEST_ID"])
         vAR_request_date_list.append(vAR_batch_elp_configuration.iloc[i].to_dict()["REQUEST_DATE"])
    
    response = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[
        {"role": "user", "content": """Consider a california dmv customer applying new licese plate configuration. Perform below tasks for given word as below format:
1.Please Provide the probability value and detailed explanation for each of the categories (profanity, obscene, insult, hate, toxic, threat).
2.Deny the configuration if any one of the above categories probability value is greater than 0.2. Otherwise, accept the configuration.
3.If it's denied, recommend new configuration which must not represent/fall any of the profanity,insult,hate,threat,obscene,toxic categories and the configuration length must be less than 8 characters. Also, provide the recommended configuration reason, why it is recommended? If it's accepted no recommendation needed.

Given configurations are : ['RAC1SM','GOOD','AE51']

{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.2,0.3,0.8,0.9,0.7,0.1],"Reason":["'RAC1SM' is not considered profane language.","'RAC1SM' is not considered to be an obscene expression.","'RAC1SM' can be used as an insult, depending on the context.","'RAC1SM' is a term associated with hate speech and discrimination.","'RAC1SM' can be used in a toxic manner, depending on the context.","'RAC1SM' is not typically used to express a threat."]}
{"Conclusion": ["Denied"],"Conclusion Reason":["The configuration 'RAC1SM' is DENIED as the probability value of Hate is greater than 0.2."],"Recommended Configuration":["UNITY"],"Recommendation Reason":["The configuration 'UNITY' represents a positive message of togetherness and does not represent/fall any of the profanity,insult,hate,threat,obscene,toxic categories and the configuration length is less than 8 characters."]}

{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.0,0.0,0.0,0.0,0.0,0.0],"Reason":["'GOOD' is not considered profane language.","'GOOD' is not considered to be an obscene expression.","'GOOD' is not typically used as an insult.","'GOOD' is not associated with hate speech and discrimination.","'GOOD' is not typically used in a toxic manner.","'GOOD' is not typically used to express a threat."]}
{"Conclusion": ["Accepted"],"Conclusion Reason":["N/A"],"Recommended Configuration":["N/A"],"Recommendation Reason":["N/A"]}

{"Category":["Profanity","Obscene","Insult","Hate","Toxic","Threat"],"Probability":[0.0,0.0,0.0,0.0,0.0,0.0],"Reason":["'AE51' is not a profanity word.","'AE51' is not an obscene word.","'AE51' is not an insulting word.","'AE51' is not a word which is used to express hatred.","'AE51' is not a toxic word."," 'AE51' is not a threatening word."]}
{"Conclusion": ["Accepted"],"Conclusion Reason":["N/A"],"Recommended Configuration":["N/A"],"Recommendation Reason":["N/A"]}
Given configurations are : """+str(vAR_processed_order_config_list)},
    ],
    temperature=0,
    max_tokens=2000,
    top_p=1,
    frequency_penalty=0,
    presence_penalty=0.9,

)
    vAR_model_result = response['choices'][0]['message']['content']

    print('GPT4 response - ',response)

    
    for m in re.finditer('}', vAR_model_result):
       str_idx2 = m.start()
       vAR_idx+=1
       if vAR_idx%2==0:
          vAR_model_result_list.append(vAR_model_result[str_idx:str_idx2+1])
          str_idx = m.end()
    
    vAR_result_df = pd.DataFrame({'ORDER_CONFIGURATION':vAR_order_config_list,'ORDER_PRINTED_DATE':vAR_order_date_list,'REQUEST_ID':vAR_request_id_list,'REQUEST_DATE':vAR_request_date_list,'MODEL_RESULT':vAR_model_result_list})
    print('GPT4 Model Result - ',vAR_model_result)
    return vAR_result_df