from openai import AzureOpenAI  

import json  

import snowflake.connector



#-------------Set up your Azure OpenAI API key and endpoint----------------  

api_key = "you-api-key"  

api_base = "your-url"  

api_version = "your-version"  # Ensure this is the correct API version  



#---------------Initialize the Azure OpenAI client-------------------------

azure_openai = AzureOpenAI(api_key=api_key, azure_endpoint=api_base, api_version=api_version)  



#----------------Snowflake Connection---------------------

def connect_to_snowflake(user,password,account):

    try:

        conn=snowflake.connector.connect(user=user

                                        ,password=password

                                        ,account=account)

        return conn

    except Exception as e:

        print(f"unable to connect: {e}")



#---------Execute in SnowFlake Function----------------

def execute_in_snowflake(code, conn):

    try:

        cursor=conn.cursor()



        cursor.execute('USE DATABASE GENAI_SANDBOX_DB')

        cursor.execute(code)

        res=cursor.fetchall()



        cursor.close()

        #conn.close()



        #Logging result

        if res:

            print(f"Query Successful: {res}")

        else:

            print("Query returned no results")



        return res

    except snowflake.connector.errors.DatabaseError as e:

        #logging.error(f"Database error:{e}")

        print(f"Database error:{e}")

        return e.msg

    except Exception as e:

        #logging.critical(f"Unexpected error: {e}")

        print(f"Unexpected error: {e}")



#Snowflake Connection Object

user="you-user-name"

password="your-password"

account="your-account"



#-----Utility function to read content from a file-----

def read_file(file_path):    

    with open(file_path, 'r') as file:  

        return file.read()  



       

def construct_messages(file_content):

    # System prompt template with placeholders  



    snowflake_system_prompt_template= """

    ### Task        

    You are a data engineering assistant specialized in PLSQL Procedures and Snowflake SQL. Your task is to read and interpret a procedure. From this procedure, extract the following objects:

    - Source Tables and their Source Columns

    - Target Tables and their Target Columns

   

    ## Ensure:

    - Table names and column names match exactly as found in the Procedure.

    - Return empty arrays if no source or target is found.

    - Do not infer or guess values—only extract what is explicitly present.



    ### Output Format: Return the result strictly in the following JSON structure:

    {

     "sources": [

       {

         "table_name": "SOURCE_TABLE_NAME",

         "columns": ["column1", "column2", "..."]

       }

     ],

     "targets": [

       {

         "table_name": "TARGET_TABLE_NAME",

         "columns": ["column1", "column2", "..."]

       }

     ]

    }

"""

    system_prompt=snowflake_system_prompt_template

    user_prompt=file_content



    # Constructing the messages list for the API call  

    messages = [  

        {"role": "system", "content": system_prompt},  

        {"role": "user", "content": user_prompt}  

    ]  



    return messages  



def chat_with_gpt4(messages, temperature=0.3, top_p=0.6, model="your-model-deployment-name"):  

    try:  

        # Call the Azure OpenAI API for chat completion  

        response = azure_openai.chat.completions.create(  

            model=model,  

            messages=messages,  

            temperature=temperature,  

            top_p=top_p,  

            max_tokens=1500,  

            n=1,  

            frequency_penalty=0,

            presence_penalty=0,

            response_format ={"type":"json_object"},

            stop=None

             

        )  

         

        #---------------Convert the response to JSON----------------  

        # response_json = response.choices[0].message.content.strip()

        # print(response.choices[0])

        #print(response.choices[0].message.content)

        # return json.loads(response_json)  

       

        print("\tResponse before JSON parsing: ",response.choices[0].message.content,"\n\n")

        op_json = json.loads(response.choices[0].message.content) #json.dumps(, indent=4)  

        print(json.dumps(json.loads(response.choices[0].message.content), indent=4))

        return op_json



        #-------------------Determine the count of objects-----------------------  

        # if isinstance(pretty_json, dict):  

            # If it's a dictionary, count the number of keys  

            # object_count = len(pretty_json)  

            # print(f"The JSON contains {object_count} top-level keys (objects).")  

        # elif isinstance(pretty_json, list):  

            # If it's a list, count the number of items  

            # object_count = len(pretty_json)  

            # print(f"The JSON contains {object_count} items (objects) in the list.")  

        # else:  

            # print("The JSON content is neither a dictionary nor a list.")



    except Exception as e:  

        print(f"An error occurred: {e}")  

        return None  



xml_script = read_file(r"C:\Users\2106624\Downloads\informatica to pyspark sample_2\informatica to pyspark demo_sample_2\m_so_stg_addresses.XML")

xml_script = read_file(r"C:\Users\2106624\Downloads\m_ff_cdm_bill_accounts_monthly.xml")

#print(xml_script,"\n\n")



messages = construct_messages(xml_script) #,json_out) #, examples)

response = chat_with_gpt4(messages)



#------------------------------- Generating Test Data ----------------------------------------------------------



get_data_sys_prompt="""Read the given JSON with schema information and write DML, DDL queries in Snowflake for respective tables and its columns.

Write 5 Records for each column in a table.



Output JSON Format:

{

    "DDL": ['CREATE TABLE <table_name>',....],

    "DML": ["INSERT INTO <table_name> (<col1_name>,...) VALUES (REC1,...);",...]

}

"""

get_data_user_prompt=f"""Schema JSON:{response}"""

print("\n\n\tSCHEMA\n",get_data_user_prompt,"\n\n")



messages = [  

        {"role": "system", "content": get_data_sys_prompt},  

        {"role": "user", "content": get_data_user_prompt}  

    ]



response = chat_with_gpt4(messages)



code=response["DML"][0]

print(code)



#-------------------------------Optional: Execute Test Data ----------------------------------------------------------



conn=connect_to_snowflake(user,password,account)

for query in response["DML"]:  

  execute_in_snowflake(query,conn)



# Print the response  

print(json.dumps(response, indent=4))  
