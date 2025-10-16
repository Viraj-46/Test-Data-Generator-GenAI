import xml.etree.ElementTree as ET
import json
from openai import AzureOpenAI  
 
# Set up your Azure OpenAI API key and endpoint  
api_key = "your-api-key"  
api_base = "you-url"  
api_version = "you-version"  # Ensure this is the correct API version  
 
# Initialize the Azure OpenAI client  
azure_openai = AzureOpenAI(api_key=api_key, azure_endpoint=api_base, api_version=api_version)  
 
#-------Utility function to read content from a file--------
def read_file(file_path):    
    with open(file_path, 'r') as file:  
        return file.read()  
 
 
#------------------------------- XML Parser ----------------------------------------------------------
def extract_schema_from_xml(xml_file_path):
   tree = ET.parse(xml_file_path)
   root = tree.getroot()
   sources = []
   targets = []
   for source in root.findall(".//SOURCE"):
       table = {
           'type': 'Source',
           'name': source.get('NAME') or 'Unknown_Source',
           'fields': []
       }
       for field in source.findall(".//SOURCEFIELD"):
           table['fields'].append({
               'col_name': field.get('NAME'),
               'datatype': field.get('DATATYPE')
           })
       sources.append(table)
   for target in root.findall(".//TARGET"):
       table = {
           'type': 'Target',
           'name': target.get('NAME') or 'Unknown_Target',
           'fields': []
       }
       for field in target.findall(".//TARGETFIELD"):
           table['fields'].append({
               'col_name': field.get('NAME'),
               'datatype': field.get('DATATYPE')
           })
       targets.append(table)
 
   source_dict = { src['name']: src['fields'] for src in sources }
   target_dict = { tgt['name']: tgt['fields'] for tgt in targets }
   schema_json={"sources":source_dict,"targets":target_dict}
   return schema_json
 
#-----------------------GPT 4o Function-------------------------------------------
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
 
        # --------------Convert the response to JSON----------------  
        # response_json = response.choices[0].message.content.strip()
        # print(response.choices[0])
        #print(response.choices[0].message.content)
        # return json.loads(response_json)  
       
        print(response.choices[0].message.content)
        return response.choices[0].message.content
 
        #------------- Determine the count of objects -------------------------
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
 
 
xml_script_path =r"C:\Users\2106624\Downloads\22-xml-files\InfaPC9.6_ApacheSparkPython_cmplx.XML"
 
schema_json=extract_schema_from_xml(xml_script_path)
 
print(json.dumps(schema_json, indent=4))
#print("\tCount of Target:"+str(key)+f'{len(schema_json["targets"][str(key)])}\n' for key in schema_json["targets"].keys())
 
#--------------Count of Target Columns for each Target Table-----------------------
for key in schema_json["targets"].keys():
     print("\tCount of Target:"+key+f'  {len(schema_json["targets"][key])}\n')
 
#------------------------------- Verify JSON for missing columns ----------------------------------------------------------
 
system_prompt="""
    ### Task        
    You are a data engineering assistant specialized in ETL systems and Informatica PowerCenter specifications. Your task is:
        Read and interpret the given JSON, then read and interpret the given XML file to find any missing:
          - Source Tables and Source Columns
          - Target Tables and Target Columns
        That are not present in the JSON.
   
    ## Ensure:
    - Table names and column names match exactly as found in the XML.
    - Do not infer or guess values—only extract what is explicitly present.
   
    ### Output Format: Return the result in the below JSON format
    {"Source":[{"Table_name":["col_name",...]},...],"Target":[{"Table_name":["col_name",...]},...]}
    """
 
xml_script = read_file(r"C:\Users\2106624\Downloads\m_ff_cdm_bill_accounts_monthly.xml")
 
user_prompt=f"JSON:{schema_json}\n\nXML file:{xml_script}"
 
messages = [  
        {"role": "system", "content": system_prompt},  
        {"role": "user", "content": user_prompt}  
    ]  
 
#chat_with_gpt4(messages)
