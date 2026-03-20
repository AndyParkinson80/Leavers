# Standard library
import os
import re
import ast
import math
import time
import json
import tempfile
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

# Third-party libraries
import requests
import pandas as pd
import openpyxl
from dateutil.relativedelta import relativedelta

# Google Cloud SDK
from google.cloud import secretmanager, bigquery
from google.auth import default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2 import service_account

debug = False                                                           
Data_export = False                                                                 #True --> export data to data store
testing = False
gcloud = True                                                                      #True --> Pulls all data from ADP WFN, not just current

directory = Path(__file__).resolve().parent
data_store = directory / "Data Store"

columns_to_check = ['H1', 'H2', 'H3', 'H4', 'H5', 'H6']

today = datetime.today()
formatted_today = today.strftime("%Y-%m-%dT00:00:00Z")
first_day_of_this_month = today.replace(day=1)
last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
formatted_last_day = last_day_of_last_month.strftime("%Y-%m-%dT00:00:00Z")
first_day_of_this_year = today.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0)
formatted_first_day = first_day_of_this_year.strftime("%Y-%m-%dT00:00:00Z")

#---------- Create authentification tokens
def googleAuth():
    try:
        # 1. Try Application Default Credentials (Cloud Run)
        credentials, project_id = default()
        print("✅ Authenticated with ADC")
        return credentials, project_id

    except DefaultCredentialsError:
        print("⚠️ ADC not available, trying GOOGLE_CLOUD_SECRET env var...")

        # 2. Codespaces (secret stored in env var)
        secret_json = os.getenv('GOOGLE_CLOUD_SECRET')
        if secret_json:
            service_account_info = json.loads(secret_json)
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            project_id = service_account_info.get('project_id')
            print("✅ Authenticated with service account from env var")
            return credentials, project_id

        # 3. Local dev (service account file path)
        file_path = os.getenv("GCP")
        if file_path and os.path.exists(file_path):
            credentials = service_account.Credentials.from_service_account_file(file_path)
            with open(file_path) as f:
                project_id = json.load(f).get("project_id")
            print("✅ Authenticated with service account from file")
            return credentials, project_id

        raise Exception("❌ No valid authentication method found")

def get_secrets(secret_id):
    def access_secret_version(project_id, secret_id, version_id="latest"):

        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")

        return payload

    project_id = "api-integrations-412107"
    version_id = "latest" 

    secret = access_secret_version(project_id, secret_id, version_id)
    return secret

def load_ssl(certfile_content, keyfile_content):
    """
    Create temporary files for the certificate and keyfile contents.
    
    Args:
        certfile_content (str): The content of the certificate file.
        keyfile_content (str): The content of the key file.
    
    Returns:
        tuple: Paths to the temporary certificate and key files.
    """
    # Create temporary files for certfile and keyfile
    temp_certfile = tempfile.NamedTemporaryFile(delete=False)
    temp_keyfile = tempfile.NamedTemporaryFile(delete=False)

    try:
        # Write the contents into the temporary files
        temp_certfile.write(certfile_content.encode('utf-8'))
        temp_keyfile.write(keyfile_content.encode('utf-8'))
        temp_certfile.close()
        temp_keyfile.close()

        # Return the paths of the temporary files
        return temp_certfile.name, temp_keyfile.name
    except Exception as e:
        # Clean up in case of error
        os.unlink(temp_certfile.name)
        os.unlink(temp_keyfile.name)
        raise e

def cascade_bearer ():
    cascade_token_url='https://api.iris.co.uk/oauth2/v1/token'
    
    cascade_token_data = {
        'grant_type':'client_credentials',
                    }
    cascade_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        "Authorization": f'Basic:{cascade_API_id}'
            }

    cascade_token_response = requests.post(cascade_token_url, data=cascade_token_data, headers=cascade_headers)

    #checks the api response and extracts the bearer token
    if cascade_token_response.status_code == 200:
        cascade_token = cascade_token_response.json()['access_token']
    #print (cascade_token)
    return cascade_token

def adp_bearer():

    adp_token_url = 'https://accounts.adp.com/auth/oauth/v2/token'                                                                                          

    adp_token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    adp_headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    adp_token_response = requests.post(adp_token_url, cert=(certfile, keyfile), verify=True, data=adp_token_data, headers=adp_headers)

    if adp_token_response.status_code == 200:
        access_token = adp_token_response.json()['access_token']

    #print (access_token)
    return access_token

def adjust_column_widths(file_path,sheet=None):
    wb = openpyxl.load_workbook(file_path)

    if sheet is None:
        ws = wb["Sheet1"]
    else:
        ws = wb[sheet]
                                                                #Sets the widths of columns in a worksheet to fit
    for col in ws.columns:
        max_length = 0
        column = col[0].column_letter  # Get the column name
        for cell in col:
            try:
                text_length = len(str(cell.value))
                if text_length > max_length:
                    max_length = text_length
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = adjusted_width
    
    wb.save(file_path)

#---------- 
def export_data(filename,variable):
    file_path = data_store / filename
    with open(file_path, "w") as outfile:
        json.dump(variable, outfile, indent=4)

def import_data(filename):
    file_path = data_store / filename
    with open(file_path, "r") as file:
        return json.load(file)

#---------- api calls

def api_count(api_response):
    response_data = api_response.json()
    total_number = response_data['@odata.count']
    api_calls = math.ceil(total_number / 200)
    
    return api_calls

def api_call(api_url,api_headers,api_params=None):
    
    api_response = requests.get(api_url, headers = api_headers, params = api_params)
    time.sleep(0.6)   
   
    return api_response

def get_payroll(hierarchyRecord,ID):
    payrolls_L2_str = get_secrets("payrolls_L2")
    payrolls_L2 = ast.literal_eval(payrolls_L2_str)

    if any("Surveyors (" in str(hierarchyRecord.get(f'hierarchyLevel{i}') or '') for i in range(2, 7)):
        hierarchyRecord['payroll'] = 'Not on Payroll'
    elif hierarchyRecord.get('hierarchyLevel4') == "Lemac (0500)":
        hierarchyRecord['payroll'] = 'Lemac'
    elif hierarchyRecord.get('hierarchyLevel3') == "Engineer Admin (935)" and ID in {"8609","8906", "9050", "9215", "9912", "10542", "10612"}:   #Fr Engineer Admins before site move
        hierarchyRecord['payroll'] = 'Acorn UK'
    else:
        hierarchyLevel2 = hierarchyRecord.get('hierarchyLevel2')
        if hierarchyLevel2 in payrolls_L2:
            hierarchyRecord['payroll'] = payrolls_L2[hierarchyLevel2]
        else:
            hierarchyRecord['payroll'] = None  # Set to None or a default value if no match found  

#---------- create a list of hierarchy nodes

def hierarchy_nodes():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("Creating Hierarchy Nodes (" + time_now + ")")

    skip_param = 0
    hierarchyNodes = []

    api_response = api_call(api_hierarchy,api_headers,None)                       #Calls the api to find total records
    api_calls = api_count(api_response)                                     #converts the total records into number of calls needed

    for i in range(api_calls):
        skip_param = i * 200
        api_params = {
            "$top": 200,
            "$skip": skip_param,
            "$select": "Id,ParentId,Level,Title",
            }
        
        api_response = api_call(api_hierarchy,api_headers,api_params)

        if api_response.status_code == 200:
            json_data = api_response.json()
            json_data = json_data['value']
            hierarchyNodes.extend(json_data)

    if debug:
        export_data ("000 - Hierarchy Nodes.json",hierarchyNodes)

    return hierarchyNodes

#---------- Downloads current leavers data
def link_cascadeId_to_DisplayId(cascade_responses):
    id_to_display = {}
    for record in cascade_responses:
        id_to_display[record["Id"]] = record.get("DisplayId", "")
    return id_to_display

def link_cascadeId_to_latestJob(cascade_jobs_filter):
    id_to_job = {}
    for job in cascade_jobs_filter:
        emp_id = job["EmployeeId"]
        if emp_id not in id_to_job or job.get("StartDate", "") > id_to_job[emp_id].get("StartDate", ""):
            id_to_job[emp_id] = job   
    return id_to_job

def link_cascadeId_to_lm_path(cascade_responses,id_to_display,id_to_job):
    id_to_lm_path = {}
    for record in cascade_responses:
        emp_id = record["Id"]
        lm_path = []
        current_id = emp_id
        visited = set()

        while True:
            job = id_to_job.get(current_id)
            if not job:
                break
            lm_id = job.get("LineManagerId")
            if not lm_id or lm_id in visited:
                break
            visited.add(lm_id)
            display_id = id_to_display.get(lm_id)
            if display_id:
                lm_path.append(display_id)
            current_id = lm_id

        path_str = "|" + "|".join(lm_path) + "|" if lm_path else ""
        id_to_lm_path[emp_id] = path_str
    return id_to_lm_path

def convert_date_format(variable):
    if variable:
        return datetime.fromisoformat(variable.replace("Z", "+00:00"))
    else:
        return None

def GET_workers_cascade():
    print ("Collecting Cascade Personal Data")
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving current Personal Data from Cascade HR (" + time_now + ")")

    cascade_responses = []

    api_response = api_call(api_employees,api_headers,None)
    api_calls = api_count(api_response)                

    for i in range(api_calls):
            skip_param = i * 200
            api_params = {
                "$top": 200,
                "$skip": skip_param
            } 
            
            api_response = api_call(api_employees,api_headers,api_params)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['value']
                cascade_responses.extend(json_data)

    cascadeId_to_drop = get_secrets("cascadeId_to_drop")

    filtered_data = [
        record
        for record in cascade_responses
        if record.get('DisplayId') is not None and record["DisplayId"] not in cascadeId_to_drop
    ]

    x_years_ago = datetime.now() - timedelta(days = 2*365)
    filtered_records = []
    for record in filtered_data:
        employment_left_date = record.get('EmploymentLeftDate')
        
        if not employment_left_date:
            filtered_records.append(record)
            continue

        left_date = datetime.fromisoformat(employment_left_date.replace('Z', '+00:00'))
        left_date = left_date.replace(tzinfo=None)  # Remove timezone for comparison
        
        if left_date >= x_years_ago:
            filtered_records.append(record)

    if debug:
        export_data ("001 - cascade personal.json",filtered_records)

    return filtered_records

def GET_jobs_cascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Retrieving Job Data from Cascade HR (" + time_now + ")")

    cascade_job_responses = []

    api_response = api_call(api_jobs,api_headers,None)
    api_calls = api_count(api_response)   
    
    for i in range(api_calls):
            skip_param = i * 200
            api_params = {
                "$top": 200,
                "$skip": skip_param
            } 
            
            api_response = api_call(api_jobs,api_headers,api_params)

            if api_response.status_code == 200:
                json_data = api_response.json()
                json_data = json_data['value']
                cascade_job_responses.extend(json_data)

    if debug:
        export_data ("002a - Cascade jobs.json",cascade_job_responses)
    
    return cascade_job_responses

def filter_latest_jobs(cascade_jobs):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Filtering the most recent jobs (" + time_now + ")")

    employee_latest_jobs = defaultdict(lambda: None)

    for job in cascade_jobs:
        employee_id = job.get('EmployeeId')
        if employee_id:
            current_latest = employee_latest_jobs[employee_id]
            if (current_latest is None or 
                job.get('LastModifiedOn', '') > current_latest.get('LastModifiedOn', '')):
                employee_latest_jobs[employee_id] = job

    # Convert to list
    latest_cascade_jobs = list(employee_latest_jobs.values())
    
    if debug:
        export_data ("002b - Cascade jobs - Latest.json",latest_cascade_jobs)
    
    return latest_cascade_jobs

def rearrange_cascade(cascade_responses, cascade_jobs_filter):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Rearranging into the required form (" + time_now + ")")

    id_to_display = link_cascadeId_to_DisplayId(cascade_responses)
    id_to_job = link_cascadeId_to_latestJob(cascade_jobs_filter)
    id_to_lm_path = link_cascadeId_to_lm_path(cascade_responses,id_to_display,id_to_job)

    output=[]

    for entry in cascade_responses:
        Cascade_full = entry.get("Id")
        Cascade_ID = entry.get("DisplayId")
        EndDate = entry.get("EmploymentLeftDate")
        KnownAs = entry.get("KnownAs", "")
        name = entry.get("FirstName", "")
        if entry.get("Addresses") and len(entry["Addresses"]) > 0:
            postcode = entry["Addresses"][0].get("PostCode", "")
        else:
            postcode = ""
        
        jobTitle = None
        hierarchy = None
        level = None
        Title = None
        lineManagerId = None
                    
        for record in cascade_jobs_filter:
            if record["EmployeeId"] == Cascade_full:
                jobTitle = record.get('JobTitle', "")
                hierarchy = record.get('HierarchyNodeId', "")
                lineManagerId = record.get("LineManagerId","")
                break  # Exit loop once the matching EmployeeId is found

        # Loop through hierarchyNodes to set level and Title
        if hierarchy is not None:  # Proceed only if hierarchy was set
            for record in hierarchyNodes:
                if record["Id"] == hierarchy:
                    level = record.get('Level', "")
                    Title = record.get('Title', "")
                    break  # Exit loop once the matching Id is found

        hierarchyRecordStr = get_secrets("hierarchyRecord")
        hierarchyRecord = ast.literal_eval(hierarchyRecordStr)

        def find_node_by_id(node_id):
            return next((node for node in hierarchyNodes if node['Id'] == node_id), None)
    
        while True:
            current_node = find_node_by_id(hierarchy)
            if not current_node:
                break

            level = current_node['Level']
            Title = current_node['Title']
            parent_id = current_node['ParentId']

            variable_name = f"hierarchyLevel{level}"
            hierarchyRecord[variable_name] = Title

            if level ==2:
                break

            hierarchy = parent_id
            if not hierarchy:
                break
           
        date_of_birth_str = entry.get("DateOfBirth")
        continuous_service_date_str = entry.get("ContinuousServiceDate")
        employment_left_date_str = entry.get("EmploymentLeftDate")

        get_payroll(hierarchyRecord,Cascade_ID)


        date_of_birth = convert_date_format(date_of_birth_str)
        continuous_service_date = convert_date_format(continuous_service_date_str)
        employment_left_date = convert_date_format(employment_left_date_str)

        # Example calculations if dates are valid
        if date_of_birth and employment_left_date:
            # 1) Age in Years and Months at leaving date
            age_at_leaving = relativedelta(employment_left_date, date_of_birth)
            age_years_months = f"{age_at_leaving.years} years, {age_at_leaving.months} months"
        else:
            age_years_months = "N/A"

        if continuous_service_date and employment_left_date:
            # 2) Length of Service in Years and Months
            service_duration = relativedelta(employment_left_date, continuous_service_date)
            service_years_months = f"{service_duration.years} years, {service_duration.months} months"
            # 3) Length of Service in Months
            length_of_service_in_months = (service_duration.years * 12) + service_duration.months
        else:
            service_years_months = None
            length_of_service_in_months = None

        # Get the LM_Path from id_to_lm_path dictionary
        lm_path = id_to_lm_path.get(Cascade_full, "")

        cascade_reorder ={
            "Employee Id": Cascade_ID,
            "Forename": KnownAs if KnownAs is not None else name,
            "Surname": entry.get("LastName", ""),
            "JobTitle": jobTitle,   
            "HierarchyLevel1": "Acorn Stairlifts",
            "HierarchyLevel2": hierarchyRecord["hierarchyLevel2"],
            "HierarchyLevel3": hierarchyRecord["hierarchyLevel3"],
            "HierarchyLevel4": hierarchyRecord["hierarchyLevel4"],
            "HierarchyLevel5": hierarchyRecord["hierarchyLevel5"],
            "HierarchyLevel6": hierarchyRecord["hierarchyLevel6"],
            "Payroll": hierarchyRecord['payroll'],
            "ContServiceDate": entry.get("ContinuousServiceDate", ""),
            "NationalInsuranceNo": entry.get("NationalInsuranceNumber", ""),
            "ContractEndDate": EndDate,
            "PostCode": postcode,
            "MonthYear": employment_left_date.replace(day=1).strftime("01/%m/%Y") if employment_left_date is not None else None,
            "LeaverReason": entry.get("LeaverReason",""),
            "WorksFor": lineManagerId,
            "Age": age_years_months,
            "LengthofService": service_years_months,
            "LOS": length_of_service_in_months,
            "LM_Path": lm_path
            }

        output.append(cascade_reorder)

    # Sort the output
    output = sorted(
        output,
        key=lambda x: (
            x["HierarchyLevel1"] or "",
            x["HierarchyLevel2"] or "",
            x["HierarchyLevel3"] or "",
            x["HierarchyLevel4"] or "",
            x["HierarchyLevel5"] or "",
            x["HierarchyLevel6"] or ""
        )
    )

    # Filter the output
    output = [
        record for record in output
        if record["ContServiceDate"] is not None and record["Payroll"] is not None and datetime.strptime(record["ContServiceDate"], "%Y-%m-%dT%H:%M:%SZ") <= today
        ]

    if debug:
        export_data("004 - Cascade reordered.json",output)
    
    return output

def output_cascade():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("    Outputting Data (" + time_now + ")")
    df = pd.DataFrame(cascade_data)

    if Data_export:
        file_path_excel = data_store / "000 - Cascade staff (API).xlsx"
        file_path_csv = data_store / "000 - Cascade staff (API).csv"

        df.to_excel(file_path_excel, index=False)
        df.to_csv(file_path_csv, index=False)

        adjust_column_widths(file_path_excel)

def classify_jobs(df,hierarcy):
        df['Category'] = df[hierarcy].apply(
        lambda x: 'Ops' if pd.notna(x) and 'Operations' in x 
        else ('Ops' if pd.notna(x) and 'Engineer' in x
        else ('Ops' if pd.notna(x) and 'Install' in x
        else ('Ops' if pd.notna(x) and 'Service' in x
        else ('Ops' if pd.notna(x) and 'Logistics' in x
        else ('Ops' if pd.notna(x) and 'Warehouse' in x
            else ('Sales' if pd.notna(x) and 'Sales' in x
            else ('Sales' if pd.notna(x) and ' to ' in x 
                else ('Production' if pd.notna(x) and 'Production' in x 
                    else 'Other')))))))))

def voluntary(df,reason):
    df['Voluntary'] = df[reason].apply(
        lambda x: 'V' if pd.notna(x) and 'Resigned' in x
        else ('V' if pd.notna(x) and 'AWOL' in x
              else None))

def extract_last_house(row):
    last_number = None
    for col in ['HierarchyLevel6', 'HierarchyLevel5', 'HierarchyLevel4', 
                'HierarchyLevel3', 'HierarchyLevel2', 'HierarchyLevel1']:
        match = re.search(r'\((\d+)\)', str(row[col]))
        if match:
            last_number = match.group(1)
            break
    return last_number

def delete_table_data(project_id, dataset_id, table_id,client):
    query = f"DELETE FROM `{project_id}.{dataset_id}.{table_id}` WHERE TRUE"
    client.query(query).result()  # Executes the query
    print(f"All rows deleted from {table_id}")

def load_data(data, project_id, dataset_id, table_id,client):
    df = pd.DataFrame(data)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    job = client.load_table_from_dataframe(df, table_ref)  # Load data
    job.result()  # Wait for the job to complete
    print(f"Data loaded into {table_id}")

def upload_to_bigquery(data, table_id):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("        Rebuilding Data Table in BigQuery (" + time_now + ")")

    # Initialize BigQuery client using default credentials
    client = bigquery.Client(project=project, credentials=credentials)

    project_id = "api-integrations-412107"
    dataset_id = "leavers_dashboard"

    delete_table_data(project_id, dataset_id, table_id,client)
    load_data(data, project_id, dataset_id, table_id,client)

def looker_data_set(cascade):    
    print ("Arranging Data for export to Looker")
    print (".........")
    df = pd.DataFrame(cascade)

    classify_jobs(df,'HierarchyLevel3')
    voluntary(df,'LeaverReason')

    df['LastHouse'] = df.apply(extract_last_house, axis=1)
    df.columns = df.columns.str.replace(' ', '')                                                                  #Removes all spaces for uploading to bigQuery
    
    df['Payroll'] = df['Payroll'].str.replace(' ', '')
   
    df['HierarchyLevel4'] = df['HierarchyLevel4'].str.replace(' ', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace(' ', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace('()', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace(')', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace('/', '')

    payroll_conversion_str = get_secrets("payroll_conversion")
    payroll_conversion = ast.literal_eval(payroll_conversion_str)

    df['Payroll'] = df['Payroll'].map(payroll_conversion)  

    df['EmployeeId'] = pd.to_numeric(df['EmployeeId'], errors='coerce').astype('Int64')
    df['LOS'] = pd.to_numeric(df['LOS'], errors='coerce').astype('Int64')
    df['LastHouse'] = pd.to_numeric(df['LastHouse'], errors='coerce').astype('Int64')
    df['ContServiceDate'] = pd.to_datetime(df['ContServiceDate'], errors='coerce')
    df['ContractEndDate'] = pd.to_datetime(df['ContractEndDate'], errors='coerce')
    df['MonthYear'] = pd.to_datetime(df['MonthYear'], errors='coerce')
       
    if Data_export:
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # If the column contains timezone-aware datetime objects, convert them to naive
                if df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)
        
        file_path_csv = data_store / "Extra Cols.csv"

        df.to_csv(file_path_csv, index=False)
    
    if gcloud:
        upload_to_bigquery(df,"staff_data")

    return df

if __name__ == "__main__":
    #---------- Create authentification tokens
    try:
        credentials, project = googleAuth()
        print(f"Authenticated successfully! Project ID: {project}\n")
    except Exception as e:
        print(f"Authentication error: {e}")

    client_id = get_secrets("ADP-usa-client-id")
    client_secret = get_secrets("ADP-usa-client-secret")
    strings_to_exclude = get_secrets("strings_to_exclude")
    keyfile_USA = get_secrets("usa_cert_key")
    certfile_USA = get_secrets("usa_cert_pem")
    cascade_API_id = get_secrets("cascade_API_id")

    certfile,keyfile = load_ssl(certfile_USA,keyfile_USA)

    cascade_token                           = cascade_bearer()
    adp_token                               = adp_bearer()

    api_hierarchy   = 'https://api.iris.co.uk/hr/v2/hierarchy?%24count=true'
    api_employees   = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
    api_jobs        = 'https://api.iris.co.uk/hr/v2/jobs?%24count=true'

    api_headers = {
        'Authorization': f'Bearer {cascade_token}',
        }
    #---------- 

    if testing is False:
        hierarchyNodes          = hierarchy_nodes()
        cascade_responses       = GET_workers_cascade()
        cascade_jobs            = GET_jobs_cascade()
        cascade_jobs_filter     = filter_latest_jobs(cascade_jobs)
        cascade_data            = rearrange_cascade(cascade_responses,cascade_jobs_filter)

    if testing is True:
        print ("Loading from saved file")
        hierarchyNodes          = import_data("000 - Hierarchy Nodes.json")
        cascade_responses       = import_data("001 - cascade personal.json")
        cascade_jobs            = import_data("002a - Cascade jobs.json")
        cascade_jobs_filter     = import_data("002b - Cascade jobs - Latest.json")
        cascade_data            = import_data("004 - Cascade reordered.json")
    
    output_cascade()   
    looker_data = looker_data_set(cascade_data)