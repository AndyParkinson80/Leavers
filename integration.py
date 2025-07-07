#Set-ExecutionPolicy Bypass -Scope Process
#cd "C:\Users\andre\OneDrive - acornstairlifts.com\001 - Data + Systems\000 - Deployed Programs\002 - Absences and Leavers (Looker)\001 - Leavers"
#cd ~/Documents/GitHub/Leavers
#gcloud builds submit --tag europe-west2-docker.pkg.dev/api-integrations-412107/looker-files/daily_leavers_download:latest
#gcloud run jobs update daily-leavers-download --image europe-west2-docker.pkg.dev/api-integrations-412107/looker-files/daily_leavers_download:latest --region europe-west2
#gcloud run jobs execute daily-leavers-download --region europe-west2

from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
import math
import time
import os
import json
import pandas as pd
import sys

import constants                                                                   
import functions
import security


debug = False                                                           
Data_export = False                                                                 #True --> export data to data store
testing = False
gcloud = True                                                                      #True --> Pulls all data from ADP WFN, not just current

data_store = constants.dataDropJson

if debug is False:
    functions.clear_files(constants.dataDropJson)
    functions.clear_files(constants.dataDropExcel)
    functions.clear_files(constants.dataDropCsv)

def hierarchy_nodes():
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print ("Creating Hierarchy Nodes (" + time_now + ")")

    hierarchyNodes = []
    api_url = 'https://api.iris.co.uk/hr/v2/hierarchy?%24count=true'
    api_headers = {
        'Authorization': f'Bearer {security.cascade_token}',
        }
    
    def api_count():
        api_response = requests.get(api_url, headers=api_headers)
        response_data = api_response.json()
        total_number = response_data['@odata.count']
        api_calls = math.ceil(total_number / 200)
        rounded_total_number = api_calls * 200
        
        return api_calls,rounded_total_number

    def api_call(skip_param):
        api_params = {
            "$top": 200,
            "$skip": skip_param,
            "$select": "Id,ParentId,Level,Title",
            }
        
        api_response = requests.get(api_url, headers = api_headers, params = api_params)
        time.sleep(0.6)

        if api_response.status_code == 200:
            json_data = api_response.json()
            json_data = json_data['value']
            hierarchyNodes.extend(json_data)
            
            if api_response.status_code == 204:
                return True
        elif api_response.status_code == 204:
            return True
        else:
            print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

    api_calls, max_records = api_count()
    total_records = 0
    skip_param = 0

    for i in range(api_calls):
        skip_param = i * 200
        api_call(skip_param)

    if debug:
        file_path = os.path.join(data_store,"000 - Hierarchy Nodes.json")
        with open(file_path, "w") as outfile:
            json.dump(hierarchyNodes, outfile, indent=4)

    return hierarchyNodes

def cascade_report():
    print ("Collecting Cascade Personal Data")
    print (".........")
    def GET_workers_cascade():
        print ("Collecting Cascade Personal Data")
        print (".........")
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Retrieving current Personal Data from Cascade HR (" + time_now + ")")

        cascade_responses = []

        def api_count():
            api_url = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
            api_headers = {
                'Authorization': f'Bearer {security.cascade_token}',
            }

            api_params = {
            #"$filter": f"EmploymentLeftDate eq null or EmploymentLeftDate ge {constants.formatted_last_day}",
            #"$filter": f"EmploymentLeftDate eq null or EmploymentLeftDate ge {constants.formatted_today}",
                } 
        
            api_count_response = requests.get(api_url, headers=api_headers, params=api_params)
            response_data = api_count_response.json()

            total_number = response_data['@odata.count']
            rounded_total_number = math.ceil(total_number / 200) * 200
            print (rounded_total_number)
            return rounded_total_number
        
        def make_api_request(skip_param):
            api_url = 'https://api.iris.co.uk/hr/v2/employees?%24count=true'
            api_headers = {
                'Authorization': f'Bearer {security.cascade_token}',
            }
            
            api_params = {
                #"$filter": f"EmploymentLeftDate eq null or EmploymentLeftDate ge {constants.formatted_last_day}",
                #"$filter": f"EmploymentLeftDate eq null or EmploymentLeftDate ge {constants.formatted_today}",
                "$top": 250,
                "$skip": skip_param
            }                

            api_response = requests.get(api_url, headers=api_headers, params=api_params)
            time.sleep(0.6)
            if api_response.status_code == 200:
                #checks the response and writes the response to a variable
                json_data = api_response.json()

                # Append the response to all_responses
                cascade_responses.append(json_data)

                # Check for a 204 status code and break the loop
                if api_response.status_code == 204:
                    return True
            elif api_response.status_code == 204:
                return True
            else:
                print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

        max_records = api_count()

        total_records = 0
        skip_param = 0

        while True:
            make_api_request(skip_param)
            #maximum returned records for WFN is 100. This small loop alters the $skip variable and requests the 'next' 100
            # Increment skip_param by 100 for the next request
            skip_param += 250
            total_records += 250  # Keep track of the total number of records retrieved
            
            # Break the loop when there are no more records to retrieve
            if total_records >= max_records:  
                break

            time.sleep(0.6)

        # Combine all the "workers" arrays into a single array
        combined_value = []
        for item in cascade_responses:
            combined_value.extend(item["value"])

        # Create a new dictionary with the combined workers
        combined_data = [{
            "value": combined_value,
            "meta": None,
            "confirmMessage": None
        }]

        filtered_data = []

        for record_set in combined_data:
            for record in record_set.get('value', []):
                #if record.get('DisplayId') is not None:
                filtered_data.append(record)

        filtered_data = [
            record for record in filtered_data
            if record["DisplayId"] not in constants.cascadeId_to_drop
        ]

        if debug:
            file_path = os.path.join(data_store,"001 - cascade personal.json")
            with open(file_path, "w") as outfile:
                json.dump(filtered_data, outfile, indent=4)

    
        return filtered_data

    def GET_jobs_cascade():
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Retrieving Job Data from Cascade HR (" + time_now + ")")

        api_url = 'https://api.iris.co.uk/hr/v2/jobs?%24count=true'
        cascade_job_responses = []

        def api_count():
            api_headers = {
                'Authorization': f'Bearer {security.cascade_token}',
            }

            api_params = {
                #"$filter": f"(EndDate eq null or EndDate ge 2014-01-01T00:00:00.000Z)",
                } 
        
            api_count_response = requests.get(api_url, headers=api_headers, params=api_params)
            response_data = api_count_response.json()

            total_number = response_data['@odata.count']
            rounded_total_number = math.ceil(total_number / 200) * 200
            print (rounded_total_number)
            return rounded_total_number

        def make_api_request(skip_param):
            api_headers = {
                'Authorization': f'Bearer {security.cascade_token}',
            }
            api_params = {
                #"$filter": f"(EndDate eq null or EndDate ge 2014-01-01T00:00:00.000Z)",
                "$top": 250,
                "$skip": skip_param,
                "$select": "EmployeeId,JobTitle,EndDate,StartDate,HierarchyNodeId,LineManagerId",
            }
        
            api_response = requests.get(api_url, headers=api_headers, params=api_params)
            time.sleep(0.75)

            if api_response.status_code == 200:
                json_data = api_response.json()
                cascade_job_responses.append(json_data)

                if not json_data:
                    return True
            
            else:
                print(f"Failed to retrieve data from API for skip_param {skip_param}. Status code: {api_response.status_code}")

        rounded_total_number = api_count()

        total_records = 0
        skip_param = 0

        while total_records <= rounded_total_number:
            if make_api_request(skip_param):
                break
            skip_param += 100
            total_records += 100
        
        combined_value = []
        for item in cascade_job_responses:
            combined_value.extend(item["value"])      

        if debug:
            file_path = os.path.join(data_store,"002 - Cascade jobs.json")
            with open(file_path, "w") as outfile:
                json.dump(combined_value, outfile, indent=4)

        return combined_value

    def filter_latest_jobs(cascade_jobs):
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Filtering the most recent jobs (" + time_now + ")")

        latest_jobs = {}

        for job in cascade_jobs:
            emp_id = job['EmployeeId']
            start_date = datetime.fromisoformat(job['StartDate'].replace('Z', '+00:00'))
            
            if emp_id not in latest_jobs:
                latest_jobs[emp_id] = job
            else:
                existing_start_date = datetime.fromisoformat(latest_jobs[emp_id]['StartDate'].replace('Z', '+00:00'))
                if start_date > existing_start_date:
                    latest_jobs[emp_id] = job

        if debug:
            file_path = os.path.join(data_store,"003 - Cascade jobs II.json")
            with open(file_path, "w") as outfile:
                json.dump(list(latest_jobs.values()), outfile, indent=4)
        
        return list(latest_jobs.values())

    def rearrange_cascade(cascade_responses, cascade_jobs_filter):
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Rearranging into the required form (" + time_now + ")")

        # Create id_to_display mapping
        id_to_display = {}
        for record in cascade_responses:
            id_to_display[record["Id"]] = record.get("DisplayId", "")
        
        # Create id_to_job mapping
        id_to_job = {}
        for job in cascade_jobs_filter:
            emp_id = job["EmployeeId"]
            # If there are multiple jobs for an employee, we'll keep the most recent one
            if emp_id not in id_to_job or job.get("StartDate", "") > id_to_job[emp_id].get("StartDate", ""):
                id_to_job[emp_id] = job
                
        # Create line manager paths
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
            
        output=[]

        for entry in cascade_responses:
            Cascade_full = entry.get("Id")
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

                    
            hierarchyRecord={"hierarchyLevel1": None,
                        "hierarchyLevel2": None,
                        "hierarchyLevel3": None,
                        "hierarchyLevel4": None,
                        "hierarchyLevel5": None,
                        "hierarchyLevel6": None,         
                        }        
            
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
        
            functions.get_payroll(hierarchyRecord)
            
            # Parse dates with a check
            date_of_birth_str = entry.get("DateOfBirth")
            continuous_service_date_str = entry.get("ContinuousServiceDate")
            employment_left_date_str = entry.get("EmploymentLeftDate")

            # Ensure that dates are available before parsing
            if date_of_birth_str:
                date_of_birth = datetime.fromisoformat(date_of_birth_str.replace("Z", "+00:00"))
            else:
                date_of_birth = None  # or handle accordingly

            if continuous_service_date_str:
                continuous_service_date = datetime.fromisoformat(continuous_service_date_str.replace("Z", "+00:00"))
            else:
                continuous_service_date = None  # or handle accordingly

            if employment_left_date_str:
                employment_left_date = datetime.fromisoformat(employment_left_date_str.replace("Z", "+00:00"))
                # Format Employment Left Date to "01/mm/yyyy"
                formatted_employment_left_date = employment_left_date.replace(day=1).strftime("01/%m/%Y")
            else:
                employment_left_date = None
                formatted_employment_left_date = None  # or handle accordingly

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
                "Employee Id": entry.get("DisplayId", ""),
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
                "MonthYear": formatted_employment_left_date,
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
            if record["ContServiceDate"] is not None and record["Payroll"] is not None and datetime.strptime(record["ContServiceDate"], "%Y-%m-%dT%H:%M:%SZ") <= constants.today
            ]

        if debug:
            file_path = os.path.join(data_store,"004 - Cascade reordered.json")
            with open(file_path, "w") as json_file:
                json.dump(output, json_file, indent=4)
        
        return output

    def output_cascade():
        time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print ("    Outputting Data (" + time_now + ")")
        df = pd.DataFrame(cascade_data)

        if Data_export:
            file_path_excel = os.path.join(constants.dataDropExcel,"000 - Cascade staff (API).xlsx")
            file_path_csv = os.path.join(constants.dataDropCsv,"000 - Cascade staff (API).csv")


            df.to_excel(file_path_excel, index=False)
            df.to_csv(file_path_csv, index=False)


            functions.adjust_column_widths(file_path_excel)
    
    if testing is False:
        cascade_responses                       = GET_workers_cascade()
        cascade_jobs                            = GET_jobs_cascade()
        cascade_jobs_filter                     = filter_latest_jobs(cascade_jobs)
        cascade_data                            = rearrange_cascade(cascade_responses,cascade_jobs_filter)
        output_cascade()

    if testing is True:
        print ("Loading cascade staff from saved file")
        file_path = os.path.join(data_store,"004 - Cascade reordered.json")
        with open(file_path, "r") as file:
            cascade_data = json.load(file)

    return cascade_data
    
def looker_data_set(cascade):    
    print ("Arranging Data for export to Looker")
    print (".........")
    df = pd.DataFrame(cascade)

    functions.classify_jobs(df,'HierarchyLevel3')
    functions.voluntary(df,'LeaverReason')

    df['LastHouse'] = df.apply(functions.extract_last_house, axis=1)
    df.columns = df.columns.str.replace(' ', '')                                                                  #Removes all spaces for uploading to bigQuery
    
    df['Payroll'] = df['Payroll'].str.replace(' ', '')
   
    #insert the payroll mapping here when API back up

    df['HierarchyLevel4'] = df['HierarchyLevel4'].str.replace(' ', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace(' ', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace('()', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace(')', '')
    df['HierarchyLevel5'] = df['HierarchyLevel5'].str.replace('/', '')

    df['Payroll'] = df['Payroll'].map(constants.payroll_conversion)  

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
        
        file_path_csv = os.path.join(constants.dataDropCsv,"Extra Cols.csv")

        df.to_csv(file_path_csv, index=False)
    
    if gcloud:
        functions.upload_to_bigquery(df,"staff_data")

    return df

hierarchyNodes                                                                  = hierarchy_nodes()
cascade_data                                                                    = cascade_report()
looker_data                                                                     = looker_data_set(cascade_data)     


