import pandas as pd
import json
import sys
import openpyxl
import shutil
from google.cloud import storage, bigquery
from pathlib import Path

import shutil
from datetime import datetime
import constants
import re

def download_from_gcs(page):                #page will be either the word jobs or personal
    client = storage.Client(credentials=constants.credentials)
    
    bucket_name = 'acorn_looker_uploads'
    blob_name = f"Json - Ex Staff/{page} archive.json"

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    json_string = blob.download_as_text()

    archive = json.loads(json_string)

    return archive
    #Ensure you define the variable in the main code.
   
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

def get_payroll(hierarchyRecord):
    if hierarchyRecord.get('hierarchyLevel6') == "Self Employed Surveyors (0835)":
        hierarchyRecord['payroll'] = 'Not on Payroll'
    elif hierarchyRecord.get('hierarchyLevel4') == "Lemac (0500)":
        hierarchyRecord['payroll'] = 'Lemac'
    elif hierarchyRecord.get('hierarchyLevel3') == "Engineer Admin (935)":
        hierarchyRecord['payroll'] = 'Acorn UK'
    else:
        hierarchyLevel2 = hierarchyRecord.get('hierarchyLevel2')
        if hierarchyLevel2 in constants.payrolls_L2:
            hierarchyRecord['payroll'] = constants.payrolls_L2[hierarchyLevel2]
        else:
            hierarchyRecord['payroll'] = None  # Set to None or a default value if no match found 

def date_difference(start,end):
    if start is None or end is None:
        return None,None
    else:
        start = datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")
        end = datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ")

        years = end.year - start.year
        if end.month < start.month or (end.month == start.month and end.day < start.day):
            years -= 1

        months = end.month - start.month
        if months < 0:
            months += 12
        
        if end.day < start.day:
            months -= 1

        if months < 0:
            months += 12
        
        total_months = (years * 12) + months

        return f"{years} Years, {months} Months", total_months
        
def clear_files(folder_path):
    folder = Path(folder_path)

    if folder.exists() and folder.is_dir():
        for item in folder.iterdir():
            try:
                if item.is_file() or item.is_symlink():
                    item.unlink()  # Delete file or symlink
                elif item.is_dir():
                    shutil.rmtree(item)  # Delete directory and its contents
            except Exception as e:
                print(f'Failed to delete {item}. Reason: {e}')
    else:
        print(f"The folder {folder_path} does not exist.")

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
  
def categorise_LOS(value):
    if value is None:
        return 'NS'
    elif value <=3:
        return '0 < M < 3'
    elif 4 <= value <=6:
        return '3 < M < 6'
    elif 7 <= value <=9:
        return '6 < M < 9'
    elif 10 <= value <=12:
        return '9 < M < 12'
    elif 13 <= value <=24:
        return '1 < Y < 2'
    elif 25 <= value <=36:
        return '2 < Y < 3'
    elif 37 <= value <=48:
        return '3 < Y < 4'
    elif 49 <= value <=60:
        return '4 < Y < 5'
    elif value >61:
        return '5+ years'
    else:
        return None

def categorise_LOS_pos(value):
    if value is None:
        return 1
    elif value <=3:
        return 2
    elif 4 <= value <=6:
        return 3
    elif 7 <= value <=9:
        return 4
    elif 10 <= value <=12:
        return 5
    elif 13 <= value <=24:
        return 6
    elif 25 <= value <=36:
        return 7
    elif 37 <= value <=48:
        return 8
    elif 49 <= value <=60:
        return 9
    elif value >61:
        return 10
    else:
        return None

def extract_last_house(row):
    last_number = None
    for col in ['HierarchyLevel6', 'HierarchyLevel5', 'HierarchyLevel4', 
                'HierarchyLevel3', 'HierarchyLevel2', 'HierarchyLevel1']:
        match = re.search(r'\((\d+)\)', str(row[col]))
        if match:
            last_number = match.group(1)
            break
    return last_number

def upload_to_bigquery(data, table_id):
    time_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("        Rebuilding Data Table in BigQuery (" + time_now + ")")

    # Initialize BigQuery client using default credentials
    client = bigquery.Client()

    project_id = "api-integrations-412107"
    dataset_id = "leavers_dashboard"

    def delete_table_data(project_id, dataset_id, table_id):
        query = f"DELETE FROM `{project_id}.{dataset_id}.{table_id}` WHERE TRUE"
        client.query(query).result()  # Executes the query
        print(f"All rows deleted from {table_id}")

    def load_data(data, project_id, dataset_id, table_id):
        df = pd.DataFrame(data)

        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        job = client.load_table_from_dataframe(df, table_ref)  # Load data
        job.result()  # Wait for the job to complete
        print(f"Data loaded into {table_id}")

    delete_table_data(project_id, dataset_id, table_id)
    load_data(data, project_id, dataset_id, table_id)