from datetime import datetime,timedelta
from pathlib import Path

directory = Path(__file__).resolve().parent

dataDropExcel =  directory / "excel"
dataDropJson =  directory / "json"
dataDropCsv =  directory / "csv"


today = datetime.today()
formatted_today = today.strftime("%Y-%m-%dT00:00:00Z")
first_day_of_this_month = today.replace(day=1)
last_day_of_last_month = first_day_of_this_month - timedelta(days=1)
formatted_last_day = last_day_of_last_month.strftime("%Y-%m-%dT00:00:00Z")
first_day_of_this_year = today.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0)
formatted_first_day = first_day_of_this_year.strftime("%Y-%m-%dT00:00:00Z")

newRecord = {'Employee Id':8842,
                'Known As':'Andy',
                'Surname':'Parkinson',
                'Job Title':'HR Data and System specialist',
                'Hierarchy Level 1':'Acorn Stairlifts',
                'Hierarchy Level 2':'Acorn Group',
                'Hierarchy Level 3':'Human Resources (0150)',
                'Hierarchy Level 4':'Advice and Payroll (0155)',
                'Payroll Name':'Acorn UK',
                'Cont. Service Date': '2023-09-04T00:00:00Z',
                'National Insurance No.': 'JG976138C'
}

payrolls_L2 = {
    "Acorn (Australia)":"Acorn Australia (Bureau)",
    "Acorn (Canada)":"Acorn Canada (ADP)",
    "Acorn (France) (930)":"Acorn France (Bureau)",
    "Acorn (Germany) (910)":"Acorn Germany (Bureau)",
    "Acorn (Isle of Man)(950)":"Acorn Isle of Man",
    "Acorn (Italy) (920)":"Acorn Italy (Bureau)",
    "Acorn (New Zealand)":"Acorn New Zealand",
    "Acorn (South Africa) (940)":"Acorn South Africa (Mazars)",
    "Acorn (UK) (0550)":"Acorn UK",
    "Acorn (USA)":"Acorn Inc (ADP)",
    "Acorn Group":"Acorn UK",
    "Acorn Group Production (0300)":"Acorn UK",
    "Acorn (Singapore)":"Acorn (Singapore)",
}

payroll_conversion = {
    "AcornUK":"UK",
    "Lemac":"Lemac",
    "AcornInc(ADP)":"USA",
    "AcornGermany(Bureau)":"Germany",
    "AcornItaly(Bureau)":"Italy",
    "AcornFrance(Bureau)":"France",
    "AcornAustralia(Bureau)":"Australia",
    "NotonPayroll":"NOP",
    "AcornSouthAfrica(Mazars)":"SA",
    "AcornNewZealand":"NZ",
    "AcornCanada(ADP)":"Canada",
    "AcornIsleofMan":"IOM",
    "Acorn(Singapore)":"Singapore",
}

names_to_drop = ["Gledhill, Kate", "Parkinson, Andrew Robert", "Report Totals:"]

cascadeId_to_drop = ["9","11","2286","6352","7565","8058","6712","8065","9464","6203"]

columns_to_check = ['H1', 'H2', 'H3', 'H4', 'H5', 'H6']
