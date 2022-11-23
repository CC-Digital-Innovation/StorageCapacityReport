import configparser
import csv
from itertools import zip_longest
from fastapi import FastAPI
from loguru import logger
import pandas as pd
import requests

######## UPDATE REQUIREMENTS.TXT, message Ben to update anything on his end before pushing this code.
app = FastAPI(
    title = "Capacity Storage",
    description= "Downloads data from NocoDB as CSV"
)

# params takes in date range and a dictionary that holds all fields user needs to concatenate to URL. Comment & update Tettra.
# params make not required only as optional

config = configparser.ConfigParser()
config.read('config.ini')
url = config.get('noco', 'url')
API_TOKEN = config.get('noco', 'xc-auth')
CONTENT_TYPE = 'application/json'

@logger.catch
@app.get("/NocoDB/filter/")
async def get_Storage_Capacity_Reportings(array_name: str,
                                          start_date: str, 
                                          end_date: str,
                                          Array: bool,
                                          Type: bool,
                                          Division: bool,
                                          Geo: bool,
                                          Serial_Number: bool,
                                          Used: bool,
                                          Failed: bool,
                                          Free: bool,
                                          Total_Capacity: bool,
                                          Percent_Used: bool,
                                          Percentage_Used: bool,
                                          x: bool):
    
    # passed three fields and ignore the remainder. Append three fields to 'tableFields' string 
    fields = {'Array': 'Array', 'created_at': 'created_at', 'Type': 'Type', 'Division': 'Division',
              'Geo': 'Geo', 'SerialNumber': 'SerialNumber', 'Used': 'Used', 'Failed': 'Failed', 'Free': 'Free',
              'TotalCapacity': 'TotalCapacity', 'PercentUsed': 'PercentUsed', 'PercentUsedString': 'PercentUsedString', 'x': 'x'}
            

    # this dictionary holds respective string and boolean values. The key names are used as csv column names
    dict = {
        "created_at": start_date,
        #"Date": "",
        "Array": Array,
        #"array_name": array_name,
        "Type": Type,
        "Division": Division,
        "Geo": Geo,
        "Serial Number": Serial_Number,
        "Used": Used,
        "Failed": Failed,
        "Free": Free,
        "Total Capacity": Total_Capacity,
        "Percent Used": Percent_Used,
        "Percent Used (%)": Percentage_Used,
        "x": x
        }


    csv_Headings = []
    for k in dict.keys():
        csv_Headings.append(k)

    print(csv_Headings)

    '''
    # concatenates fields to url
    tableFields = "&fields="
    for values in fields:
        tableFields += fields[values] + ","
    tableFields = tableFields[:-1]
    #print(tableFields)
    '''


    # for loop will concatenate tableFields if uncommented instead of having hardcode for testing purposes.
    tableFields = "&fields=created_at,Array,Type,Division,Geo,SerialNumber,Used,Failed,Free,TotalCapacity,PercentUsed,PercentUsedString,x"
    '''
    for e in csv_Headings:
        tableFields += e + ","
    tableFields = tableFields[:-1]
    '''

    
    table_url = url + 'testTable/?'
    table = f"&where=(Array,eq,{array_name})"
    table_url = table_url + table + tableFields
    print(table_url)

    header = {
        'xc-auth': API_TOKEN,
        "accept": CONTENT_TYPE
        }

    response = requests.get(table_url, headers=header)
    response = response.json()

    df = ''
    if len(csv_Headings) == 1:
        df = pd.DataFrame([response])
    else:
        df = pd.DataFrame(response)

    print(response)
    #start_date = "2022-11-17"
    #end_date = "2022-11-17"

    start_date = start_date + " 00:00:00" # 00:00:00
    end_date = end_date + " 23:59:59"     # 23:59:59
    print(start_date)
    print(end_date)
    # Filters DataFrame rows between two dates
    data = (df['created_at'] >= start_date) & (df['created_at'] <= end_date)
    df = df.loc[data]

    Date_ = []
    Array_ = []
    Type_ = []
    Division_ = []
    Geo_ = []
    Serial_Number_ = []
    Used_ = []
    Failed_ = []
    Free_ = []
    Total_Capacity_ = []
    Percent_Used_ = []
    Percentage_Used_ = []
    x_ = []

    #csv_Rows = [Date_, Array_, Type_, Division_, Geo_, Serial_Number_, Used_, Failed_, Free_, Total_Capacity_, Percent_Used_, Percentage_Used_, x_]

    for columns in fields.values():
        for data_ofColumns in df[f'{columns}']:
            if columns == "created_at":
                Date_.append(data_ofColumns)
            elif columns == "Array":
                Array_.append(data_ofColumns)
            elif columns == "Type" and dict['Type'] is True:
                Type_.append(data_ofColumns)
            elif columns == "Division" and dict['Division'] is True:
                Division_.append(data_ofColumns)
            elif columns == "Geo" and dict['Geo'] is True:
                Geo_.append(data_ofColumns)
            elif columns == "SerialNumber" and dict['Serial Number'] is True:
                Serial_Number_.append(data_ofColumns)
            elif columns == "Used" and dict['Used'] is True:
                Used_.append(data_ofColumns)
            elif columns == "Failed" and dict['Failed'] is True:
                Failed_.append(data_ofColumns)
            elif columns == "Free" and dict['Free'] is True:
                Free_.append(data_ofColumns)
            elif columns == "TotalCapacity" and dict['Total Capacity'] is True:
                Total_Capacity_.append(data_ofColumns)
            elif columns == "PercentUsed" and dict['Percent Used'] is True:
                Percent_Used_.append(data_ofColumns)
            elif columns == "PercentUsedString" and dict['Percent Used (%)'] is True:
                Percentage_Used_.append(data_ofColumns)
            elif columns == "x" and dict['x'] is True:
                x_.append(data_ofColumns)
    #csv_Headings.remove('created_at') # data is set to 'Date' and removes 'created_at' column

    #print(Array)
    #print(Geo)
    #print(Date)

    #csv_Rows = zip_longest(csv_Rows)
    csv_Rows = list(zip_longest(
                        Date_,
                        Array_,
                        Type_,
                        Division_,
                        Geo_,
                        Serial_Number_,
                        Used_,
                        Failed_,
                        Free_,
                        Total_Capacity_,
                        Percent_Used_,
                        Percentage_Used_,
                        x_
                    ))
    

    with open(f'fakeName.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)

        # write the headers
        writer.writerow(csv_Headings)
        # write the rows
        for write_data in csv_Rows:
            writer.writerow(write_data)

    # Removes all unnecessary chars/symbols from csv file
    # reading the CSV file
    text = open("fakeName.csv", "r")

    #join() method combines all contents of 
    # csv file and formed as a string
    text = ''.join([i for i in text]) 

    # search and replace the contents
    text = text.replace("[None]", "") 
    text = text.replace("None", "") 
    text = text.replace("[]", "") 
    text = text.replace("[", "") 
    text = text.replace("]", "") 
    text = text.replace(f"[\'", "")
    text = text.replace(f"\']", "")
    text = text.replace(f"\'", "")

    # NeedsName.csv is the output file opened in write mode
    x = open("fakeName.csv","w")

    # all the replaced text is written back to NeedsName.csv file
    x.writelines(text)
    x.close()