import configparser
import csv
import json
import requests

config = configparser.ConfigParser()
config.read('config.ini')
URL = config.get('NocoDB', 'url')
PROJECT_ID = config.get('NocoDB', 'project_id')
API_TOKEN = config.get('NocoDB', 'xc-auth')
CONTENT_TYPE = config.get('NocoDB', 'type')

header = {
    'xc-auth': API_TOKEN,
    "accept": CONTENT_TYPE
    }

def convertToCSV(id):
    getRowByID = f'{URL}/' + str(id)
    r = requests.get(getRowByID, headers=header)
    dictionary = json.loads(r.text)
 
    date = [dictionary['Date']]
    array = [dictionary['Array']]
    type = [dictionary['Type']]
    division = [dictionary['Division']]
    geo = [dictionary['Geo']]
    serialNum = [dictionary['SerialNumber']]
    used = [dictionary['Used']]
    failed = [dictionary['Failed']]
    free = [dictionary['Free']]
    totalCapacity = [dictionary['TotalCapacity']]
    percentUsed = [dictionary['PercentUsed']]
    percentUsedSymbol = [dictionary['PercentUsedString']]
    x = [dictionary['x']]

    csv_Headers = [
                    "Date",
                    "Array",
                    "Type",
                    "Division",
                    "Geo",
                    "Serial Number",
                    "Used",
                    "Failed",
                    "Free",
                    "Total Capacity",
                    "Percent Used",
                    "Percent Used(%)",
                    "x"
                   ]

    csv_Rows = list(zip(
                    date,
                    array,
                    type,
                    division,
                    geo,
                    serialNum,
                    used,
                    failed,
                    free,
                    totalCapacity,
                    percentUsed,
                    percentUsedSymbol,
                    x
                ))

    name = dictionary['Name']
    with open(f'{name}.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)

        # write the headers
        writer.writerow(csv_Headers)
        # write the rows
        for data in csv_Rows:
            writer.writerow(data)


if __name__ == '__main__':
    convertToCSV(3)