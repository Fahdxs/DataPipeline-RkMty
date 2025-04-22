import json
import csv
import requests




def fetch_data(url,endpoint):

    fullurl = url + f'{endpoint}'

    try:

        response = requests.get(fullurl)

        if response.status_code == 200:

            filepath = '/opt/airflow/data/' + f'{endpoint}' + '.json'

            with open(filepath, 'w') as file:
                json.dump(response.json(), file, indent= 4)

            print('Succefully dumped date in a Json File')
    
        else:

            error_message = f'Failed to Fetch data from API . Status : {response.status_code}'
            print(error_message)
            raise Exception(error_message)
    
    except requests.exceptions.Timeout:

        print("Request Timed Out")
        raise

    except requests.exceptions.RequestException as e:

        print(f"An error occurred: {e}")
        raise
