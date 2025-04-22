import json
import os 


def check_data(path, endpoint, filetype= '.json'):

    fullpath = os.path.join(path, endpoint + filetype)

    if not os.path.exists(fullpath):

        return f'{endpoint}_tasks.fetch_{endpoint}_data'

    with open(fullpath, 'r') as file:

        data = json.load(file)

        if not data:

            return f'{endpoint}_tasks.{endpoint}_api'

        else:
            
            return f'{endpoint}_tasks.{endpoint}_csv_generator'
