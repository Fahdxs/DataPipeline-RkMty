import csv
import json



def character_json_parse(pwd, endpoint):

    jsonfilepath = pwd + endpoint + '.json'

    with open(jsonfilepath, 'r') as file:
        data = json.load(file)

    rows = []

    for items in data['results']:
        row = {
            'id' : items['id'],
            'name' : items['name'],
            'status' : items['status'],
            'type' : items['type'],
            'gender' : items['gender'],
            'image' : items['image']
        }

        rows.append(row)

    csvfilepath = pwd + endpoint + '.csv'

    with open(csvfilepath, mode= 'w', newline= "") as file:
        fieldnames = rows[0].keys()
        writer = csv.DictWriter(file, fieldnames= fieldnames)

        writer.writeheader()
        writer.writerows(rows)



def location_json_parse(pwd, endpoint):

    jsonfilepath = pwd + endpoint + '.json' 

    with open(jsonfilepath, 'r') as file:
        data = json.load(file)

    rows = []

    for items in data['results']:
        char_ids = []

        for i in items['residents']:
            char_ids.append(i.split('/')[5])

        row = {
            'id' : items['id'],
            'name' : items['name'],
            'type' : items['type'],
            'dimension' : items['dimension'],
            'char_ids' : char_ids
        }

        rows.append(row)

    csvfilepath = pwd + endpoint + '.csv' 

    with open(csvfilepath, mode= 'w', newline= "") as file:
        fieldnames = rows[0].keys()
        writer = csv.DictWriter(file, fieldnames= fieldnames)

        writer.writeheader()
        writer.writerows(rows)


def episode_json_parse(pwd, endpoint):

    jsonfilepath = pwd + endpoint + '.json'

    with open(jsonfilepath, 'r') as file:
        data = json.load(file)

    rows = []

    for items in data['results']:

        char_ids = []

        for i in items['characters']:
            char_ids.append(i.split('/')[5])

        row = {
            'id' : items['id'],
            'name' : items['name'],
            'air_date' : items['air_date'],
            'episode' : items['episode'],
            'char_ids' : char_ids
        }

        rows.append(row)

        
    csvfilepath = pwd + endpoint + '.csv'

    with open(csvfilepath, mode= 'w', newline= "") as file:
        fieldnames = rows[0].keys()
        writer = csv.DictWriter(file, fieldnames= fieldnames)

        writer.writeheader()
        writer.writerows(rows)

    


