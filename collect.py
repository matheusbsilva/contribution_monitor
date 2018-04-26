"""
Collect commits of each user on github graphql API
"""

import datetime
import requests
import os
import json
import pandas

from dotenv import find_dotenv
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from dateutil.relativedelta import MO, TU, WE, TH, FR, SA, SU

load_dotenv(find_dotenv())

TOKEN = "Bearer {token}".format(token=os.getenv('TOKEN'))
HEADERS = {"Authorization": TOKEN}


def run_query(query):
    # TODO: Probably is the parsing of the query string to json that is taking too long 
    # 2-5 seconds
    request = requests.post('https://api.github.com/graphql',
                            json={'query': query}, headers=HEADERS)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))


def get_collabs():
    query_collabs = """
    {
      repository(name: "2018.1-TropicalHazards-BI", owner: "fga-gpp-mds") {
        collaborators(first: 50, affiliation: DIRECT){
          nodes {
            id
            login
          }
        }
      }
    } """
    raw_collabs = run_query(query_collabs)
    collabs = []

    for collab in raw_collabs['data']['repository']['collaborators']['nodes']:
        if collab['login'] == 'arkye' or collab['login'] == 'pyup-bot':
            continue
        collabs.append((collab['login'], collab['id']))

    return tuple(collabs)


def get_branches():

    query_branches = """
    {
      repository(name: "2018.1-TropicalHazards-BI", owner: "fga-gpp-mds") {
        refs(first: 50, refPrefix: "refs/heads/"){
          nodes {
            name
          }
        }
      }
    }
    """
    raw_branches = run_query(query_branches)
    branches = []

    for branch in raw_branches['data']['repository']['refs']['nodes']:
        branches.append(branch['name'])

    return branches


def get_week_day(weekday):
    """ Expect one of dateutil weekdays:
        MO, TU, WE, TH, FR, SA, SU"""

    last_week_day = datetime.date.today() + relativedelta(weekday=weekday(-1))

    return str(last_week_day)


def get_commits(weekday):
    branches = get_branches()
    collabs = get_collabs()
    day = get_week_day(weekday)
    result = {}
    for collab in collabs:
        print("Collecting commits of {} on {}".format(collab[0], day))
        number = 0
        for branch in branches:
            query = """
            {
              repository(name: "2018.1-TropicalHazards-BI", owner: "fga-gpp-mds") {
                ref(qualifiedName: "%(branch)s") {
                  target {
                    ... on Commit {
                      history(first: 50, since: "%(date)sT00:00:00-03:00", until: "%(date)sT23:59:59-03:00", author: {id: "%(author)s"}) {
                        totalCount
                      }
                    }
                  }
                }
              }
            }
            """ % {'branch': branch, 'author': collab[1], 'date': day}
            response = run_query(query)
            number += response["data"]["repository"]["ref"]["target"]["history"]["totalCount"]

        result[collab[0]] = number
    return result


def get_commits_of_week():
    weekdays = (MO, TU, WE)
    result = {}

    for weekday in weekdays:
        result[str(weekday)] = get_commits(weekday)

    return result


def write_json_to_csv(result_dict):
    df = pandas.DataFrame.from_dict(result_dict, orient='index')
    return df.to_csv('data.csv', sep='\t', encoding='utf-8')


result = get_commits_of_week()
write_json_to_csv(result)

