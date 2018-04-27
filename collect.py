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
from dateutil import parser
from dateutil import tz

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
            email
          }
        }
      }
    } """
    raw_collabs = run_query(query_collabs)
    collabs = {}

    for collab in raw_collabs['data']['repository']['collaborators']['nodes']:
        if collab['login'] == 'arkye' or collab['login'] == 'pyup-bot':
            continue
        collabs[collab['login']] = {"id": collab['id'], "email": collab['email']}

    return collabs


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
        if branch['name'] == 'development' or branch['name'] == 'master':
            continue
        branches.append(branch['name'])

    return branches


def get_week_day(weekday):
    """ Expect one of dateutil weekdays:
        MO, TU, WE, TH, FR, SA, SU"""

    last_week_day = datetime.date.today() + relativedelta(weekday=weekday(-1))

    return str(last_week_day)


# TODO: Refact this method
def get_co_authored(response):
    co_authoreds = {}
    messages = response['nodes']
    if messages:
        for message in messages:
            if message['messageBody']:
                body = message['messageBody'].split('Co-authored-by:')
                body.pop(0)
                for co_authored in body:
                    author = co_authored.split("<")[1].split(">")[0]
                    try:
                        co_authoreds[author] += 1
                    except KeyError:
                        co_authoreds[author] = 1

    return co_authoreds


def clean_old_commits(response, date):
    """
    Remove commits that have authored date different
    from the committed date, that means old commits pushed
    togheter with the new ones
    """

    date = parser.parse(date)
    date = date.date()
    nodes = response["data"]["repository"]["ref"]["target"]["history"]["nodes"]
    total_count = response["data"]["repository"]["ref"]["target"]["history"]["totalCount"]
    clean_commits = []

    if nodes:
        for commit in nodes:
            commit_date = parser.parse(commit['authoredDate'])
            commit_date = commit_date.astimezone(tz.tzlocal())
            commit_date = commit_date.date()
            if commit_date != date:
                total_count -= 1
                continue
            clean_commits.append(commit)

    return {"totalCount": total_count, "nodes": clean_commits}


def get_commits(weekday):
    branches = get_branches()
    collabs = get_collabs()
    day = get_week_day(weekday)
    result = {}
    co_authoreds = {}

    for collab in collabs:
        print("Collecting commits of {} on {}".format(collab, day))
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
                        nodes {
                          authoredDate
                          messageBody
                        }
                      }
                    }
                  }
                }
              }
            }
            """ % {'branch': branch, 'author': collabs[collab]['id'], 'date': day}
            response = run_query(query)
            response = clean_old_commits(response, day)
            co_authoreds = {**co_authoreds, **get_co_authored(response)}
            number += response["totalCount"]

        result[collab] = number
    return result


def get_commits_of_week():
    weekdays = (WE,)
    result = {}

    for weekday in weekdays:
        result[str(weekday)] = get_commits(weekday)

    return result


def write_json_to_csv(result_dict):
    df = pandas.DataFrame.from_dict(result_dict, orient='index')
    return df.to_csv('data.csv', sep='\t', encoding='utf-8')


result = get_commits_of_week()
write_json_to_csv(result)

