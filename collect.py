"""
Collect commits of each user on github graphql API
"""

import datetime
import requests
import os
import pandas

from dotenv import find_dotenv
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from dateutil.relativedelta import MO, TU, WE, TH, FR, SA, SU
from dateutil import parser
from dateutil import tz

load_dotenv(find_dotenv())

TOKEN = "Bearer {token}".format(token=os.getenv('TOKEN'))
HEADERS = {"Authorization": TOKEN, "Content-Type": "application/json"}


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


def arrange_co_authoreds(co_authoreds: dict, new_co_authoreds: dict):
    for co_author in new_co_authoreds:
        try:
            co_authoreds[co_author] += new_co_authoreds[co_author]
        except KeyError:
            co_authoreds[co_author] = new_co_authoreds[co_author]

    return co_authoreds


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
            raw_co_authores = get_co_authored(response)
            co_authoreds = arrange_co_authoreds(co_authoreds, raw_co_authores)
            number += response["totalCount"]

        result[collab] = number
    return {"commits": result, "co_authoreds": co_authoreds}


# TODO: Fix this hardcoded emails and users
def rename_email_columns(df: pandas.DataFrame):
    return df.rename(
                     columns={'joaok8@gmail.com': 'jppgomes',
                              'pedrodaniel.unb@gmail.com': 'pdaniel37',
                              'maxhb.df@gmail,com': 'Maxlobo',
                              'matheusbattista@hotmail.com': 'matheusbsilva',
                              'arthur120496@gmail.com': 'arthur0496',
                              'andre.filho001@outlook.com': 'andre-filho',
                              'renata.rfsouza@gmail.com': 'Renatafsouza',
                              'edry97@hotmail.com': 'Yoshida-Eduardo',
                              'andrebargas@gmail.com': 'andrebargas',
                              'iagovc_2012@hotmail.com': 'IagoCarvalho'})


def sum_data_frames(df1, df2):
    return df1.add(df2).fillna(df1)


def turn_into_df(result: dict):
    return pandas.DataFrame.from_dict(result, orient='index')


def get_commits_of_week():
    weekdays = (TU, WE)
    result_commits = {}
    result_co_authoreds = {}

    for weekday in weekdays:
        commits = get_commits(weekday)
        result_commits[str(weekday)] = commits['commits']
        result_co_authoreds[str(weekday)] = commits['co_authoreds']

    return {"commits": result_commits, "co_authoreds": result_co_authoreds}


def write_json_to_csv(df: pandas.DataFrame):
    return df.to_csv('data.csv', sep='\t', encoding='utf-8')


result = get_commits_of_week()
df_commit = turn_into_df(result['commits'])
df_auhored = turn_into_df(result['co_authoreds'])

sum_df = sum_data_frames(df_commit, rename_email_columns(df_auhored))
write_json_to_csv(sum_df)


