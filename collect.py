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
REPO_GITHUB = "2018.1-TropicalHazards-BI-FrontEnd"
OWNER_REPO = "fga-gpp-mds"
FILE_NAME = "front.csv"


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
      repository(name: "%(repo)s", owner: "%(owner)s") {
        collaborators(first: 50, affiliation: DIRECT){
          nodes {
            id
            login
            email
          }
        }
      }
    } """ % {"repo": REPO_GITHUB, "owner": OWNER_REPO}
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
      repository(name: "%(repo)s", owner: "%(owner)s") {
        refs(first: 50, refPrefix: "refs/heads/"){
          nodes {
            name
          }
        }
      }
    }
    """ % {"repo": REPO_GITHUB, "owner": OWNER_REPO}
    raw_branches = run_query(query_branches)
    branches = []

    for branch in raw_branches['data']['repository']['refs']['nodes']:
        if branch['name'] == 'master':
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
                    try:
                        author = co_authored.split("<")[1].split(">")[0].lower()
                        co_authoreds[author] += 1
                    except KeyError:
                        co_authoreds[author] = 1
                    except IndexError:
                        continue

    return co_authoreds


def clean_commits(response, date, hash_list):
    """
    Remove commits that have authored date different
    from the committed date, that means old commits pushed
    togheter with the new ones and duplicated commits
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
            if (commit_date != date or hash_list.count(commit['abbreviatedOid']) > 1):
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


def list_commits_hash(response, hash_list):
    nodes = response["data"]["repository"]["ref"]["target"]["history"]["nodes"]
    for commit in nodes:
        hash_list.append(commit['abbreviatedOid'])

    return hash_list


def get_commits(weekday):
    branches = get_branches()
    collabs = get_collabs()
    day = get_week_day(weekday)
    result = {}
    co_authoreds = {}

    for collab in collabs:
        print("Collecting commits of {} on {}".format(collab, day))
        number = 0
        hash_list = []

        for branch in branches:
            query = """
            {
              repository(name: "%(repo)s", owner: "%(owner)s") {
                ref(qualifiedName: "%(branch)s") {
                  target {
                    ... on Commit {
                      history(first: 50, since: "%(date)sT00:00:00-03:00", until: "%(date)sT23:59:59-03:00", author: {id: "%(author)s"}) {
                        totalCount
                        nodes {
                          authoredDate
                          messageBody
                          abbreviatedOid
                        }
                      }
                    }
                  }
                }
              }
            }
            """ % {"repo": REPO_GITHUB, "owner": OWNER_REPO,
                    'branch': branch, 'author': collabs[collab]['id'],
                    'date': day, }

            response = run_query(query)

            hash_list = list_commits_hash(response, hash_list)
            response = clean_commits(response, day, hash_list)

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
                              'maxhb.df@gmail.com': 'Maxlobo',
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
    weekdays = (MO, TU, WE, TH, FR, SA)
    result_commits = {}
    result_co_authoreds = {}

    for weekday in weekdays:
        commits = get_commits(weekday)
        result_commits[str(weekday)] = commits['commits']
        result_co_authoreds[str(weekday)] = commits['co_authoreds']

    return {"commits": result_commits, "co_authoreds": result_co_authoreds}


def write_json_to_csv(df: pandas.DataFrame):
    return df.to_csv(FILE_NAME, sep='\t', encoding='utf-8')


result = get_commits_of_week()

df_commit = turn_into_df(result['commits'])
df_auhored = turn_into_df(result['co_authoreds'])
rename = rename_email_columns(df_auhored)
sum_df = sum_data_frames(df_commit, rename)
write_json_to_csv(sum_df)

