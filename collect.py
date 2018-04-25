import requests
import os
import datetime

from dotenv import find_dotenv
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from dateutil.relativedelta import MO, TU, WE, TH, FR, SA, SU


GH_URL = "https://api.github.com"
REPO_URL = "/repos/fga-gpp-mds/2018.1-TropicalHazards-BI"
URL = GH_URL + REPO_URL

load_dotenv(find_dotenv())

USER = os.getenv("USER_GITHUB")
PASS = os.getenv("PASS_GITHUB")


def get_branches():
    branches_url = "/branches"
    url = URL + branches_url
    branches = requests.get(url, auth=(USER, PASS))

    return branches.json()


def get_contributors():
    contributors_url = "/contributors"
    url = URL + contributors_url
    contributors = requests.get(url, auth=(USER, PASS))

    return contributors.json()


def formatted_date(date):
    """ Expect a datetime object as param"""
    return datetime.datetime.strftime(date, "%Y-%m-%dT%H:%M:%S")


def retrive_commits(time):
    commits_url = "/commits"
    url = URL + commits_url + time

    contributors = get_contributors()
    branches = get_branches()

    number = 0
    response = {}
    for contributor in contributors:
        number = 0
        print("Coletando commits do {}".format(contributor['login']))
        for branch in branches:
            if branch['name'] == 'gh-pages' or branch['name'] == 'master' or branch['name'] == 'development':
                continue

            get_url = url + "&sha={}&author={}".format(branch['name'],contributor['login'])
            data = requests.get(get_url, auth=(USER, PASS)).json()
            number += len(data)
        response[contributor['login']] = number

    return response


def get_last_past_weekday(weekday):
    """ Expect one of dateutil weekdays:
    MO, TU, WE, TH, FR, SA, SU"""
    last_weekday_init = datetime.datetime.now() + relativedelta(weekday=weekday(-1), hours=0, minutes=0, seconds=0)
    last_weekday_end = datetime.datetime.now() + relativedelta(weekday=weekday(-1), hours=23, minutes=59, seconds=58)
    last_weekday = {'initial': last_weekday_init, 'end': last_weekday_end}

    return last_weekday


def get_commits_by_day():
    weekdays = (MO, TU, WE, TH, FR, SA, SU)
    day = get_last_past_weekday(MO)
    result_by_days = {}
    for weekday in weekdays:
        commits = retrive_commits(get_last_past_weekday(weekday))
        result_by_days[weekday] =


    commits = retrive_commits(day)

    return commits


result = get_commits_by_day()
import ipdb;ipdb.set_trace()
