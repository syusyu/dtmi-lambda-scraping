import requests
import os
import boto3
import bs4
import datetime
import hashlib
# import json
import simplejson as json

from boto3.session import Session
from requests_aws4auth import AWS4Auth
import re


def lambda_handler(event, context):
    dynamodb = prepare_dynamodb()
    users = fetch_user(event, dynamodb)
    users = scraping_main(users)
    update_user(dynamodb, users)


def prepare_dynamodb():
    if os.environ.get('EXEC_ENV') == 'TEST':
        session = Session(profile_name='local-dynamodb-user')
        dynamodb = session.resource('dynamodb')
    else:
        dynamodb = boto3.resource('dynamodb')
    return dynamodb


def fetch_user(event, dynamodb):
    if 'user_id' in event and event['user_id']:
        return specify_user(dynamodb, event['user_id'])
    else:
        return scan_user(dynamodb)


def specify_user(dynamodb, user_id):
    return [{'UserId': 'xxxx', 'SearchWords': ["スペイン", "移住"]}]
    # table = dynamodb.Table('User')
    # response = table.scan()
    # data = response['Items']
    # while 'LastEvaluatedKey' in response:
    #     response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    #     data.extend(response['Items'])
    # return data


def scan_user(dynamodb):
    table = dynamodb.Table('User')
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return data


def scraping_main(users):
    for user in users:
        programs = []
        if 'SearchWords' not in user:
            continue
        for search_word in user['SearchWords']:
            programs.append({'SearchWord': search_word, 'Programs': scraping_programs(search_word)})

        user['Programs'] = programs
    return users


def scraping_programs(search_word):
    result = []
    programs = scraping_execute(search_word)
    for program in programs:
        result.append({
            'Title': program['title'],
            'Station': program['station'],
            'Date': program['date'],
            'ProgramId': hashlib.md5((program['title'] + program['date']).encode('utf-8')).hexdigest(),
            'Link': program['link'],
            'Notify': 1
        })
    return result


def scraping_execute(search_word):
    response = requests.post('https://tv.yahoo.co.jp/search/category/',
                             data={'q': search_word, 'a': '23', 'oa': '1', 'tv': '1', 'bsd': '1'});
    soup = bs4.BeautifulSoup(response.content)
    search_result = soup.select('.programlist li')
    # print('len=' + str(len(search_result)))

    result = []
    for elem in search_result:
        date = get_date(elem.select('.leftarea > p.yjMS ')[0].get_text())
        time = elem.select('.leftarea > p:nth-of-type(2) ')[0].get_text()
        program = elem.select('.rightarea > p.yjLS')[0]
        station = elem.select('.rightarea > p.yjMS > span.pr35')[0].get_text()
        title = program.get_text()
        link = program.select('a')[0].get('href')
        result.append({'date': date, 'time': time, 'station': station, 'title': title, 'link': link})

    return result


def get_date(month_day):
    month_day = month_day[0:month_day.find('（')]
    today = datetime.date.today()
    this_year = today.year

    date_str = str(this_year) + '/' + month_day
    date = datetime.datetime.strptime(date_str, "%Y/%m/%d").date()

    if date < today:
        date_str = str(this_year + 1) + '/' + month_day
        date = datetime.datetime.strptime(date_str, "%Y/%m/%d").date()
    return date.strftime('%Y/%m/%d')


def replace_query(org_str):
    words = ["SearchWord", "Programs", "Title", "Station", "Date", "ProgramId", "Link", "Notify"]
    result = org_str
    for word in words:
        result = re.sub('\"' + word + '\"', word, result)
    return result


def update_user(dynamodb, users):
    try:
        credentials = get_credential()
    except Exception as e:
        print(e)
        print('NG')
        return

    access_key_id = credentials['AccessKeyId']
    secret_access_key = credentials["SecretAccessKey"]
    session_token = credentials["SessionToken"]
    region_name = 'ap-northeast-1'
    auth = AWS4Auth(access_key_id, secret_access_key, region_name, 'appsync', session_token=session_token)
    headers = {'Content-Type': 'application/graphql'}

    for user in users:
        print('user.programs={0}'.format(user['Programs']))
        programs = replace_query(json.dumps(user['Programs'])) if 'Programs' in user else {}
        data = {
            'query': 'mutation {{updateUserPrograms(UserId: "{0}", Programs: {1}) {{UserId \
             Programs{{SearchWord Programs{{Title Station Date ProgramId Link Notify}} }} }} }}'.format(
                user['UserId'], programs)}
        response = requests.post(os.environ['APP_SYNC_URL'], headers=headers, data=json.dumps(data), auth=auth)
        print(response)
    return users


def get_credential():
    if os.environ.get('EXEC_ENV') == 'TEST':
        session = Session(profile_name='local-dynamodb-user')
        sts = session.client('sts')
    else:
        sts = boto3.client('sts')

    role_arn = os.environ['ROLE_ARN']
    role = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName='test',
        )
    return role['Credentials']
