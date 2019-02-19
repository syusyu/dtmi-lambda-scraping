import requests
import os
import boto3
import bs4
import datetime
import hashlib
from boto3.session import Session


def lambda_handler(event, context):
    dynamodb = prepare_dynamodb()
    users = fetch_user(dynamodb)
    users = scraping_main(users)
    # print(users)
    return users
    update_user(dynamodb, users)


def prepare_dynamodb():
    if os.environ.get('EXEC_ENV') == 'TEST':
        session = Session(profile_name='local-dynamodb-user')
        dynamodb = session.resource('dynamodb')
    else:
        dynamodb = boto3.resource('dynamodb')
    return dynamodb


def fetch_user(dynamodb):
    # return [{'NotifyToken': 'www', 'SearchWords': [], 'UserId': 'user02'}, {'SearchWords': ['JP', 'BO', 'NL'], 'UserId': 'user01'}]
    table = dynamodb.Table('User')
    response = table.scan()
    data = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    print(data)
    return data


def scraping_main(users):
    for user in users:
        programs = []
        for search_word in user['SearchWords']:
            programs.extend(scraping_programs(search_word))

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
            'ProgramId': hashlib.md5((program['title'] + str(program['date'])).encode('utf-8')).hexdigest(),
            'SearchWord': search_word,
            'Link': program['link'],
            'Notify': 1
        })
    return result


def scraping_execute(search_word):
    search_word = 'オランダ'
    response = requests.post('https://tv.yahoo.co.jp/search/category/',
                             data={'q': search_word, 'a': '23', 'oa': '1', 'tv': '1', 'bsd': '1'});
    soup = bs4.BeautifulSoup(response.content)
    search_result = soup.select('.programlist li')
    print('len=' + str(len(search_result)))

    result = []
    for elem in search_result:
        date = get_date(elem.select('.leftarea > p.yjMS ')[0].get_text())
        time = elem.select('.leftarea > p:nth-of-type(2) ')[0].get_text()
        program = elem.select('.rightarea > p.yjLS')[0]
        station = elem.select('.rightarea > p.yjMS > span.pr35')[0].get_text()
        title = program.get_text()
        link = program.select('a')[0].get('href')
        result.append({'date': date, 'time': time, 'station': station, 'title': title, 'link': link})

    for elem in result:
        print(str(elem['date']) + '/' + elem['time'] + ' / ' + elem['station'] + ' / '+ elem['title'] + ', ' + elem['link'])

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
    return date


def update_user(dynamodb, users):
    for user in users:
        table = dynamodb.Table('User')
        response = table.update_item(
            Key={
                'UserId': user['UserId']
            },
            UpdateExpression="set Programs = :p",
            ExpressionAttributeValues={
                ':p': user['Programs']
            },
            ReturnValues="UPDATED_NEW"
        )
    return users
