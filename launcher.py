import os
import function


def main():
    os.environ['EXEC_ENV'] = 'TEST'
    os.environ['ROLE_ARN'] = 'arn:aws:iam::914953492051:role/lambda_basic_execution'
    os.environ['APP_SYNC_URL'] = 'https://gezr5gm6lrf5lgczk2q3v34epu.appsync-api.ap-northeast-1.amazonaws.com/graphql/'

    event = {}
    event['user_id'] = 'xxxx'

    function.lambda_handler(event, {})


main()
