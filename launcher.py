import os
import function


def main():
    os.environ['EXEC_ENV'] = 'TEST'
    function.lambda_handler({}, {})


main()
