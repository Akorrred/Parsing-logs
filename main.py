import psycopg2
from final import Parser
from config import host, user, db_name, password
import argparse

parser = argparse.ArgumentParser(
                    prog='Course work',
                    description='What the program does',
                    epilog='Text at the bottom of help',
                    add_help=True)

parser.add_argument('--dashboard')
parser.add_argument('--user')
parser.add_argument('-o', dest='output')

parser.add_argument('--actions-dashboard', dest='actionsDashboard')
parser.add_argument('--actions-dashboard-Name', dest='actionsDashboardName', default='actionsDashboard.txt')

parser.add_argument('--actions-user', dest='actionsUser')
parser.add_argument('--actions-user-Name', dest='actionsUserName', default='actionsUser.txt')

parser.add_argument('--graph-weekday', dest='graphWeekday', action='store_true')
parser.add_argument('--gw-name', dest='graphWeekdayName', default='weekday.png')

parser.add_argument('--graph-hour', dest='graphHour', action='store_true')
parser.add_argument('--gh-name', dest='graphHourName', default='hour.png')

parser.add_argument('--graph-weekday-monthes', dest='graphWeekdayMonthes', action='store_true')
parser.add_argument('--gwm-name', dest='graphWeekdayMonthesName', default='weekdayMonthes.png')

parser.add_argument('--graph-hour-monthes', dest='graphHourMonthes', action='store_true')
parser.add_argument('--ghm-name', dest='graphHourMonthesName', default='hourMonthes.png')

parser.add_argument('--lastAction', dest='lastAction', action='store_true')
parser.add_argument('--lastActionName', dest='lastActionName', default='lastAction.txt')

parser.add_argument('--lastUse', dest='lastUse')
parser.add_argument('--lastUseName', dest='lastUseName', default='lastUse.txt')

parser.add_argument('--count', dest='count')
parser.add_argument('--countName', dest='countName', default='count.txt')

parser.add_argument('--count-interval', dest='countInterval')
parser.add_argument('--countIntervalName', dest='countIntervalName', default='countInterval.txt')


args = parser.parse_args()

try:
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )

    parser = Parser(args, connection)
    parser.parseArgs()

except Exception as ex:
    print("[INFO] Error while working with PostgreSQL", ex)

