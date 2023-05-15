import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="darkgrid")

class Parser():
    def __init__(self, args, connection):
        self.args = args
        self.connection = connection

    def getLastUse(self, cursor):
        delta = datetime.timedelta(days=int(self.args.lastUse))

        cursor.execute(
            """WITH  lastActions AS
            (SELECT dashboard_id, MAX(dttm) as last FROM logs GROUP BY dashboard_id)
            SELECT dashboard_id, last FROM lastActions WHERE last<%s;""", ((datetime.datetime.now() - delta),)
        )

        result = cursor.fetchall()
        if self.args.output is not None:
            lastUse = open(self.args.output, 'w')
        else:
            lastUse = open(self.args.lastUseName, 'w')
        for i in range(len(result)):
            lastUse.write(f"Dashboard id: {str(result[i][0])}, date: {str(result[i][1])}\n")
        lastUse.close()

    def getActionsByDashboard(self, cursor):
        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT action, dttm FROM logs WHERE dashboard_id=%s ORDER BY dttm DESC;""",
                ((self.args.dashboard),)
            )
        else:
            raise Exception("Set dashboard")

        result = cursor.fetchall()
        if self.args.output is not None:
            dashboardActions = open(self.args.output, 'w')
        else:
            dashboardActions = open(self.args.actionsDashboard, 'w')

        for i in range(len(result)):
            dashboardActions.write(f"Action: {str(result[i][0])}, date: {str(result[i][1])}\n")
        dashboardActions.close()

    def getActionsByUser(self, cursor):
        if self.args.user is not None:
            cursor.execute(
                """SELECT action, dttm FROM logs WHERE user_id=%s ORDER BY dttm DESC;""",
                ((self.args.user),)
            )
        else:
            raise Exception("Set user")

        result = cursor.fetchall()
        if self.args.output is not None:
            userActions = open(self.args.output, 'w')
        else:
            userActions = open(self.args.actionsUserName, 'w')

        for i in range(len(result)):
            userActions.write(f"Action: {str(result[i][0])}, date: {str(result[i][1])}\n")
        userActions.close()


    def getCount(self, cursor):
        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT dttm, dashboard_id FROM logs WHERE dashboard_id=%s;""", ((self.args.dashboard),)
            )
        else:
            cursor.execute(
                """SELECT dttm, dashboard_id FROM logs;"""
            )
        result = cursor.fetchall()

        delta = datetime.timedelta(days=int(self.args.count))
        dashboardToCount = {}

        for i in range(len(result)):
            if (result[i][0] > (datetime.datetime.now() - delta)):
                if result[i][1] is not None:
                    if result[i][1] in dashboardToCount:
                        dashboardToCount[result[i][1]] += 1
                    else:
                        dashboardToCount[result[i][1]] = 1


        if self.args.output is not None:
            countFile = open(self.args.output, 'w')
        else:
            countFile = open(self.args.countName, 'w')
        for key in dashboardToCount:
            countFile.write(f"Dashboard id: {key}, count: {dashboardToCount[key]}.\n")
        countFile.close()

    def getCountInterval(self, cursor):
        timeFrom, timeTo = self.args.countInterval.split('-')
        deltaFrom = datetime.datetime.strptime(timeFrom, '%Y.%m.%d')
        deltaTo = datetime.datetime.strptime(timeTo, '%Y.%m.%d')

        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT dttm, dashboard_id FROM logs WHERE dashboard_id=%s;""", ((self.args.dashboard),)
            )
        else:
            cursor.execute(
                """SELECT dttm, dashboard_id FROM logs;"""
            )
        result = cursor.fetchall()


        dashboardToCount = {}

        for i in range(len(result)):
            if (result[i][0] > deltaFrom and result[i][0] < deltaTo):
                if result[i][1] is not None:
                    if result[i][1] in dashboardToCount:
                        dashboardToCount[result[i][1]] += 1
                    else:
                        dashboardToCount[result[i][1]] = 1

        if self.args.output is not None:
            countFile = open(self.args.output, 'w')
        else:
            countFile = open(self.args.countName, 'w')
        for key in dashboardToCount:
            countFile.write(f"Dashboard id: {key}, count: {dashboardToCount[key]}.\n")
        countFile.close()

    def lastAction(self, cursor):
        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT dashboard_id, MAX(dttm) as last FROM logs WHERE dashboard_id=%s GROUP BY dashboard_id;""",
                ((self.args.dashboard),)
            )
        else:
            cursor.execute(
                """SELECT dashboard_id, MAX(dttm) as last FROM logs GROUP BY dashboard_id;"""
            )

        result = cursor.fetchall()
        if self.args.output is not None:
            lastAction = open(self.args.output, 'w')
        else:
            lastAction = open(self.args.lastActionName, 'w')

        for i in range(len(result)):
            lastAction.write(f"Dashboard id: {str(result[i][0])}, date: {str(result[i][1])}\n")
        lastAction.close()

    def graphWeekday(self, cursor):
        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT id, dttm FROM logs WHERE dashboard_id=%s;""", ((self.args.dashboard),)
            )
        else:
            cursor.execute(
                """SELECT id, dttm FROM logs;"""
            )

        result = cursor.fetchall()
        logs = pd.DataFrame(result, columns=[c[0] for c in cursor.description])
        logs['dttm'] = pd.to_datetime(logs['dttm'], errors='coerce')

        logs['weekday'] = logs['dttm'].dt.day_name()
        groupby_weekday = logs.groupby('weekday').agg({'id': 'count'}).reset_index()
        groupby_weekday.rename(columns={'id': 'count'}, inplace=True)

        sns.relplot(data=groupby_weekday, x='weekday', y='count', height=5, aspect=2, kind='line')
        plt.title('Dependence of the number of actions on the day of the week')
        if self.args.output is not None:
            plt.savefig(self.args.output)
        else:
            plt.savefig(self.args.graphWeekdayName)

    def graphHour(self, cursor):
        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT id, dttm FROM logs WHERE dashboard_id=%s;""", ((self.args.dashboard),)
            )
        else:
            cursor.execute(
                """SELECT id, dttm FROM logs;"""
            )
        result = cursor.fetchall()
        logs = pd.DataFrame(result, columns=[c[0] for c in cursor.description])
        logs['dttm'] = pd.to_datetime(logs['dttm'], errors='coerce')

        logs['hour'] = logs['dttm'].dt.hour
        groupby_hour = logs.groupby('hour').agg({'id': 'count'}).reset_index()
        groupby_hour.rename(columns={'id': 'count'}, inplace=True)

        sns.relplot(data=groupby_hour, x='hour', y='count', height=5, aspect=2, kind='line')
        plt.title('Dependence of the number of actions on the hours in a day')
        if self.args.output is not None:
            plt.savefig(self.args.output)
        else:
            plt.savefig(self.args.graphHourName)

    def graphWeekdayMonthes(self, cursor):
        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT id, dttm FROM logs WHERE dashboard_id=%s;""", ((self.args.dashboard),)
            )
        else:
            cursor.execute(
                """SELECT id, dttm FROM logs;"""
            )
        result = cursor.fetchall()
        logs = pd.DataFrame(result, columns=[c[0] for c in cursor.description])
        logs['dttm'] = pd.to_datetime(logs['dttm'], errors='coerce')

        logs['weekday'] = logs['dttm'].dt.day_name()
        logs['month'] = logs['dttm'].dt.month_name()

        groupby_weekday_month = logs.groupby(['weekday', 'month']).agg({'id': 'count'}).reset_index()
        groupby_weekday_month.rename(columns={'id': 'count'}, inplace=True)

        sns.relplot(data=groupby_weekday_month, x='weekday', y='count', height=8, aspect=2, kind='line', hue='month')
        plt.title('Dependence of the number of actions on the day of the week for different monthes')
        if self.args.output is not None:
            plt.savefig(self.args.output)
        else:
            plt.savefig(self.args.graphWeekdayMonthesName)

    def graphHourMonthes(self, cursor):
        if self.args.dashboard is not None:
            cursor.execute(
                """SELECT id, dttm FROM logs WHERE dashboard_id=%s;""", ((self.args.dashboard),)
            )
        else:
            cursor.execute(
                """SELECT id, dttm FROM logs;"""
            )
        result = cursor.fetchall()
        logs = pd.DataFrame(result, columns=[c[0] for c in cursor.description])
        logs['dttm'] = pd.to_datetime(logs['dttm'], errors='coerce')

        logs['hour'] = logs['dttm'].dt.hour
        logs['month'] = logs['dttm'].dt.month_name()

        groupby_hour_month = logs.groupby(['hour', 'month']).agg({'id': 'count'}).reset_index()
        groupby_hour_month.rename(columns={'id': 'count'}, inplace=True)

        sns.relplot(data=groupby_hour_month, x='hour', y='count', height=8, aspect=2, kind='line', hue='month')
        plt.title('Dependence of the number of actions on the hours in a day for different monthes')
        if self.args.output is not None:
            plt.savefig(self.args.output)
        else:
            plt.savefig(self.args.graphHourMonthesName)

    def parseArgs(self):
        with self.connection.cursor() as cursor:
            if self.args.graphWeekday:
                self.graphWeekday(cursor)
            if self.args.graphHour:
                self.graphHour(cursor)
            if self.args.graphWeekdayMonthes:
                self.graphWeekdayMonthes(cursor)
            if self.args.graphHourMonthes:
                self.graphHourMonthes(cursor)
            if self.args.lastAction:
                self.lastAction(cursor)
            if self.args.count:
                self.getCount(cursor)
            if self.args.countInterval:
                self.getCountInterval(cursor)
            if self.args.actionsUser:
                self.getActionsByUser(cursor)
            if self.args.actionsDashboard:
                self.getActionsByDashboard(cursor)
            if self.args.lastUse:
                self.getLastUse(cursor)
