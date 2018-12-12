import pandas as pd
import numpy as np
from turbodbc import connect
import datetime
import dateutil.relativedelta
        
today = datetime.date.today()
today = today.replace(year=2016)
today_prev_year = today.replace(year=2015)

first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
start_date = lastMonth.strftime("%Y-%m-")+'01'
end_date = first.strftime("%Y-%m-%d")

first_prev_year = today_prev_year.replace(day=1)
lastMonth_prev_year = first_prev_year - datetime.timedelta(days=1)
start_date_prev_year = lastMonth_prev_year.strftime("%Y-%m-")+'01'
end_date_prev_year = first_prev_year.strftime("%Y-%m-%d")

start_date_prev_month = datetime.datetime.strptime(start_date,"%Y-%m-%d") - dateutil.relativedelta.relativedelta(months=1)
start_date_prev_month = start_date_prev_month.strftime("%Y-%m-%d")

connection = connect(dsn='AradRoundRock',uid='OriKronfeld',pwd='Basket76&Galil')
meter = '5478'

sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter)+"' and ConsValid='1' and ConsInterval<'"+str(end_date)+"' and ConsInterval>='"+str(start_date)+"'"
MonthlyReadingPrevMonth = pd.read_sql(sql,connection)
sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter)+"' and ConsValid='1' and ConsInterval<'"+str(start_date)+"' and ConsInterval>='"+str(start_date_prev_month)+"'"
MonthlyReadingPrev2Months = pd.read_sql(sql,connection)
sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter)+"' and ConsValid='1' and ConsInterval<'"+str(end_date_prev_year)+"' and ConsInterval>='"+str(start_date_prev_year)+"'"
MonthlyReadingPrevMonthYear = pd.read_sql(sql,connection)

connection.close()
