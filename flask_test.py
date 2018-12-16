from flask import Flask
from flask_ask import Ask, statement, question
import logging
import pandas as pd
import numpy as np
from turbodbc import connect
import datetime
import dateutil.relativedelta

app = Flask(__name__)
ask = Ask(app, '/')

logging.getLogger("flask_ask").setLevel(logging.DEBUG)

#Date Inputs
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

@ask.launch
def launch():
    welcome_sentence = 'Hello, this is a Arad test skill. Please state a command.'
    return question(welcome_sentence)

@ask.intent('MeterIntent')
def hello(meter_number):
    #meter_number = '5478'
    connection = connect(dsn='AradRoundRock',uid='OriKronfeld',pwd='Basket76&Galil')
    sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval<'"+str(end_date)+"' and ConsInterval>='"+str(start_date)+"'"
    MonthlyReadingPrevMonth = pd.read_sql(sql,connection)
    sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval<'"+str(start_date)+"' and ConsInterval>='"+str(start_date_prev_month)+"'"
    MonthlyReadingPrev2Months = pd.read_sql(sql,connection)
    sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval<'"+str(end_date_prev_year)+"' and ConsInterval>='"+str(start_date_prev_year)+"'"
    MonthlyReadingPrevMonthYear = pd.read_sql(sql,connection)
    connection.close()
    DiffRatioPrevMonth=round((MonthlyReadingPrevMonth-MonthlyReadingPrev2Months)/MonthlyReadingPrev2Months*100,2)
    DiffRatioPrevYear=round((MonthlyReadingPrevMonth-MonthlyReadingPrevMonthYear)/MonthlyReadingPrevMonthYear*100,2)
    if (MonthlyReadingPrevMonth.dropna().empty):
        speech_text = "Round Rock Meter number: "+meter_number+" is not availble"
    else:
        speech_text = "Round Rock Meter number: "+str(meter_number)+" consumption from "+start_date+" to "+end_date+" is "+MonthlyReadingPrevMonth.to_string(index=False)+" cubes, which is a "+DiffRatioPrevMonth.to_string(index=False)+"% difference from the previous month, and a "+DiffRatioPrevYear.to_string(index=False)+"% difference from the previous year"
    return statement(speech_text).simple_card('Hello', speech_text)

@ask.session_ended
def session_ended():
    return "{}", 200

if __name__ == '__main__':
    app.run()