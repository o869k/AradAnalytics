from flask import Flask, render_template
from flask_ask import Ask, session, question, statement
import logging
import pandas as pd
from turbodbc import connect #unsuitable for lambda?
import pyodbc
import datetime
import dateutil.relativedelta
import os

import requests
r = requests.get('https://totalconsumptionbetweendatesapp.azurewebsites.net/api/TotalConsumptionBetweenDates?', params={'code':'bQ0X/ZxRzoquK9rrYDt2GFRDtRG1DpjlbdYa7Y2Y9M2h5sE5VRZy9A==',
                                                                                                                        'AccountNo':'0000041696',
                                                                                                                        'DateFrom':'2018-11-01T00:00:00',
                                                                                                                        'DateTo': '2018-11-30T23:59:00'})
print(r.content)

app = Flask(__name__)
ask = Ask(app, '/')
logging.getLogger("flask_ask").setLevel(logging.DEBUG)

#Date Inputs
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
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

#Entities
SESSION_METER = "meter_number"
SESSION_USAGE_DATE = "usage_date"
           
@ask.launch
def launch():
    welcome_sentence = render_template('welcome')
    session.attributes[SESSION_METER] = '?'
    session.attributes[SESSION_USAGE_DATE] = yesterday.strftime("%Y-%m-%d") #by default mean yersteday
    return question(welcome_sentence)

@ask.intent('MyMeterIs', default={'meter_number': None})
def my_meter_is(meter_number):
    #just checking if that's a real number
    #Meter number is {meter_number}
    if meter_number is not None:
        try:
            val = int(meter_number)
        except ValueError:
            meter_number = None
    if meter_number is not None:
        #Need to check for a valid meter
        #Connect to data base
        #PyODBC
        #connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
        #                      "Server=192.168.33.85;"
        #                      "Database=RoundRockTX;"
        #                      "UID=Analytics;"
        #                      "PWD=hweg%^90Fdd;")
        #TurboODBC
        connection = connect(dsn='40.74.254.5',uid='Analytics',pwd='hweg%^90Fdd')
        sql = "select distinct(MeterCount) from dbo.Meters"
        meters_list = pd.read_sql(sql,connection)
        if meter_number in str(meters_list.MeterCount):
            session.attributes[SESSION_METER] = meter_number
            question_text = render_template('known_meter', meter_number=meter_number)
            reprompt_text = render_template('known_meter_reprompt')
        else:
            question_text = render_template('non_valid_meter', meter_number=meter_number)
            reprompt_text = render_template('non_valid_meter_reprompt')  
    else:
        session.attributes[SESSION_METER] = meter_number
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
    return question(question_text).reprompt(reprompt_text)

@ask.intent('MeterStatusPrevMonth')
def meter_status_prev_month(meter_number):
    #meter_number = '5478'
    #What was meter {meter_number} previous month usage
    #What was my previous month usage
    if (meter_number is None) or (meter_number=='?'):
        meter_number = session.attributes.get(SESSION_METER)
    if (meter_number is None) or (meter_number=='?'):
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
        return question(question_text).reprompt(reprompt_text)
    if meter_number is not None:
        #Connect to data base
        #PyODBC
        #connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
        #                      "Server=192.168.33.85;"
        #                      "Database=RoundRockTX;"
        #                      "UID=Analytics;"
        #                      "PWD=hweg%^90Fdd;")
        #TurboODBC
        connection = connect(dsn='40.74.254.5',uid='Analytics',pwd='hweg%^90Fdd')
        sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval<'"+str(end_date)+"' and ConsInterval>='"+str(start_date)+"'"
        MonthlyReadingPrevMonth = pd.read_sql(sql,connection)
        sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval<'"+str(start_date)+"' and ConsInterval>='"+str(start_date_prev_month)+"'"
        MonthlyReadingPrev2Months = pd.read_sql(sql,connection)
        sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval<'"+str(end_date_prev_year)+"' and ConsInterval>='"+str(start_date_prev_year)+"'"
        MonthlyReadingPrevMonthYear = pd.read_sql(sql,connection)
        connection.close()
        if (MonthlyReadingPrevMonth.dropna().empty):
            speech_text = "Round Rock Meter number: "+meter_number+" is not availble"
        else:
            DiffRatioPrevMonth=round((MonthlyReadingPrevMonth-MonthlyReadingPrev2Months)/MonthlyReadingPrev2Months*100,2)
            DiffRatioPrevYear=round((MonthlyReadingPrevMonth-MonthlyReadingPrevMonthYear)/MonthlyReadingPrevMonthYear*100,2)
            speech_text = "Round Rock Meter number: "+meter_number+" consumption from "+start_date+" to "+end_date+" is "+MonthlyReadingPrevMonth.to_string(index=False)+" cubes, which is a "+DiffRatioPrevMonth.to_string(index=False)+"% difference from the previous month, and a "+DiffRatioPrevYear.to_string(index=False)+"% difference from the previous year"
        return question(speech_text)

@ask.intent('MeterStatusSpecificDate')
def meter_status_specific_date(meter_number,usage_date):
    #meter_number = '5478'
    #usage_date = '2016-01-01'
    #What was my usage on {usage_date}
    #What was meter {meter_number} usage on {usage_date}
    if (meter_number is None) or (meter_number=='?'):
        meter_number = session.attributes.get(SESSION_METER)
    if (usage_date is None) or (usage_date=='?'):
        usage_date = session.attributes.get(SESSION_USAGE_DATE)
    if (meter_number is None) or (meter_number=='?'):
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
        return question(question_text).reprompt(reprompt_text)
    #Somtime we can get a month-year and that's all - need to dechiper it as a monthly region
    #usage_date = '2018-11'
    if len(usage_date)==7:
        start_date_spcf_month = usage_date+'-01'
        end_date_spcf_month = datetime.datetime.strptime(usage_date,"%Y-%m") + dateutil.relativedelta.relativedelta(months=1)
        end_date_spcf_month = end_date_spcf_month.strftime("%Y-%m-%d")
        if meter_number is not None:
            #Connect to data base
            #PyODBC
            #connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
            #                      "Server=192.168.33.85;"
            #                      "Database=RoundRockTX;"
            #                      "UID=Analytics;"
            #                      "PWD=hweg%^90Fdd;")
            #TurboODBC
            connection = connect(dsn='40.74.254.5',uid='Analytics',pwd='hweg%^90Fdd')
            sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval<'"+str(end_date_spcf_month)+"' and ConsInterval>='"+str(start_date_spcf_month)+"'"
            MonthlyReadingInputDate = pd.read_sql(sql,connection)
            connection.close()
            if (MonthlyReadingInputDate.dropna().empty):
                speech_text = "Round Rock Meter number: "+meter_number+" usage from "+start_date_spcf_month+" to "+end_date_spcf_month+" is not availble"
            else:
                MonthlyReadingInputDate = round(MonthlyReadingInputDate.Cons,2)
                speech_text = "Round Rock Meter number: "+meter_number+" usage from "+start_date_spcf_month+" to "+end_date_spcf_month+" is "+MonthlyReadingInputDate.to_string(index=False)+" cubes"
            return question(speech_text)
    #For a specific date
    if len(usage_date)==10:
        if meter_number is not None:
            #Connect to data base
            #PyODBC
            #connection = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
            #                      "Server=192.168.33.85;"
            #                      "Database=RoundRockTX;"
            #                      "UID=Analytics;"
            #                      "PWD=hweg%^90Fdd;")
            #TurboODBC
            connection = connect(dsn='40.74.254.5',uid='Analytics',pwd='hweg%^90Fdd')
            sql = "select Cons from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval='"+str(usage_date)+"'"
            DailyReadingInputDate = pd.read_sql(sql,connection)
            connection.close()
            if (DailyReadingInputDate.dropna().empty):
                speech_text = "Round Rock Meter number: "+meter_number+" usage on "+usage_date+" is not availble"
            else:
                DailyReadingInputDate = round(DailyReadingInputDate.Cons,2)
                speech_text = "Round Rock Meter number: "+meter_number+" usage on "+usage_date+" is "+DailyReadingInputDate.to_string(index=False)+" cubes"
            return question(speech_text)
    
@ask.intent('AMAZON.HelpIntent')
def help():
    meter_number = session.attributes.get(SESSION_METER)
    if (meter_number is None) or (meter_number=='?'):
        help_text = render_template('help')
        return question(help_text)
    if meter_number is not None:
        help_known_meter = render_template('help_known_meter', meter_number=meter_number)
        return question(help_known_meter)

@ask.intent('AMAZON.StopIntent')
def stop():
    bye_text = render_template('bye')
    return statement(bye_text)

@ask.intent('AMAZON.CancelIntent')
def cancel():
    cancel_text = render_template('cancel')
    return statement(cancel_text)

@ask.intent('AMAZON.FallbackIntent')
def fallback():
    fallback_text = render_template('fallback')
    return question(fallback_text)

@ask.session_ended
def session_ended():
    return "{}", 200

if __name__ == '__main__':
    if 'ASK_VERIFY_REQUESTS' in os.environ:
        verify = str(os.environ.get('ASK_VERIFY_REQUESTS', '')).lower()
        if verify == 'false':
            app.config['ASK_VERIFY_REQUESTS'] = False
    app.run(debug=True)