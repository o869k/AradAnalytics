from flask import Flask, render_template
from flask_ask import Ask, session, question, statement
import logging
import pandas as pd
import pyodbc
import datetime
import dateutil.relativedelta
import os
import requests

app = Flask(__name__)
ask = Ask(app, '/')
logging.getLogger("flask_ask").setLevel(logging.DEBUG)

#Date Inputs
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

first = today.replace(day=1)
lastMonth = first - datetime.timedelta(days=1)
start_date_last_month = lastMonth.strftime("%Y-%m-")+'01'
end_date_last_month = first.strftime("%Y-%m-%d")

#Entities
SESSION_ACCOUNT = "account_number"
SESSION_USAGE_DATE = "usage_date"

#Connect to server
#PyODBC
connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};"
                            "Server=40.74.254.5;"
                            "Database=RoundRockTX;"
                            "UID=Analytics;"
                            "PWD=hweg%^90Fdd;")
#TurboODBC
#connection = connect(dsn='AnalyticsResultsAzure',uid='Analytics',pwd='hweg%^90Fdd')
sql = "select distinct MeterCount,ConsumerID from Metercarddetails"
meters_list = pd.read_sql(sql,connection)
meters_list['ConsumerID'] = meters_list['ConsumerID'].str.replace('-','')
meters_list = meters_list.loc[~(pd.isnull(meters_list['ConsumerID']))]
meters_list['ConsumerID'] = meters_list['ConsumerID'].astype(str)

#Examples
#account_number = '0000004747'
#account_number = '0000999999'
#account_number = '0000041696'
#usage_date = '2016-01-01'
           
@ask.launch
def launch():
    welcome_sentence = render_template('welcome')
    session.attributes[SESSION_ACCOUNT] = '?'
    session.attributes[SESSION_USAGE_DATE] = yesterday.strftime("%Y-%m-%d") #by default mean yersteday
    return question(welcome_sentence)

@ask.intent('MyMeterIs', default={'account_number': None})
def my_meter_is(account_number):
    if account_number is not None:
        try:
            val = int(account_number)
        except ValueError:
            account_number = None
    if account_number is not None:
        if account_number in meters_list['ConsumerID'].unique():
            session.attributes[SESSION_ACCOUNT] = account_number
            question_text = render_template('known_account', account_number=str(int(account_number)))
            reprompt_text = render_template('known_account_reprompt')
        else:
            question_text = render_template('non_valid_account', account_number=str(int(account_number)))
            reprompt_text = render_template('non_valid_account_reprompt') 
    else:
        session.attributes[SESSION_ACCOUNT] = account_number
        question_text = render_template('unknown_account')
        reprompt_text = render_template('unknown_account_reprompt')
    return question(question_text).reprompt(reprompt_text)

@ask.intent('MeterStatusSpecificRange')
def meter_status_specific_range(start_date,end_date):
    account_number = session.attributes.get(SESSION_ACCOUNT)
    if (account_number is None) or (account_number=='?'):
        question_text = render_template('unknown_account')
        reprompt_text = render_template('unknown_account_reprompt')
        return question(question_text).reprompt(reprompt_text)
    if account_number is not None:
        usage = requests.get('https://totalconsumptionbetweendatesapp.azurewebsites.net/api/TotalConsumptionBetweenDates?', params={'code':'bQ0X/ZxRzoquK9rrYDt2GFRDtRG1DpjlbdYa7Y2Y9M2h5sE5VRZy9A==',
                                                                                                                        'AccountNo': account_number,
                                                                                                                        'DateFrom': start_date+'T00:00:00',
                                                                                                                        'DateTo': end_date+'T23:59:00'})
        usage = str(usage.content)
        usage = usage.split(':"')[1]
        usage = usage.split('"')[0]
        if (usage=='null'):
            speech_text = "Account number: "+str(int(account_number))+" consumption from "+start_date+" to "+end_date+" is not availble" 
        else:
            speech_text = "Account number: "+str(int(account_number))+" consumption from "+start_date+" to "+end_date+" is "+usage+" gallons"
        reprompt_text = render_template('next_step')
        return question(speech_text).reprompt(reprompt_text)

@ask.intent('MeterStatusSpecificDate')
def meter_status_specific_date(usage_date):
    account_number = session.attributes.get(SESSION_ACCOUNT)
    if (account_number is None) or (account_number=='?'):
        question_text = render_template('unknown_account')
        reprompt_text = render_template('unknown_account_reprompt')
        return question(question_text).reprompt(reprompt_text)
    if (usage_date is None) or (usage_date=='?'):
        usage_date = session.attributes.get(SESSION_USAGE_DATE)
    #Somtime we can get a month-year and that's all - need to dechiper it as a monthly region
    meters = list(meters_list.MeterCount[meters_list['ConsumerID']==account_number])
    meters = [str(meter) for meter in meters]
    if len(usage_date)==7:
        start_date_spcf_month = usage_date+'-01'
        end_date_spcf_month = datetime.datetime.strptime(usage_date,"%Y-%m") + dateutil.relativedelta.relativedelta(months=1)
        end_date_spcf_month = end_date_spcf_month.strftime("%Y-%m-%d")
        if (usage_date == end_date_last_month[0:7]):
            #Last cycle situation
           start_date_spcf_month = start_date_last_month
           end_date_spcf_month = end_date_last_month
        if account_number is not None:
            sql = "select sum(Cons) as Cons from dbo.MetersConsDaily where MeterCount in ('"+"','".join(meters)+"') and ConsValid='1' and ConsInterval<'"+str(end_date_spcf_month)+"' and ConsInterval>='"+str(start_date_spcf_month)+"'"
            MonthlyReadingInputDate = pd.read_sql(sql,connection)
            if (MonthlyReadingInputDate.dropna().empty):
                speech_text = "Account number: "+str(int(account_number))+" usage from "+start_date_spcf_month+" to "+end_date_spcf_month+" is not availble"
            else:
                MonthlyReadingInputDate = round(float(MonthlyReadingInputDate.Cons),2)
                speech_text = "Account number: "+str(int(account_number))+" usage from "+start_date_spcf_month+" to "+end_date_spcf_month+" is "+str(MonthlyReadingInputDate)+" gallons"
                #Check for anomaly in last month usage!
            reprompt_text = render_template('next_step')
            return question(speech_text).reprompt(reprompt_text)
    #For a specific date
    if len(usage_date)==10:
        if account_number is not None:
            sql = "select sum(Cons) as Cons from dbo.MetersConsDaily where MeterCount in ('"+"','".join(meters)+"') and ConsValid='1' and ConsInterval='"+str(usage_date)+"'"
            DailyReadingInputDate = pd.read_sql(sql,connection)
            if (DailyReadingInputDate.dropna().empty):
                speech_text = "Account number: "+str(int(account_number))+" usage on "+usage_date+" is not availble"
            else:
                DailyReadingInputDate = round(float(DailyReadingInputDate.Cons),2)
                speech_text = "Account number: "+str(int(account_number))+" usage on "+usage_date+" is "+str(DailyReadingInputDate)+" gallons"
            reprompt_text = render_template('next_step')
            return question(speech_text).reprompt(reprompt_text)
    #For any other date:
    if (len(usage_date)!=10 and len(usage_date)!=7):
        question_text = render_template('unknown_date')
        reprompt_text = render_template('unknown_date_reprompt')
        return question(question_text).reprompt(reprompt_text)

@ask.intent('WateringSpecificRange')
def watering_specific_date(start_date,end_date):
    account_number = session.attributes.get(SESSION_ACCOUNT)
    if (account_number is None) or (account_number=='?'):
        question_text = render_template('unknown_account')
        reprompt_text = render_template('unknown_account_reprompt')
        return question(question_text).reprompt(reprompt_text)
    if (start_date is None) or (start_date=='?'):
        question_text = render_template('unknown_date')
        reprompt_text = render_template('unknown_date_reprompt')
        return question(question_text).reprompt(reprompt_text)
    if (end_date is None) or (end_date=='?'):
        question_text = render_template('unknown_date')
        reprompt_text = render_template('unknown_date_reprompt')
        return question(question_text).reprompt(reprompt_text)    
    #Somtime we can get a month-year and that's all - need to dechiper it as a monthly region
    meters = list(meters_list.MeterCount[meters_list['ConsumerID']==account_number])
    meters = [str(meter) for meter in meters]
    if account_number is not None:
        connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};"
                            "Server=40.74.254.5;"
                            "Database=AnalyticsResults;"
                            "UID=Analytics;"
                            "PWD=hweg%^90Fdd;")
        sql = "select distinct WateringDate,WateringHour from WateringDetectionResults where MeterCount in ('"+"','".join(meters)+"') and WateringDate<'"+str(end_date)+"' and WateringDate>='"+str(start_date)+"'"
        WateringEvents = pd.read_sql(sql,connection)
        WateringEventsCount = len(WateringEvents)
        speech_text = "Account number: "+str(int(account_number))+" number of watering events from "+start_date+" to "+end_date+" is "+str(WateringEventsCount)
        reprompt_text = render_template('next_step')
        return question(speech_text).reprompt(reprompt_text)
    
@ask.intent('AMAZON.HelpIntent')
def help():
    account_number = session.attributes.get(SESSION_ACCOUNT)
    if (account_number is None) or (account_number=='?'):
        help_text = render_template('help')
        return question(help_text)
    if account_number is not None:
        help_known_account = render_template('help_known_account', account_number=str(int(account_number)))
        return question(help_known_account)

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