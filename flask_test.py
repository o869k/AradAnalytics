from flask import Flask, render_template
from flask_ask import Ask, session, question, statement
import logging
import pandas as pd
from turbodbc import connect
import datetime
import dateutil.relativedelta
import os

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

#Entities
SESSION_METER = "meter_number"

@ask.launch
def launch():
    welcome_sentence = render_template('welcome')
    session.attributes[SESSION_METER] = '?'
    return question(welcome_sentence)

@ask.intent('MyMeterIs', default={'meter_number': None})
def my_meter_is(meter_number):
    #just checking if that's a real number
    if meter_number is not None:
        try:
            val = int(meter_number)
        except ValueError:
            meter_number = None
    if meter_number is not None:
        session.attributes[SESSION_METER] = meter_number
        question_text = render_template('known_meter', meter_number=meter_number)
        reprompt_text = render_template('known_meter_reprompt')
    else:
        session.attributes[SESSION_METER] = meter_number
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
    return question(question_text).reprompt(reprompt_text)

@ask.intent('MeterStatusPrevMonth')
def meter_status_prev_month(meter_number):
    #meter_number = '5478'
    if (meter_number is None) or (meter_number=='?'):
        meter_number = session.attributes.get(SESSION_METER)
    if (meter_number is None) or (meter_number=='?'):
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
        return question(question_text).reprompt(reprompt_text)
    if meter_number is not None:
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
    bye_text = render_template('bye')
    return statement(bye_text)

@ask.intent('ThanksIntent')
def thanks():
    thanks_text = render_template('thanks')
    return question(thanks_text)

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