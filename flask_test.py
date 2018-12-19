from flask import Flask, json, render_template
from flask_ask import Ask, request, session, question, statement
import logging
import pandas as pd
import numpy as np
from turbodbc import connect
import datetime
import dateutil.relativedelta
from six.moves.urllib.request import urlopen
from six.moves.urllib.parse import urlencode
import os
import math
import re

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
SESSION_DATE = "date"

@ask.launch
def launch():
    welcome_sentence = render_template('welcome')
    return question(welcome_sentence)

@ask.intent('MyMeterIs', mapping={'meter_number': 'meter_number'})
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
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
    return question(question_text).reprompt(reprompt_text)

@ask.intent('MeterStatusPrevMonth')
def meter_status_prev_month():
    #meter_number = '5478'
    meter_number = session.attributes.get(SESSION_METER)
    if meter_number is None:
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
        question(question_text).reprompt(reprompt_text)
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
        return question(speech_text).reprompt(speech_text)

@ask.intent('MeterUsageAtDate',mapping={'date':'date'})
def meter_usage_at_date(date):
    #meter_number = '5478'
    #date = '2016-11-11'
    meter_number = session.attributes.get(SESSION_METER)
    date = session.attributes.get(SESSION_DATE)
    if meter_number is None:
        question_text = render_template('unknown_meter')
        reprompt_text = render_template('unknown_meter_reprompt')
        question(question_text).reprompt(reprompt_text)
    if date is None:
        question_text = render_template('unknown_date')
        reprompt_text = render_template('unknown_date_reprompt')
        question(question_text).reprompt(reprompt_text)
    if (meter_number is not None) and (date is not None):
        connection = connect(dsn='AradRoundRock',uid='OriKronfeld',pwd='Basket76&Galil')
        sql = "select sum(Cons) from dbo.MetersConsDaily where MeterCount='"+str(meter_number)+"' and ConsValid='1' and ConsInterval='"+str(date)+"'"
        ReadingAtDate = pd.read_sql(sql,connection)
        connection.close()
        if (ReadingAtDate.dropna().empty):
            speech_text = "Round Rock Meter number: "+meter_number+" is not availble"
        else:
            speech_text = "Round Rock Meter number: "+str(meter_number)+" consumption on "+date+" is "+ReadingAtDate.to_string(index=False)+" cubes"
        return question(speech_text).reprompt(speech_text)

@app.template_filter()
def humanize_date(dt):
    # http://stackoverflow.com/a/20007730/1163855
    ordinal = lambda n: "%d%s" % (n,"tsnrhtdd"[(n/10%10!=1)*(n%10<4)*n%10::4])
    month_and_day_of_week = dt.strftime('%A %B')
    day_of_month = ordinal(dt.day)
    year = dt.year if dt.year != datetime.datetime.now().year else ""
    formatted_date = "{} {} {}".format(month_and_day_of_week, day_of_month, year)
    formatted_date = re.sub('\s+', ' ', formatted_date)
    return formatted_date

@ask.intent('AMAZON.HelpIntent')
def help():
    meter_number = session.attributes.get(SESSION_METER)
    if meter_number is None:
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

@ask.intent('AMAZON.FallbackIntent')
def fallback():
    fallback_text = render_template('fallback')
    return question(fallback_text).reprompt(fallback_text)

@ask.session_ended
def session_ended():
    return "{}", 200

if __name__ == '__main__':
    if 'ASK_VERIFY_REQUESTS' in os.environ:
        verify = str(os.environ.get('ASK_VERIFY_REQUESTS', '')).lower()
        if verify == 'false':
            app.config['ASK_VERIFY_REQUESTS'] = False
    app.run(debug=True)