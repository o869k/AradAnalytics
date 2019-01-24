from flask import Flask, render_template
from flask_ask import Ask, session, question, statement
import logging
import pandas as pd
import numpy as np
import pyodbc
import datetime
import dateutil.relativedelta
import os
import requests
from datetime import timedelta
import warnings
warnings.filterwarnings("ignore")

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
#connection = connect(dsn='40.74.254.5',uid='Analytics',pwd='hweg%^90Fdd')
sql = "select distinct MeterCount,ConsumerID from Metercarddetails"
meters_list = pd.read_sql(sql,connection)
meters_list['ConsumerID'] = meters_list['ConsumerID'].str.replace('-','')
meters_list = meters_list.loc[~(pd.isnull(meters_list['ConsumerID']))]
meters_list['ConsumerID'] = meters_list['ConsumerID'].astype(str)
meters_list['ConsumerID_numeric'] = meters_list['ConsumerID'].astype('int').astype('str')

#Examples
#account_number = '0000004747'
#account_number = '0000999999'
#account_number = '0000041696'
#usage_date = '2016-01-01'

ratio_cycle = 0.5 #ratio of current cycle to previous cycle to find an abnormal cycle usgae
daily_burst = 5000 #amount of hourly burst 
hourly_burst = 1000 #amount of daily biurst
daily_missing_ratio = 5 #ratio in percent of missing daily sampels
hourly_missing_ratio = 10 #ratio in percent of missing hourly sampels
daily_anomaly_std = 2 #num of std for avg daily usgae
hourly_anomaly_std = 3 #num of std for avg hourly usgae
force_high_consump_cycle = 0

#Fucntions
def MetersConsEdit(tmp,time_type) :
    if (len(tmp.index)>1):
        extra_dates_idx = pd.DataFrame(pd.date_range(min(tmp.ConsInterval),max(tmp.ConsInterval),freq=time_type),columns=['ConsInterval'])
        tmp = pd.merge(tmp,extra_dates_idx,how='outer',on='ConsInterval',sort=True)
        tmp['avg'] = tmp.Cons/tmp.PeriodsSinceLastCons
        tmp['avg'] = tmp.avg.fillna(method='bfill')
        tmp.Cons[pd.isnull(tmp.PeriodsSinceLastCons) | (tmp.PeriodsSinceLastCons>1)] = tmp.avg[pd.isnull(tmp.PeriodsSinceLastCons) | (tmp.PeriodsSinceLastCons>1)]
        tmp['Cons'] = np.round(tmp['Cons'],2); tmp = tmp.drop('PeriodsSinceLastCons',1); tmp = tmp.drop('avg',1)
        tmp['year'] = tmp.ConsInterval.map(lambda x: x.year); tmp['month'] = tmp.ConsInterval.map(lambda x: x.month)
        tmp['wday'] = tmp.ConsInterval.map(lambda x: x.weekday()); tmp['day'] = tmp.ConsInterval.map(lambda x: x.day)
    return tmp

def calc_date_range_alarm(dataset,idx,value,time_units):    
    j = 0
    dataset['events'] = j
    for i in range(1, len(dataset)):
        if ((dataset.iloc[i][idx]==value) & (dataset.iloc[i-1][idx]!=value)):
           j+=1 
        if (dataset.iloc[i][idx]==value):
           dataset.iloc[i,dataset.columns.get_loc('events')] = j
    events_summ = dataset.groupby(['events']).agg({'Cons':'sum','ConsInterval': ['min', 'max']})
    events_summ = events_summ.drop(events_summ.index[[0]])
    events_summ['samples'] = 1+(events_summ['ConsInterval']['max']-events_summ['ConsInterval']['min']).astype('timedelta64['+time_units+']')
    return events_summ

def OutputSpeechText(speech_text,dataset,text1,text2):
    if (len(dataset.index)>0):
        if (len(dataset.index)==1):
            speech_text = speech_text+text1
        if (len(dataset.index)>1):
            speech_text = speech_text+text2
        if (len(dataset[dataset.samples==1].index)>0):
            speech_text = speech_text+",".join([str(date) for date in dataset.ConsInterval['min'][dataset.samples==1]])+" in the total amount of "+str(np.round(dataset.Cons[dataset.samples==1].sum()[0],2))+" gallons.\n"
        if (len(dataset[dataset.samples>1].index)>0):
            speech_text = speech_text+",".join([str(date) for date in 'from '+dataset.ConsInterval['min'][dataset.samples>1].astype(str)+' to '+dataset.ConsInterval['max'][dataset.samples>1].astype(str)])+" in the total amount of "+str(np.round(dataset.Cons[dataset.samples>1].sum()[0],2))+" gallons.\n"
    return speech_text

def RemoveUnwantedAlerts(dataset,dataset_to_remove):
    rm_rows = []
    if (len(dataset.index)>0):
        for i in list(dataset.index):
            tmp = np.where((dataset_to_remove.ConsInterval['min']>=dataset.ConsInterval['min'][i]) & (dataset_to_remove.ConsInterval['max']<=dataset.ConsInterval['max'][i]))[0]
            if len(tmp)>0:
                rm_rows.append(tmp[0])
    if (len((rm_rows))>0):
        dataset_to_remove = dataset_to_remove.drop(dataset_to_remove.index[rm_rows])
    return dataset_to_remove

def RemoveUnwantedAlerts2(dataset,dataset_to_remove):
    keep_rows = []
    if (len(dataset.index)>0):
        for i in list(dataset.index):
            tmp = np.where((dataset_to_remove.ConsInterval['min']>=dataset.ConsInterval['min'][i]) & (dataset_to_remove.ConsInterval['max']<=dataset.ConsInterval['max'][i]))[0]
            if len(tmp)>0:
                keep_rows.append(tmp[0])
    if (len((keep_rows))>0):
        dataset_to_remove = dataset_to_remove.iloc[keep_rows]
    else:
        dataset_to_remove = dataset_to_remove.iloc[0:0] #drop everything
    return dataset_to_remove
      
def run_anomalities_summary(meter,cycle,next_cycle):
    print("Meter: ",meter,"\n"); print("Cycle: ",cycle,"\n")
    speech_text = ""
    cycle = datetime.datetime.strptime(cycle,'%Y-%m-%d')
    next_cycle = datetime.datetime.strptime(next_cycle,'%Y-%m-%d')
    curr_cycle_month = cycle.month; curr_cycle_year = cycle.year
    prev_month_cycle_year = curr_cycle_year
    prev_cycle_year = curr_cycle_year-1; prev_month_cycle_month = curr_cycle_month-1
    if(prev_month_cycle_month==0): 
        prev_month_cycle_month=12; prev_month_cycle_year=prev_cycle_year
    high_consump_cycle = force_high_consump_cycle
    
    #Extract Data
    print("Reading data...\n")
    #PyODBC
    connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};"
                                "Server=40.74.254.5;"
                                "Database=RoundRockTX;"
                                "UID=Analytics;"
                                "PWD=hweg%^90Fdd;")
    #TurboODBC
    #connection = connect(dsn='AradRoundRockAzure',uid='Analytics',pwd='hweg%^90Fdd')
    sql = "select ConsInterval,Cons,PeriodsSinceLastCons,MeterCurrentState from dbo.MetersConsDaily where MeterCount='"+meter+"' and ConsInterval>='"+(cycle - timedelta(days=365)).strftime("%Y-%m-%d")+"' and ConsInterval<'"+(next_cycle + timedelta(days=30)).strftime("%Y-%m-%d")+"' and ConsValid='1'"
    MetersConsDaily = pd.read_sql(sql,connection)
    sql = "select ConsInterval,Cons,PeriodsSinceLastCons,MeterCurrentState from dbo.MetersConsHourly where MeterCount='"+meter+"' and ConsInterval>='"+(cycle - timedelta(days=365)).strftime("%Y-%m-%d")+"' and ConsInterval<'"+(next_cycle + timedelta(days=30)).strftime("%Y-%m-%d")+"' and ConsValid='1'"
    MetersConsHourly = pd.read_sql(sql,connection)
    #connection.close()
    
    #Fill in missing date and calcualte avg flux
    MetersConsDaily = MetersConsEdit(MetersConsDaily.copy(),"D")
    MetersConsHourly = MetersConsEdit(MetersConsHourly.copy(),"h")
    
    #Check for abnormal cycle
    if (len(MetersConsDaily.index)>1) & (len(MetersConsHourly.index)>1):        
        MetersConsMonthly = MetersConsDaily.groupby(['year','month']).Cons.agg({'Cons':'sum'})
        MetersConsHourly['hour'] = MetersConsHourly.ConsInterval.map(lambda x: x.hour)
        if (len(MetersConsMonthly.index)>1):
            curr_cycle_usage = MetersConsMonthly.query('year=='+str(curr_cycle_year)+' and month=='+str(curr_cycle_month)).Cons
            if len(curr_cycle_usage)>0:
                curr_cycle_usage = float(curr_cycle_usage)
            prev_cycle_usage = MetersConsMonthly.query('year=='+str(prev_month_cycle_year)+' and month=='+str(prev_month_cycle_month)).Cons
            if len(prev_cycle_usage)>0:
                prev_cycle_usage = float(prev_cycle_usage)
                if prev_cycle_usage>0:
                    if ((curr_cycle_usage-prev_cycle_usage)/prev_cycle_usage>ratio_cycle):
                        print("anomaly monthly usage prev month this year\n"); high_consump_cycle=1
            last_year_cycle_usage = MetersConsMonthly.query('year=='+str(prev_cycle_year)+' and month=='+str(curr_cycle_month)).Cons
            if len(last_year_cycle_usage)>0:
                last_year_cycle_usage = float(last_year_cycle_usage)
                if last_year_cycle_usage>0:
                    if ((curr_cycle_usage-last_year_cycle_usage)/last_year_cycle_usage>ratio_cycle):
                        print("anomaly monthly usage same month last year\n"); high_consump_cycle=1
            mean_cycle_usage_last_year = MetersConsMonthly.Cons.head(-2).mean()
            if not(np.isnan(mean_cycle_usage_last_year)):
                mean_cycle_usage_last_year = float(mean_cycle_usage_last_year)
                if mean_cycle_usage_last_year>0:
                    if ((curr_cycle_usage-mean_cycle_usage_last_year)/mean_cycle_usage_last_year>ratio_cycle):
                        print("anomaly monthly usage last year monthly avg\n"); high_consump_cycle=1
    else:
        speech_text = speech_text+"Not a valid input cycle and meter."
    
    #Order of importance
    events_of_int_leak_summ = None; events_of_int_leak_summ_hours = None; WateringDetection = None
    events_of_hourly_burst_summ = None; events_of_daily_burst_summ = None
    events_of_hourly_anomaly_summ = None; events_of_daily_anomaly_summ = None
    
    if (high_consump_cycle):
        #create avg and sd of different periods for anomaly consumption
        daily_day = MetersConsDaily[MetersConsDaily.Cons>0].groupby(['wday']).Cons.agg({'mean_cons_daily':'mean','std_cons_daily':'std'})
        daily_day = daily_day.fillna(0)
        daily_day.mean_cons_daily = daily_day.mean_cons_daily+daily_day.std_cons_daily*daily_anomaly_std
        hourly_hour = MetersConsHourly[MetersConsHourly.Cons>0].groupby(['hour']).Cons.agg({'mean_cons_hourly':'mean','std_cons_hourly':'std'})
        hourly_hour = hourly_hour.fillna(0)
        hourly_hour.mean_cons_hourly = hourly_hour.mean_cons_hourly+hourly_hour.std_cons_hourly*hourly_anomaly_std
        
        #A min usage per day/hour for the case of leakage
        min_avg_non_leak_hour = 0
        max_rows_to_look = np.where((MetersConsHourly.MeterCurrentState==8) & (MetersConsHourly.year==curr_cycle_year) & (MetersConsHourly.month==curr_cycle_month))
        if (np.size(max_rows_to_look)>0):
            max_rows_to_look = np.min(max_rows_to_look)-1
            last_data_before_leak = MetersConsHourly.loc[1:max_rows_to_look,]
            min_avg_non_leak_hour = min(last_data_before_leak.Cons[(last_data_before_leak.Cons!=0) & (last_data_before_leak.MeterCurrentState!=8)].tail(24))

        #For the rest of alerts concentrate on the cycle itself only
        MetersConsDaily = MetersConsDaily.loc[(MetersConsDaily.year==curr_cycle_year) & (MetersConsDaily.month==curr_cycle_month),]
        MetersConsHourly = MetersConsHourly.loc[(MetersConsHourly.year==curr_cycle_year) & (MetersConsHourly.month==curr_cycle_month),]
        
        #Calc days of missing samples
        missing_days = np.round(sum(np.isnan(MetersConsDaily.MeterCurrentState))/len(MetersConsDaily.index)*100,2)
        missing_hours = np.round(sum(np.isnan(MetersConsHourly.MeterCurrentState))/len(MetersConsHourly.index)*100,2)
        
        if (((missing_days<daily_missing_ratio) & (missing_hours<hourly_missing_ratio)) | (force_high_consump_cycle)):
            print("Performing analysis....\n")
            #Only if we have enough samples to look for - Calc days with watering
            connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};"
                                        "Server=40.74.254.5;"
                                        "Database=AnalyticsResults;"
                                        "UID=Analytics;"
                                        "PWD=hweg%^90Fdd;")
            #TurboODBC
            #connection = connect(dsn='AnalyticsResultsAzure',uid='Analytics',pwd='hweg%^90Fdd')
            sql = "select distinct WateringDate,WateringHour,AvgFlux from dbo.WateringDetectionResults where MeterCount='"+meter+"' and WateringDate>='"+cycle.strftime("%Y-%m-%d")+"' and WateringDate<'"+next_cycle.strftime("%Y-%m-%d")+"'"
            WateringDetection = pd.read_sql(sql,connection)
            #connection.close()
            
            WateringDetection.AvgFlux = WateringDetection.AvgFlux/3.8 #put back in gallons
            WateringDetection['ConsInterval'] = pd.to_datetime(WateringDetection.WateringDate) + pd.to_timedelta(WateringDetection.WateringHour,unit='h')
            WateringDetection['year'] = WateringDetection.ConsInterval.map(lambda x: x.year); WateringDetection['month'] = WateringDetection.ConsInterval.map(lambda x: x.month)
            WateringDetection['wday'] = WateringDetection.ConsInterval.map(lambda x: x.weekday()); WateringDetection['day'] = WateringDetection.ConsInterval.map(lambda x: x.day)
            WateringDetection['Cons'] = WateringDetection['AvgFlux']
            events_of_hourly_watering_summ = WateringDetection.groupby(['day']).agg({'Cons':'sum','ConsInterval': ['min', 'max']})
            events_of_hourly_watering_summ['samples'] = 1+(events_of_hourly_watering_summ['ConsInterval']['max']-events_of_hourly_watering_summ['ConsInterval']['min']).astype('timedelta64[h]')

            #Water bursts - short time very high usage
            MetersConsDaily['burst'] = (MetersConsDaily.Cons>daily_burst).map(lambda x: int(x))
            MetersConsHourly['burst'] = (MetersConsHourly.Cons>hourly_burst).map(lambda x: int(x))
            MetersConsHourly = MetersConsHourly.sort_values('ConsInterval'); MetersConsDaily = MetersConsDaily.sort_values('ConsInterval')
            events_of_hourly_burst_summ = calc_date_range_alarm(MetersConsHourly.copy(),8,1,"h")
            events_of_daily_burst_summ = calc_date_range_alarm(MetersConsDaily.copy(),7,1,"D")

            #Calc days of internal leakage alarm
            events_of_int_leak_summ = calc_date_range_alarm(MetersConsDaily.copy(),2,8,"D")
            events_of_int_leak_summ.Cons = min_avg_non_leak_hour*events_of_int_leak_summ.samples*24
            events_of_int_leak_summ_hours = calc_date_range_alarm(MetersConsHourly.copy(),2,8,"h")
            events_of_int_leak_summ_hours.Cons = min_avg_non_leak_hour*events_of_int_leak_summ_hours.samples
            
            #Unusual high daily consumptions & Unusual high hourly consumptions
            MetersConsDaily = pd.merge(MetersConsDaily,daily_day,how='outer',on='wday',sort=True,right_index=True)
            MetersConsHourly = pd.merge(MetersConsHourly,hourly_hour,how='outer',on='hour',sort=True,right_index=True)
            MetersConsHourly = MetersConsHourly.sort_values('ConsInterval'); MetersConsDaily = MetersConsDaily.sort_values('ConsInterval')
            MetersConsDaily['anomaly'] = (MetersConsDaily.Cons>MetersConsDaily.mean_cons_daily).map(lambda x: int(x))
            MetersConsHourly['anomaly'] = (MetersConsHourly.Cons>MetersConsHourly.mean_cons_hourly).map(lambda x: int(x))
            events_of_hourly_anomaly_summ = calc_date_range_alarm(MetersConsHourly.copy(),11,1,"h")
            events_of_daily_anomaly_summ = calc_date_range_alarm(MetersConsDaily.copy(),10,1,"D")
                
            #As a post process remove duplicate warnings (those that can be explained by previous alerts by order)
            events_of_daily_burst_summ = RemoveUnwantedAlerts(events_of_int_leak_summ,events_of_daily_burst_summ)
            events_of_daily_anomaly_summ = RemoveUnwantedAlerts(events_of_int_leak_summ,events_of_daily_anomaly_summ)
            events_of_hourly_burst_summ = RemoveUnwantedAlerts(events_of_int_leak_summ_hours,events_of_hourly_burst_summ)
            events_of_hourly_anomaly_summ = RemoveUnwantedAlerts(events_of_int_leak_summ_hours,events_of_hourly_anomaly_summ)
            events_of_hourly_burst_summ = RemoveUnwantedAlerts(events_of_hourly_watering_summ,events_of_hourly_burst_summ)
            events_of_hourly_anomaly_summ = RemoveUnwantedAlerts(events_of_hourly_watering_summ,events_of_hourly_anomaly_summ)
            events_of_hourly_anomaly_summ = RemoveUnwantedAlerts(events_of_hourly_burst_summ,events_of_hourly_anomaly_summ)
            events_of_hourly_burst_summ = RemoveUnwantedAlerts(events_of_daily_burst_summ,events_of_daily_anomaly_summ)
        
            #The hourly abnormal consumptions should onyl be part of a whole daily abnoraml consumption
            events_of_hourly_anomaly_summ = RemoveUnwantedAlerts2(events_of_daily_anomaly_summ,events_of_hourly_anomaly_summ)
            events_of_hourly_burst_summ = RemoveUnwantedAlerts2(events_of_daily_anomaly_summ,events_of_hourly_burst_summ) 
            
            #Arrenage output speech sentences
            speech_text = OutputSpeechText(speech_text,events_of_int_leak_summ_hours,"There was leakage anomality on the following occasion: ","There were leakage anomalities on the following occasions: ")
            speech_text = OutputSpeechText(speech_text,events_of_hourly_watering_summ,"There was watering pattern on the following occasion: ","There were watering patterns on the following occasions: ")
            speech_text = OutputSpeechText(speech_text,events_of_hourly_burst_summ,"There was water burst on the following occasion: ","There were water bursts on the following occasions: ")
            speech_text = OutputSpeechText(speech_text,events_of_hourly_anomaly_summ,"There was additional unknown anomality on the following occasion: ","There were additional unknown anomalities on the following occasions: ")

        else:
            speech_text = speech_text+"Not enough data to perform analysis."
    else:
        speech_text = speech_text+"Not an abnormal cycle."
    
    return speech_text
        
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
        if account_number in meters_list['ConsumerID'].unique() or account_number in meters_list['ConsumerID_numeric'].unique():
            if account_number in meters_list['ConsumerID_numeric'].unique():
                #The account input in numeric form
                account_number = str(meters_list.ConsumerID[meters_list['ConsumerID_numeric']==account_number][0])
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

@ask.intent('WhyBillHigh')
def why_bill_high():
    account_number = session.attributes.get(SESSION_ACCOUNT)
    if (account_number is None) or (account_number=='?'):
        question_text = render_template('unknown_account')
        reprompt_text = render_template('unknown_account_reprompt')
        return question(question_text).reprompt(reprompt_text)
    meters = list(meters_list.MeterCount[meters_list['ConsumerID']==account_number])
    meters = [str(meter) for meter in meters]
    speech_text = run_anomalities_summary(meters[0],start_date_last_month,end_date_last_month)  #let's assume only 1 meter at this stage (but perhaps all of account meter's anomalities should be run in parallel or summerized per account)
    reprompt_text = render_template('next_step')
    return question(speech_text).reprompt(reprompt_text)
        
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
            #Add comparison to a single day avg usage of this period to the avg of this meter and comparison to the avg of such users (residents count wise)
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
                #Check for anomaly in last month usage vs. the avg of this meter
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
                #Check anomaly for this date vs. the avg usage of this meter
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