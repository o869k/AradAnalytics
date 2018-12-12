# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import pandas as pd
from turbodbc import connect
from rasa_core_sdk import Action
import datetime

logger = logging.getLogger(__name__)

class ActionMeter(Action):
    def name(self):
        # define the name of the action which can then be included in training stories
        return "action_meter"

    def run(self, dispatcher, tracker, domain):
        # what your action should do
        meter = str(tracker.get_slot('meter'))
        #start_date = str(tracker.get_slot('start_date'))
        #end_date = str(tracker.get_slot('end_date'))
        #meter = '50775'
        #start_date = '2018-12-01'
        #end_date = '2018-12-05'
        
        today = datetime.date.today()
        first = today.replace(day=1)
        lastMonth = first - datetime.timedelta(days=1)
        start_date = lastMonth.strftime("%Y-%m-")+'01'
        end_date = lastMonth.strftime("%Y-%m-%d")

        connection = connect(dsn='AradTechNew',uid='OriKronfeld',pwd='Basket76&Galil')
        sql = 'select MeterCount,tDate,LastReadTime,LastRead,MeterStatus,EstimatedCons from ori.DailyReadingsData where MeterCount='+meter
        data = pd.read_sql(sql,connection)
        connection.close()
        data = data[(data.tDate>=start_date) & (data.tDate<=end_date)]
        if data.EstimatedCons.size==0:
            dispatcher.utter_message("Meter: "+meter+" is not availble")
        else:
            dispatcher.utter_message("Meter: "+meter+" consumption from "+start_date+" to "+end_date+" is "+str(data.EstimatedCons.sum().round())+" cubes")
        return []
