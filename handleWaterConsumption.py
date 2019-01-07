"""
Lexbot Lambda handler for waterconsumption intent.
"""

#from urllib.requests import Request, urlopen
import pyodbc

def get_slots(intent_request):
    return intent_request['currentIntent']['slots']

def connect_to_db():
    try:
        conn = pyodbc.connect(
                DRIVER='{SQL SERVER}',
                SERVER='192.168.33.85',
                DATABASE='Modiin',
                UID='ardo',
                PWD='p#ssword1!',
                autocommit=True
            )
        cursor = conn.cursor()
        return cursor
    except:
        return "unable to connect to db"

def delegate(intent_request, session_attributes=None):
    slots_dict = {"customerid": slots}
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': get_slots(intent_request),
            "fulfillmentState": "Fulfilled",
            "message": {
                "contentType": "PlainText",
                "content": "message to user"
            }
        }
    }

def welcomeUser(intent_request):
    """
    Performs dialog management and fulfillment for welcoming new user.
    """
    return {
            'sessionAttributes': None,
            'dialogAction': {
                'type': 'ElicitIntent',
                'message': {
                    "contentType": "PlainText",
                    "content": "Hi! Welcome to Round Rock Water Utility. We are here to help look into your water consumption. Please enter your customer id as 'my meter is '"
                }
            }
        }

def inquireWaterConsumption(intent_request):
    """
    Performs dialog management and fulfillment for inquiring about water consumption.
    Beyond fulfillment, the implementation of this intent demonstrates the use of the elicitSlot dialog action
    in slot validation and re-prompting.
    """
    customer_id = get_slots(intent_request)['customerid']
    if customer_id is None:
        return {
            'sessionAttributes': None,
            'dialogAction': {
                'type': 'ElicitSlot',
                'intentName': intent_request['currentIntent']['name'],
                'slots': get_slots(intent_request),
                'slotToElicit': 'customerid',
                'message': {
                    "contentType":"PlainText",
                    "content": "Please enter your customer id again"
                }
            }
        }
    else:
        res = connect_to_db()
        return {
            'sessionAttributes': None,
            'dialogAction': {
                'type': 'Close',
                'fulfillmentState': 'Fulfilled',
                'message': {
                    "contentType": "PlainText",
                    "content": "Accessed db with following result {}".format(res)
                }
            }
        }
    #return delegate(intent_request)


def lambda_handler(event, context):
    """
    Called when the user specifies an intent for this bot
    """
    intent_name = event['currentIntent']['name']
    if intent_name == 'Greetings':
        return welcomeUser(event)
    
    if (intent_name == "waterConsumption") | (intent_name == 'MyMeterIs'):
        return inquireWaterConsumption(event)
    raise Exception("Intent with name " + intent_name + " not supported yet")
    
    """return {
            'sessionAttributes': None,
            'dialogAction': {
                'type': 'ConfirmIntent',
                'intentName': "waterConsumption",
                'slots': get_slots(event),
                'message': {
                    "contentType": "PlainText",
                    "content": "Please enter your customer id again"
                }
            }
        }
    """
    #return dispatch(event)

