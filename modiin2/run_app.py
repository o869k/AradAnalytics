from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

from flask import Flask, Response, Blueprint, request, jsonify
from rasa_core.channels.channel import UserMessage, OutputChannel
from rasa_core.channels.channel import InputChannel
from rasa_core.channels.channel import CollectingOutputChannel
import json	

logger = logging.getLogger(__name__)

class GoogleConnector(InputChannel):
    """A custom http input channel.

    This implementation is the basis for a custom implementation of a chat
    frontend. You can customize this to send messages to Rasa Core and
    retrieve responses from the agent."""

    @classmethod
    def name(cls):
        return "google_home"

    def blueprint(self, on_new_message):
    
        google_webhook = Blueprint('google_webhook', __name__)

        @google_webhook.route("/", methods=['GET'])
        def health():
            return jsonify({"status": "ok"})

        @google_webhook.route("/webhook", methods=['POST'])
        def receive():
            payload = json.loads(request.data)		
            sender_id = payload['user']['userId']
            intent = payload['inputs'][0]['intent'] 			
            text = payload['inputs'][0]['rawInputs'][0]['query'] 		
            if intent == 'actions.intent.MAIN':	
                message = "<speak>Hello! <break time=\"1\"/> Welcome to the Arad Modiin previous month meter usage detector. You can start by saying Hi!."			 
            else:
                out = CollectingOutputChannel()			
                on_new_message(UserMessage(text, out, sender_id))
                responses = [m["text"] for m in out.messages]
                message = responses[0]	
            r = json.dumps(
                {
                  "conversationToken": "{\"state\":null,\"data\":{}}",
                  "expectUserResponse": 'true',
                  "expectedInputs": [
                    {
                      "inputPrompt": {
                       "initialPrompts": [
                        {
                          "ssml": message
                        }
                      ]
                     },
                    "possibleIntents": [
                    {
                      "intent": "actions.intent.TEXT"
                    }
                   ]
                  }
                 ]
               })
            return r				
          		
        return google_webhook

from rasa_core.agent import Agent
from rasa_core.interpreter import RasaNLUInterpreter
##from custom import GoogleConnector
from rasa_core.utils import EndpointConfig

action_endpoint = EndpointConfig(url="http://localhost:5055/webhook")
nlu_interpreter = RasaNLUInterpreter('C:\\Arad/ChatBots/modiin2/models/current/nlu')
agent = Agent.load('C:\\Arad/ChatBots/modiin2/models/current/dialogue', interpreter = nlu_interpreter, action_endpoint=action_endpoint)

input_channel = GoogleConnector()
agent.handle_channels([input_channel], 5004, serve_forever=True)