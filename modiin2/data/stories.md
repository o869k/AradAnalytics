## story_greet <!--- The name of the story. It is not mandatory, but useful for debugging. --> 
* greet <!--- User input expressed as intent. In this case it represents users message 'Hello'. --> 
 - utter_name <!--- The response of the chatbot expressed as an action. In this case it represents chatbot's response 'Hello, how can I help?' --> 

## story_goodbye
* goodbye
 - utter_goodbye

## story_thanks
* thanks
 - utter_thanks

## story_name
* name{"name":"Sam"}
 - utter_greet

## story_meter_01
* meter_usage
 - utter_meter_count

## story_meter_03
* meter_count{"meter":"50775"}
 - action_meter

## story_meter_02
* greet
 - utter_name
* name{"name":"Lucy"} <!--- User response with an entity. In this case it represents user message 'My name is Lucy.' --> 
 - utter_greet
* meter_usage
 - utter_meter_count
* meter_count{"meter":"50776"}
 - action_meter
* thanks
 - utter_thanks
* goodbye
 - utter_goodbye 