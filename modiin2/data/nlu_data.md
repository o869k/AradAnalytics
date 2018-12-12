<!--- Make sure to update this training data file with more training examples from https://forum.rasa.com/t/rasa-starter-pack/704 --> 

## intent:goodbye <!--- The label of the intent --> 
- Bye 			<!--- Training examples for intent 'bye'--> 
- Goodbye
- See you later
- Bye bot
- Goodbye friend
- bye
- bye for now
- catch you later
- gotta go
- See you
- goodnight
- have a nice day
- i'm off
- see you later alligator
- we'll speak soon

## intent:greet
- Hi
- Hey
- Hi bot
- Hey bot
- Hello
- Good morning
- hi again
- hi folks
- hi Mister
- hi pal!
- hi there
- greetings
- hello everybody
- hello is anybody there
- hello robot

## intent:thanks
- Thanks
- Thank you
- Thank you so much
- Thanks bot
- Thanks for that
- cheers
- cheers bro
- ok thanks!
- perfect thank you
- thanks a bunch for everything
- thanks for the help
- thanks a lot
- amazing, thanks
- cool, thanks
- cool thank you

## intent:affirm
- yes
- yes sure
- absolutely
- for sure
- yes yes yes
- definitely

## intent:name
- My name is [Juste](name)  <!--- Square brackets contain the value of entity while the text in parentheses is a a label of the entity --> 
- I am [Josh](name)
- I'm [Lucy](name)
- People call me [Greg](name)
- Usually people call me [Amy](name)
- My name is [John](name)
- You can call me [Sam](name)
- Please call me [Linda](name)
- Name name is [Tom](name)
- I am [Richard](name)
- I'm [Tracy](name)
- Call me [Sally](name)
- I am [Philipp](name)
- I am [Charlie](name)

## intent:meter_usage
- Can you tell me my meter count?
- I would like to hear my meter usage
- Tell me my meter
- A meter usage please
- Tell me my meter count please
- I would like to hear my meter count
- I would like to hear my meter usage, please
- Can you tell my meter usage?
- Please tell me my meter count
- I need to hear my meter count

## intent:meter_count
- My meter is [50775](meter)  <!--- Square brackets contain the value of entity while the text in parentheses is a a label of the entity --> 
- Meter [50776](meter)
- I use meter number [50778](meter)
- It's [11111](meter)
- Usually people call me [22222](meter)
- My meter is [33333](meter)
- You can use meter [44444](meter)
- Please check [66666](meter)
- Meter meter is [77777](meter)
- My meter is [88888](meter)
- Meter [99999](meter)
- Check number [00000](meter)
- Meter count is [12345](meter)
- Meter is [65432](meter)