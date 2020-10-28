# import pyttsx
#
#
# def welcome_Message(name, message_val):
#     engine = pyttsx.init()
#     voices = engine.getProperty('voices')
#     # engine.setProperty('voice', voices[1].id)
#     # engine.say("MY NAME IS AMIT")
#     # engine.runAndWait()
#     for voice in voices:
#         engine.setProperty('voice', voice.id)
#         print voice.id
#         engine.say('The quick brown fox jumped over the lazy dog.')
#     engine.runAndWait()
#     # saying = {1: "Hello {0} Welcome to Bt Ecoworld Office".format(name), 2: "Welcome Back to India",
#     #           3: "How's your day"}
#     # engine.say(saying[message_val])
#     # engine.runAndWait()
#
#
# welcome_Message("AMIT", 1)
import pyttsx

engine = pyttsx.init()
voices = engine.getProperty('voices')
# print voices[1]
for i in range(len(voices)):
    print voices[i]
    # x = voices[i].split('gender')
    # print x[1]
# for voice in voices:
#     print voices
#     engine.setProperty('voice', voice.id)
#     engine.setProperty('age',25)
#     # print voice.id
#     engine.say('Hello')
# engine.runAndWait()
