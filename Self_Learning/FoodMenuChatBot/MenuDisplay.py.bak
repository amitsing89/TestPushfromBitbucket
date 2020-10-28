import json
import redis
import speech_recognition as sr

connection = redis.Redis(host="localhost", db=0, password='cloudera')


class MenuDisplay:
    def __init__(self, user):
        self.user = user

    @staticmethod
    def retryMenu():
        print "DONE"

    @staticmethod
    def menu():
        r = sr.Recognizer()
        mic_list = sr.Microphone.list_microphone_names()
        mic_name = "Ensoniq AudioPCI: ES1371 DAC1 (hw:0,1)"
        for i, microphone_name in enumerate(mic_list):
            if microphone_name == mic_name:
                device_id = i
                print i, microphone_name
        # generate a list of all audio cards/microphones
        mic_list = sr.Microphone.list_microphone_names()

        print "Say What would you like to have Veg or Non Veg"
        x = " "
        with sr.Microphone() as source:
            # wait for a second to let the recognizer adjust the
            # energy threshold based on the surrounding noise level
            r.adjust_for_ambient_noise(source)
            print "Say Something"
            # listens for the user's input
            audio = r.listen(source)
            text = r.recognize_google(audio)
            print "you said: " + text
            x = text
            if x == 'veg':
                value = json.loads(connection.hget("foodmenu", "veg"))
                print value
                print "Say what would you like to have"
                audio = r.listen(source)
                text = r.recognize_google(audio)
                print "you said: " + text
                if len(text) > 1:
                    print "Confirm order", value[text[0]], value[text[1]]
            elif 'non' in x:
                value = json.loads(connection.hget("foodmenu", "non-veg"))
                print "NV", value
                # print "Enter what would you like to have"
                print "Say what would you like to have"
                audio = r.listen(source)
                text = r.recognize_google(audio)
                print "you said: " + text
                # y = raw_input()
                # split_val = y.split(" ")
                if len(text) > 1:
                    print "Confirm order", value[text[0]], value[text[1]]
                else:
                    print "Confirm order", value[text[0]]


a = MenuDisplay("Amit")
a.menu()
