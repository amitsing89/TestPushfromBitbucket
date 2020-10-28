import speech_recognition as sr
import pyttsx

engine = pyttsx.init()
r = sr.Recognizer()
mic_list = sr.Microphone.list_microphone_names()
mic_name = "Ensoniq AudioPCI: ES1371 DAC1 (hw:0,1)"
for i, microphone_name in enumerate(mic_list):
    if microphone_name == mic_name:
        device_id = i
        print i, microphone_name
# generate a list of all audio cards/microphones
mic_list = sr.Microphone.list_microphone_names()
with sr.Microphone() as source:
    # wait for a second to let the recognizer adjust the
    # energy threshold based on the surrounding noise level
    r.adjust_for_ambient_noise(source)
    print "Say Something"
    # listens for the user's input
    audio = r.listen(source)

    try:
        text = r.recognize_google(audio)
        print "you said: " + text
        # engine.say(text)
        # engine.runAndWait()

    except sr.UnknownValueError:
        print("Google Speech Recognition could not understand audio")

    except sr.RequestError as e:
        print "Could not request results from Google Speech Recognition service; {0}".format(e)
