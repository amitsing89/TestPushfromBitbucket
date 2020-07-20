# import PyPDF2
import pyttsx
# import time
# from nltk.tokenize import word_tokenize
# import nltk

# filePath = '/root/Downloads/test.pdf'
# pdfFileObj = open(filePath, 'rb')
# pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
# num_pages = pdfReader.numPages
# count = 0
# text = ""
# while count < num_pages:
#     pageObj = pdfReader.getPage(count)
#     count += 1
#     text += pageObj.extractText()
import time

engine = pyttsx.init()
saying = "Hello Gavin"
engine.say(saying)
engine.runAndWait()
# f = open('StringCheck.py','r')
# for i in f.readlines():
#     engine.say(str(i))
#     engine.runAndWait()
#     time.sleep(5)
# engine.say('You want to implement a queue that sorts items by a given priority and always returns the item with the highest priority on each pop operation.')
# print "PP",i
# engine.say('abhinav kunal is not going to london')
# engine.runAndWait()
# tokens = word_tokenize(text)
# tagged_val = nltk.pos_tag(tokens)
# for i in tagged_val:
#     print "PPP", i
#     engine.say(i[0])
#     engine.runAndWait()
#     time.sleep(1)
