import nltk
import pysolr
import PyPDF2
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

que_string = sys.argv[1]
solr = pysolr.Solr('http://localhost:8983/solr/newcontext', timeout=10)
filePath = '/root/Downloads/test.pdf'
pdfFileObj = open(filePath, 'rb')
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
num_pages = pdfReader.numPages
count = 0
text = ""
while count < num_pages:
    pageObj = pdfReader.getPage(count)
    count += 1
    text += pageObj.extractText()

tokens = word_tokenize(text)
tagged_val = nltk.pos_tag(tokens)
for i in tagged_val:
    try:
        solr.add([{i[1]: i[0]}])
    except Exception as e:
        print "Exception found while adding the solr tags", e, i[1], i[0]
