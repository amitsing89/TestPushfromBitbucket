import nltk
nltk.download('all')

def format_sentence(sent):
    return ({word: True for word in nltk.word_tokenize(sent)})


print(format_sentence("The cat is very cute"))
