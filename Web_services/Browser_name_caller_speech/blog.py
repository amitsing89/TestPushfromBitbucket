from flask import *

SECRET_KEY = 'Chocolate chip cookies'
app = Flask(__name__)
# pulls in configurations by looking for UPPERCASE variables 
app.config.from_object(__name__)
