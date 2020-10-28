from flask import *
import os
import pyttsx

# cer = os.path.join(os.path.dirname(__file__), 'crt.pem')
# key = os.path.join(os.path.dirname(__file__), 'key.pem')

app = Flask(__name__)


@app.route('/check')
def hello_world():
    engine = pyttsx.init()
    agent = request.headers.get('User-Agent')
    if 'mozilla' in agent.lower():
        engine.say(agent[0:len(agent)] + agent[55:63])
        # engine.say('my name is gummy hai')
        engine.runAndWait()
        return agent.lower()


if __name__ == '__main__':
    # context = (cer, key)
    app.run(host='0.0.0.0', port=5000, debug=True)
