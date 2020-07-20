from flask import *
# from OpenSSL import SSL
import pandas as pd
import os

# context = SSL.Context(SSL.SSLv23_METHOD)
cer = os.path.join(os.path.dirname(__file__), 'crt.pem')
key = os.path.join(os.path.dirname(__file__), 'key.pem')

app = Flask(__name__)

# dataset = tablib.Dataset()
dataset = pd.read_csv('/home/cloudera/Documents/CompleteDATA.csv')


# with open("/home/cloudera/Documents/CompleteDATA.csv") as f:
#     dataset.csv = f.read()


@app.route('/check/city')
def location():
    locations = dataset.loc[dataset.CS_SITE == 'BANGALORE']
    return render_template('view.html', tables=[locations.to_html(classes='location')],
                           titles=['na', 'Location surfers'])


@app.route('/check')
def hello_world():
    # CS_SITE
    # dataframe = dataset.df()
    call_count = dataset.loc[dataset.count_of_call >= 10]
    return render_template('view.html', tables=[call_count.to_html(classes='calls')], titles=['na', 'Calling surfers'])

    # return dataset.html
    # return 'Hello World!'


if __name__ == '__main__':
    context = (cer, key)
    app.run(host='0.0.0.0', port=5000, debug=True)
