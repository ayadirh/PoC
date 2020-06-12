import flask
import pandas as pd
from flask import request, jsonify, stream_with_context, Response, render_template
import time,datetime

app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Reading Events.csv from RetailRocket files
filepath= r"C:/Users/ayadi/Downloads/events.csv"
dataread = pd.read_csv(filepath, usecols = ['timestamp','visitorid','event','itemid', 'transactionid'], error_bad_lines = False)
print('--reading--', filepath, '| Empty:',dataread.empty)

def getData(index):
    return (dataread.iloc[[index]].to_json(orient='records'))

@app.route('/', methods=['GET'])
def home():
    return "<h1>Weblogs API 1.0</h1><br><h4>Agrolytics</h4>"

print(getData(1))

@app.route('/events')
def getWebLogs():
    def generate():
        count = 0;
        while True:
            yield getData(count)
            time.sleep(.1)
            count +=1
    return Response(generate(), mimetype='text/json')

app.run()