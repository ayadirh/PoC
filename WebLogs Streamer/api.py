import flask
import pandas as pd
from flask import request, jsonify, stream_with_context, Response, render_template
import time,datetime
#import math,json, random
from random import randrange

app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Reading Events.csv from RetailRocket files
filepath= r"C:/Users/ayadi/Downloads/events.csv"
productslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/products.csv"
productread = pd.read_csv(productslist, usecols = ['product_id'], error_bad_lines = False)
dataread = pd.read_csv(filepath, usecols = ['timestamp','visitorid','event','itemid', 'transactionid'], error_bad_lines = False)
print('--reading--', filepath, '| Empty:',dataread.empty)

'''
def getData(index):
    temp = json.loads(dataread.iloc[[index]].to_json(orient='records'))
    if dataread.iloc[[index]]["event"].item() == 'view':
        for i in temp:
           i["loggedIn"] = bool(random.getrandbits(1))
    else:
        for i in temp:
            i["loggedIn"] = True
    print(index, temp)
    return (json.dumps(temp))
'''

def getData(index):
    dataread.at[index,'itemid'] = productread.iloc[[randrange(len(productread.index))]]['product_id']
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
            time.sleep(.01)
            count +=1
    return Response(generate(), mimetype='text/json')

app.run()