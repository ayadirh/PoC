import flask
import pandas as pd
from flask import request, jsonify, stream_with_context, Response, render_template
import time,datetime
import math,json, random
#from random import randrange, sample

app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Reading Events.csv from RetailRocket files
filepath= r"C:/Users/ayadi/Downloads/events.csv"
# Reading products.csv from Instacart files
productslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/products.csv"
#Reading orders.csv from Instacart files to get user_id
orderslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/orders.csv"

orderread = pd.read_csv(orderslist, usecols = ['user_id'], error_bad_lines = False)
productread = pd.read_csv(productslist, usecols = ['product_id'], error_bad_lines = False)
dataread = pd.read_csv(filepath, usecols = ['timestamp','visitorid','event','itemid', 'transactionid'], error_bad_lines = False)
#print('--reading--', filepath, '| Empty:',dataread.empty)

#sorting the dataread by timestamp
dataread = dataread.sort_values(['timestamp']).reset_index(drop=True)

#creating a look up table for itemid->productid
uniqueItems = dataread.itemid.unique()
product_id_list = productread['product_id'].tolist()
mappedProduct = []
for i in uniqueItems:
    mappedProduct.append(random.choice(product_id_list))
lookup_dictionary = dict(zip(uniqueItems, mappedProduct))

#creating a look up table for visitorid->userid
uniqueVisitors = dataread.visitorid.unique().tolist()
uniqueUsers = orderread.user_id.unique()
mappedUser = []
for i in uniqueVisitors:
    mappedUser.append(random.choice(uniqueUsers))
user_lookup_dictionary =  dict(zip(uniqueVisitors,mappedUser))

#testing if none exists while mapping, False expected.
print('####')
print('1. None in MappedProduct?', None in mappedProduct)
print('2. None in mappedUser?', None in mappedUser)
print('####')

def getData(index):
    dataread.at[index,'itemid'] = lookup_dictionary.get(dataread.iloc[[index]]['itemid'].item())
    dataread.at[index, 'visitorid'] = user_lookup_dictionary.get(dataread.iloc[[index]]['visitorid'].item())
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