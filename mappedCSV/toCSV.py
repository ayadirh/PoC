import pandas as pd
import time,datetime, math, random

# Reading Events.csv from RetailRocket files
filepath= r"C:/Users/ayadi/Downloads/events.csv"
# Reading products.csv from Instacart files
productslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/products.csv"
#Reading orders.csv from Instacart files to get user_id
orderslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/orders.csv"

orderread = pd.read_csv(orderslist, usecols = ['user_id'], error_bad_lines = False)
productread = pd.read_csv(productslist, usecols = ['product_id'], error_bad_lines = False)
dataread = pd.read_csv(filepath, usecols = ['timestamp','visitorid','event','itemid', 'transactionid'], error_bad_lines = False)

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

def updateData(index):
    dataread.at[index,'itemid'] = lookup_dictionary.get(dataread.iloc[[index]]['itemid'].item())
    dataread.at[index, 'visitorid'] = user_lookup_dictionary.get(dataread.iloc[[index]]['visitorid'].item())

N = 100000
for i in range(N):
    updateData(i)
dataread.iloc[:N, :].to_csv('mappedData.csv', sep=',', index=False)
print('completed')