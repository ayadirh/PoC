import pandas as pd
import time,datetime, math, random

# Reading products.csv from Instacart files
productslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/products.csv"
#Reading orders.csv from Instacart files to get user_id
orderslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/orders.csv"
# Reading Events.csv from RetailRocket files
filepath= r"C:/Users/ayadi/Downloads/events.csv"

rowCount = 18000

productread = pd.read_csv(productslist, usecols = ['product_id','product_name','aisle_id','department_id'], error_bad_lines = False)
productread = productread.iloc[:rowCount, :]
productread.to_csv('shortlistedProducts.csv', sep=',', index=False)

orderread = pd.read_csv(orderslist, usecols = ['user_id'], error_bad_lines = False)
uniqueUsers = orderread.user_id.unique().tolist()

dataread = pd.read_csv(filepath, usecols = ['timestamp','visitorid','event','itemid', 'transactionid'], error_bad_lines = False)
dataread = dataread.sort_values(['timestamp']).reset_index(drop=True)

uniqueVisitors = dataread.visitorid.unique().tolist()
uniqueVisitors.sort()
uniqueItems = dataread.itemid.unique().tolist()
uniqueItems.sort()
uniqueItems = uniqueItems[:rowCount]
uniqueVisitors = uniqueVisitors[:rowCount]

dataread = dataread[dataread['visitorid'].isin(uniqueVisitors)]
dataread = dataread[dataread['itemid'].isin(uniqueItems)].reset_index(drop=True)

mappedUser = []
mappedUser = random.sample(uniqueUsers, len(uniqueVisitors))
user_lookup_dictionary =  dict(zip(uniqueVisitors,mappedUser))
selectedUser = pd.DataFrame(mappedUser, columns = ["cust_id"])
selectedUser.to_csv('selectedUser.csv', index=False)

#creating a look up table for itemid->productid
product_id_list = productread.product_id.unique().tolist()
mappedProduct = []
mappedProduct=random.sample(product_id_list, len(uniqueItems))
lookup_dictionary = dict(zip(uniqueItems, mappedProduct))

def updateData(index):
    dataread.at[index,'itemid'] = lookup_dictionary.get(dataread.iloc[[index]]['itemid'].item())
    dataread.at[index, 'visitorid'] = user_lookup_dictionary.get(dataread.iloc[[index]]['visitorid'].item())

print('before \n', dataread)
N = len(dataread.index)
for i in range(N):
    updateData(i)
dataread.to_csv('mappedData.csv', sep=',', index=False)

print('completed')
print('CSV outputs: selectedUser.csv | mappedData.csv | shortlistedProducts.csv')