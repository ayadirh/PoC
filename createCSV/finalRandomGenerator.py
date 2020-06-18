import pandas as pd
import time,datetime, math, random
from random import randint

# Reading shortlistedProducts.csv from generate.py
productslist = r"C:/Users/ayadi/PycharmProjects/poc/shortlistedProducts.csv"
#Reading mappedData.csv from generate.py
mappedEventslist = r"C:/Users/ayadi/PycharmProjects/poc/mappedData.csv"
# Reading customer from generate.py
users = r"C:/Users/ayadi/PycharmProjects/poc/selectedUser.csv"
N = 5000
productread = pd.read_csv(productslist, usecols = ['product_id','product_name','aisle_id','department_id'], error_bad_lines = False)
dataread = pd.read_csv(mappedEventslist, usecols = ['timestamp','visitorid','event','itemid'], error_bad_lines = False)
userread = pd.read_csv(users, usecols = ['cust_id'], error_bad_lines = False)

ordercount=[]
reordercount=[]
for i in range(N):
    temp = randint(0, 200)
    ordercount.append(temp)
    reordercount.append(randint(0, temp))

mappedUser = []
for i in range(N):
    mappedUser.append(random.choice(userread.cust_id.unique().tolist()))

mappedProduct = []
for i in range(N):
    mappedProduct.append(random.choice(productread.product_id.unique().tolist()))


finalOutput = pd.DataFrame(mappedUser, columns = ["cust_id"])
finalOutput["product_id"] = mappedProduct
finalOutput["order_count"] = ordercount
finalOutput["reordercount"] = reordercount

finalOutput.to_csv('final.csv', index=False)
'''
count = dataread.groupby(['visitorid','itemid'])['event'].value_counts().to_frame('count').reset_index()
print(count.loc[count['event'] == 'transaction'])
print('completed')
'''