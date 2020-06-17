import pandas as pd
import math, random
#from random import randrange, sample


# Reading orders.csv from Instacart files
orderslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/orders.csv"

orderread = pd.read_csv(orderslist, usecols = ['order_id','user_id','eval_set','order_number','order_dow','order_hour_of_day','days_since_prior_order' ], error_bad_lines = False)
print('Length: ', len(orderread.index))

ratinglist = [1,2,3,4,5]
ratings = []
for i in range(len(orderread.index)):
    ratings.append(random.choice(ratinglist))

orderread['ratings'] = ratings
orderread.to_csv( 'ratedOrders.csv', sep=',', index=False)
