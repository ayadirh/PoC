import pandas as pd
import math, random
#from random import randrange, sample


# Reading orders.csv from Instacart files
#orderslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/orders.csv"
# Reading products.csv from Instacart files
productslist = r"C:/Users/ayadi/Downloads/4931_7487_bundle_archive/products.csv"

#orderread = pd.read_csv(orderslist, usecols = ['order_id','user_id','eval_set','order_number','order_dow','order_hour_of_day','days_since_prior_order' ], error_bad_lines = False)
productread = pd.read_csv(productslist, usecols = ['product_id','product_name','aisle_id','department_id'], error_bad_lines = False)
print('Length: ', len(productread.index))

ratinglist = [1,2,3,4,5]
ratings = []
for i in range(len(productread.index)):
    ratings.append(random.choice(ratinglist))

productread['ratings'] = ratings
productread.to_csv( 'ratedProducts.csv', sep=',', index=False)
