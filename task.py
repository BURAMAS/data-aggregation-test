# For this task you will need to create a Python function that ingests some data detailing
# transaction and product information from a supermarket, before performing some aggregation
# across this data and returning some high level statistics.

# You have been provided the following data sources, which have the following schema:
# transaction_data.csv:
#   transaction_id - integer - details the basket which this item appeared in
#   item_name - string - name of the item (only one instance per transaction_id)
#   item_quantity - integer - quantity of given item in basket
# item_data.csv:
#   item_name - string - name of the item (corresponds to item_name in transaction_data.csv)
#   item_department - string - name of the department to which the item belongs
#   item_weight - string - weight of one unit of item with suffix of unit of measurement (assume to always be g)
#   item_cost - float - cost of one unit of item

# Your function should return the following information as a dictionary:
#   no_of_transactions - integer - total number of transactions
#   average_transaction_value - float - mean value spent per transaction (to 2dp)
#   average_transaction_weight - string - mean weight per transaction (to 2dp, with suffix of unit of measurement)

# Help/guidance:
# Your function will need to combine the information between the datasets in order to provide the desired
# output. Your function should work for any dataset supplied which follows the schema as defined above.
# For the data supplied the expected output should look as defined at the bottom of this script. You are
# permitted to use Python packages/libraries - you may find it easier to complete this task using Pandas.

# Please feel free to ask if you require any guidance without how to complete this task.

import pandas as pd 

def total_table_iterator(table_name, dict):

    desired_total = 0
    #row_name = str(table_name.iloc[:,-1:]).strip()

    for index, rows in table_name.iterrows():
        desired_total += (dict[rows.item_name] * rows.item_quantity)
    
    return desired_total




def store_stats(transaction_data_filepath, item_data_filepath): 

    transaction_data = pd.read_csv(transaction_data_filepath)
    item_data = pd.read_csv(item_data_filepath)

    #finding the highest transaction id as this will be the number of transactions
    no_of_transactions = transaction_data['transaction_id'].max()

    #variables to hold value and weight of transactions
    total_transaction_value = 0 
    total_transaction_weight = 0

    #dictionaries to hold price and weights of items
    item_prices = {}
    item_weights = {}

    #fill in prices dictionary
    for index, rows in item_data.iterrows():
        item_prices[rows.item_name] =  rows.item_cost

    
    total_transaction_value = total_table_iterator(transaction_data, item_prices)
    
    #divide the total trnascation value by number of transactions to get average
    average_transaction_value = (total_transaction_value /no_of_transactions)

    #fill in weight dictionary
    #covert weight to integer and remove the g from the end
    for index, rows in item_data.iterrows():
        item_weights[rows.item_name] = int(rows.item_weight[0:-1])
    

    total_transaction_weight  = total_table_iterator(transaction_data,item_weights)
    
    #calculate average transaction weight by dividing by total number of transactions
    average_transaction_weight = (total_transaction_weight/ no_of_transactions)

    #cretae the stat dictionary
    stats = {
    "no_of_transactions": int(no_of_transactions),
    "average_transaction_value": round(float(average_transaction_value),15),
    "average_transaction_weight":str(round(average_transaction_weight,2)) + 'g',
}


   
    return stats

#call function
actual_output = store_stats('transaction_data.csv','item_data.csv')

expected_output = {
    "no_of_transactions": 15,
    "average_transaction_value": 7.140000000000001,
    "average_transaction_weight": "1686.67g",
}


#check if the actual output is the same as expected output
print(actual_output)
assert expected_output == actual_output, 'something is not right'