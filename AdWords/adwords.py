
import pandas as pd # to read in data                                            #abalasu4
import numpy as np # to perform mathematcial operations


# reading in the bidders dataset
bidder = pd.read_csv('bidder_dataset.csv',index_col=None)
# reading in the queries dataset
query = pd.read_csv('queries.txt',header = None,names = ['query'])

# creating a dictionary for advertisers and their budgets
budgets = bidder.dropna(subset = ['Budget'])
budgets_dict = budgets.set_index('Advertiser')['Budget'].to_dict()

# summing the total budgets of all advertisers
budgetsum = sum(budgets_dict.values())

# creating a dictionary of queries which contains the advertisers for each query
# here the dictionary is sorted by ascending bid value for greedy algorithm
queries_dict = {}
for q in query['query']:
    if q not in queries_dict.keys():
        queries_dict[q] = bidder.loc[bidder.Keyword == q].sort_values(by = 'Bid Value',ascending = False).values

# Greedy algorithm
def greedy():
    total = 0
    for i in range(100): # performing 100 permutations
        revenue = 0
        sample = query['query'].sample(frac = 1).values # generating random samples
        budgets = budgets_dict.copy() # creating a copy of budgets to use
        for q in sample:
            for b in queries_dict[q]:
                if budgets[b[0]]>=b[2]: # checking if the bid value is below the budget amount remaining
                    budgets[b[0]] -= b[2]
                    revenue += b[2]
                    break

        total += revenue
    return total/100 #returning the mean of 100 permuations


def mssv():
    # creating a queries dictionary without the sorting
    queries_dict = {}


    for q in query['query']:
        if q not in queries_dict.keys():
            queries_dict[q]=bidder.loc[bidder.Keyword == q].values

    # implementing mssv algorithm
    total = 0
    for i in range(100): # performing 100 permutations
        revenue=0
        sample  = query['query'].sample(frac=1).values # generating random samples
        spent_dict = dict.fromkeys(budgets_dict,0) # creating a dictionary for advertisers and 0 spent values

        for q in sample:
            bid, advertiser,scaledbid = 0,0,0 # setting scaled bid to 0

            for b in queries_dict[q]:
                x = spent_dict[b[0]]/budgets_dict[b[0]] # calculating the Xu - fraction of advertisers budget that has been spent
                scaled = (b[2] * (1- np.exp(x-1))) # calculating the scaled bid for each advertiser
                if (scaled>scaledbid) and ((spent_dict[b[0]]+b[2]) <= budgets_dict[b[0]]): # calculating if the scaled bid is higher or lower than the budget and spent amount
                    scaledbid = scaled
                    bid = b[2]
                    advertiser = b[0]
            spent_dict[advertiser] += bid
            revenue += bid
        total += revenue
    return total/100 #returning the mean of 100 permuations


# Implementing Balance algorithm
def balance():
    total = 0

    for i in range(100):
        budgets = budgets_dict.copy() # creating a copy of budgets dictionary
        revenue = 0
        sample = query['query'].sample(frac = 1).values
        for q in sample:
            balance,bid,advertiser = 0,0,0 # setting the inital balance to 0

            for b in queries_dict[q]:
                if balance < budgets[b[0]] and budgets[b[0]] >=b[2]: # calculating if the balance is lower than the budgets
                    advertiser = b[0]
                    bid = b[2]
                    balance = budgets[b[0]] # switching the balance to higheer values
            budgets[advertiser] -= bid
            revenue += bid
        total += revenue
    return(total/100) #returning the mean of 100 permuations






import sys # importing sys for argument values
import random #importing random for setting seed

# checking if the algorithm to be used is specified
if len(sys.argv)<2:
    print("Enter the algorithm to be used greedy,mssv or balance")
    exit()

#reading in the argument
method = sys.argv[1]
random.seed(0)

#calling greedy function
if method =='greedy':
    greedy_revenue=greedy()
    print('The revenue is',round(greedy_revenue,2),'and the competitive ratio is',round(greedy_revenue/budgetsum,2))
#calling mssv function
if method =='mssv':
    mssv_revenue = mssv()
    print('The revenue is',round(mssv_revenue,2),'and the competitive ratio is',round(mssv_revenue/budgetsum,2))

#calling balance function
if method =='balance':
    balance_revenue = balance()
    print('The revenue is',round(balance_revenue,2),'and the competitive ratio is',round(balance_revenue/budgetsum,2))





