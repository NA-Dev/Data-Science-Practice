#!/usr/bin/env python
# coding: utf-8

# # SQL Query Practice
# 
# Data source: https://www.kaggle.com/ananta/credit-card-data

# ## Create and connect to a sqlite  database
# If we attempt to connect to a database that does not exist, it will be created. Always close your connection when you are finished.

# In[1]:


import sqlite3
from sqlite3 import Error

cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

print(sqlite3.version)

cur.close()
cnx.close()


# ## Import tables with pandas
# Using pandas dataframes prevents us from having to read the csv file and iterate through rows

# In[2]:


import pandas as pd

# Read csv to dataframes

df_cards = pd.read_csv('data/credit_card_data/CardBase.csv')
df_customers = pd.read_csv('data/credit_card_data/CustomerBase.csv')
df_transactions = pd.read_csv('data/credit_card_data/TransactionBase.csv')
df_frauds = pd.read_csv('data/credit_card_data/FraudBase.csv')


# View table info and first 5 rows of each table
newline = '\n'
print(df_cards.info())
display(df_cards.head())
print(newline)

print(df_customers.info())
display(df_customers.head())
print(newline)

print(df_transactions.info())
display(df_transactions.head())
print(newline)

print(df_frauds.info())
display(df_frauds.head())


# ## Drop and create sqlite tables
# 
# We are going to check if each table exists. If not, we will create it.
# The code below uses 'string formatting' to make string concatenation easier

# In[3]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Check if table exists in sqlite database

def table_exists(table_name):  
    #get the count of tables with the name
    result = cur.execute('SELECT count(name) FROM sqlite_master WHERE type="table" AND name="%s"' % table_name)

    #if the count is 1, then table exists
    if cur.fetchone()[0]==1 :
        print('%s exists' % table_name)
        return True
    
    print('%s does not exist' % table_name)
    return False
 
    
# DROP TABLE

def drop_table(table_name):
    print('drop %s' % table_name)
    sql = 'DROP TABLE %s' % table_name
    cur.execute(sql)
    
    
# Put table info into a list of tuples to keep code "DRY"

tables = [('cards', df_cards), ('customers', df_customers), ('transactions', df_transactions), ('frauds', df_frauds)]

for (table_name, df) in tables:
#     if table_exists(table_name):
#         drop_table(table_name)
      
    # CREATE table
    if not table_exists(table_name):
        print('CREATE %s' % table_name)   
        df.to_sql(name=table_name, con=cnx)
   
cur.close()   
cnx.close()


# ## Read the data
# 
# To understand what data we are working with, we will connect to a sqlite database, list the table names, list the column names, and preview data for each table.
# 
# We already saw this information in our dataframes above, but let's do it this way as as an exercise.

# In[4]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

newline_tab =  '\n  '

# Print table names

result = cur.execute('SELECT name FROM sqlite_master WHERE type="table"').fetchall()
table_names = list(zip(*result))[0]
print ('Tables:', newline_tab.join(table_names), sep = newline_tab)
print(newline)

# Print column names

for table_name in table_names:
    result = cur.execute('PRAGMA table_info("%s")' % table_name).fetchall()
    print ('Columns for %s:' % table_name)
    print(print(*result, sep=newline))
    print(newline)
    
    
    print ('Columns for %s:' % table_name)
    rows = cur.execute('SELECT * from %s LIMIT 5' % table_name)
    for row in rows:
        print(row)
    print(newline)
   
cur.close()  
cnx.close()


# ## Join Types
# 
# Outer joins return every row from at least one table.

# #### Left outer joins 
# All rows from the left table with any matches from the right table. If no matches, null is returned for each column of the right table.
# 
# This is the join type you will use 99% of the time. It is great for one-to-one and one-to-many relationships.

# In[5]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: Show me all of the data on customers and their credit cards

cur.execute('''SELECT * 
               FROM customers 
                 LEFT JOIN cards ON customers.CUST_ID = cards.CUST_ID''')
customer_cards = pd.DataFrame(cur.fetchall())
customer_cards.columns = [x[0] for x in cur.description]
display(customer_cards)

# Possible next steps: Group by customer and count number of cards


# ----- RIGHT JOIN (not supported by sqlite) -----

# Request: I want a list of transactions preceded by a flag if they are fraudulent
# Possible next steps: Group by credit card number and get the sum of fraudulent transaction amounts

# transactions_flagged = pd.read_sql('''SELECT frauds.Fraud_Flag, transactions.*
#                    FROM frauds 
#                      RIGHT JOIN transactions ON frauds.Transaction_ID = cards.Transaction_ID''',
#                con=cnx)
transactions_flagged = pd.read_sql(
    '''SELECT frauds.Fraud_Flag, transactions.*
       FROM transactions 
         LEFT JOIN frauds ON frauds.Transaction_ID = transactions.Transaction_ID''',
    con=cnx, index_col='index')
display(transactions_flagged)

   
cur.close()  
cnx.close()


# 
# #### Right outer joins
# All rows from the right table with any matches from the left table. If no matches, null is returned for each column of the left table.
# 
# These queries can always be written as the opposite left join. You might want to use a right join to makes your query easier to read and understand.

# In[6]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: I want a list of transactions preceded by a flag if they are fraudulent

# RIGHT JOIN below is not supported by sqlite

# transactions_flagged = pd.read_sql('''SELECT frauds.Fraud_Flag, transactions.*
#                    FROM frauds 
#                      RIGHT JOIN transactions ON frauds.Transaction_ID = cards.Transaction_ID''',
#                con=cnx)

# Equivalent supported query

transactions_flagged = pd.read_sql(
    '''SELECT frauds.Fraud_Flag, transactions.*
       FROM transactions 
         LEFT JOIN frauds ON frauds.Transaction_ID = transactions.Transaction_ID''',
    con=cnx, index_col='index')
display(transactions_flagged)


# Possible next steps: Get the total of fraudulent transaction amounts for every credit card

fraud_amounts_by_card = pd.read_sql(
    '''SELECT SUM(
         CASE WHEN frauds.Fraud_Flag = 1
           THEN transactions.Transaction_Value 
           ELSE 0 END
        ) AS Fraud_Total, transactions.Credit_Card_ID
       FROM transactions 
         LEFT JOIN frauds ON frauds.Transaction_ID = transactions.Transaction_ID
       GROUP BY transactions.Credit_Card_ID''',
   con=cnx)
display(fraud_amounts_by_card)

   
cur.close()  
cnx.close()


# #### Full outer joins
# All rows from the left table and the right table. If no matches, null is returned for each column of the left or right table as needed.
# 
# This is a great query to see if you have missing data so you can address the issue.

# In[7]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: Make sure we don't have any orphaned data between customers and credit cards

# OUTER JOIN is not supported by sqlite

# customers_cards = pd.read_sql(
#     '''SELECT *
#        FROM customers 
#          FULL OUTER JOIN cards ON customers.Customer_ID = cards.Customer_ID''',
#     con=cnx)

# Equivalent supported query

customers_cards = pd.read_sql(
    '''SELECT customers.*, cards.*
       FROM customers
         LEFT JOIN cards ON customers.Cust_ID = cards.Cust_ID
       UNION ALL
       SELECT customers.*, cards.* 
       FROM cards
         LEFT JOIN customers ON customers.Cust_ID = cards.Cust_ID
       WHERE customers.Cust_ID IS NULL''',
    con=cnx)
display(customers_cards)


# Possible next steps: Display only rows where data is missing

rows_missing_data = pd.read_sql(
    '''SELECT * FROM (
         SELECT customers.*, cards.*
         FROM customers
           LEFT JOIN cards ON customers.Cust_ID = cards.Cust_ID
         UNION ALL
         SELECT customers.*, cards.* 
         FROM cards
           LEFT JOIN customers ON customers.Cust_ID = cards.Cust_ID
         WHERE customers.Cust_ID IS NULL) as joined_table
       WHERE Cust_ID is NULL OR Card_Number is NULL''',
    con=cnx)
display(rows_missing_data)

# Note: If you only want rows with missing data from one table, a simple left join will suffice. 
# For example to show only customers with no card data

cur.close()  
cnx.close()


# #### Inner joins
# Show only rows with matches from both tables
# 
# Inner join can be replaced with a left join with where clauses to filter out rows without matches.

# In[8]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: Show me all fraudulent transactions

fraudulent_transactions = pd.read_sql(
    '''SELECT transactions.* 
       FROM transactions 
         INNER JOIN frauds ON transactions.Transaction_ID = frauds.Transaction_ID''',
    con=cnx)
display(fraudulent_transactions)

# Possible next steps: Show all the customers with fraudulent transactions

defrauded_customers = pd.read_sql(
    '''SELECT DISTINCT customers.*
       FROM transactions
         INNER JOIN frauds ON transactions.Transaction_ID = frauds.Transaction_ID
         INNER JOIN cards ON cards.Card_Number = transactions.Credit_Card_ID
         INNER JOIN customers ON customers.Cust_ID = cards.Cust_ID''',
    con=cnx)
display(defrauded_customers)  

   
cur.close()  
cnx.close()


# In[9]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: Show me all fraudulent transactions

fraudulent_transactions = pd.read_sql(
    '''SELECT transactions.* 
       FROM transactions 
         INNER JOIN frauds ON transactions.Transaction_ID = frauds.Transaction_ID''',
    con=cnx)
display(fraudulent_transactions)

# Possible next steps: Show all the customers with fraudulent transactions

defrauded_customers = pd.read_sql(
    '''SELECT DISTINCT customers.*
       FROM transactions
         INNER JOIN frauds ON transactions.Transaction_ID = frauds.Transaction_ID
         INNER JOIN cards ON cards.Card_Number = transactions.Credit_Card_ID
         INNER JOIN customers ON customers.Cust_ID = cards.Cust_ID''',
    con=cnx)
display(defrauded_customers)  

   
cur.close()  
cnx.close()


# ## Join Types
# 
# Outer joins return every row from at least one table.

# #### Left outer joins 
# All rows from the left table with any matches from the right table. If no matches, null is returned for each column of the right table.
# 
# This is the join type you will use 99% of the time. It is great for one-to-one and one-to-many relationships.

# In[ ]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: Show me all of the data on customers and their credit cards

cur.execute('''SELECT * 
               FROM customers 
                 LEFT JOIN cards ON customers.CUST_ID = cards.CUST_ID''')
customer_cards = pd.DataFrame(cur.fetchall())
customer_cards.columns = [x[0] for x in cur.description]
display(customer_cards)

# Possible next steps: Group by customer and count number of cards


# ----- RIGHT JOIN (not supported by sqlite) -----

# Request: I want a list of transactions preceded by a flag if they are fraudulent
# Possible next steps: Group by credit card number and get the sum of fraudulent transaction amounts

# transactions_flagged = pd.read_sql('''SELECT frauds.Fraud_Flag, transactions.*
#                    FROM frauds 
#                      RIGHT JOIN transactions ON frauds.Transaction_ID = cards.Transaction_ID''',
#                con=cnx)
transactions_flagged = pd.read_sql(
    '''SELECT frauds.Fraud_Flag, transactions.*
       FROM transactions 
         LEFT JOIN frauds ON frauds.Transaction_ID = transactions.Transaction_ID''',
    con=cnx, index_col='index')
display(transactions_flagged)

   
cur.close()  
cnx.close()


# 
# #### Right outer joins
# All rows from the right table with any matches from the left table. If no matches, null is returned for each column of the left table.
# 
# These queries can always be written as the opposite left join. You might want to use a right join to makes your query easier to read and understand.

# In[ ]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: I want a list of transactions preceded by a flag if they are fraudulent

# RIGHT JOIN below is not supported by sqlite

# transactions_flagged = pd.read_sql('''SELECT frauds.Fraud_Flag, transactions.*
#                    FROM frauds 
#                      RIGHT JOIN transactions ON frauds.Transaction_ID = cards.Transaction_ID''',
#                con=cnx)

# Equivalent supported query

transactions_flagged = pd.read_sql(
    '''SELECT frauds.Fraud_Flag, transactions.*
       FROM transactions 
         LEFT JOIN frauds ON frauds.Transaction_ID = transactions.Transaction_ID''',
    con=cnx, index_col='index')
display(transactions_flagged)


# Possible next steps: Get the total of fraudulent transaction amounts for every credit card

fraud_amounts_by_card = pd.read_sql(
    '''SELECT SUM(
         CASE WHEN frauds.Fraud_Flag = 1
           THEN transactions.Transaction_Value 
           ELSE 0 END
        ) AS Fraud_Total, transactions.Credit_Card_ID
       FROM transactions 
         LEFT JOIN frauds ON frauds.Transaction_ID = transactions.Transaction_ID
       GROUP BY transactions.Credit_Card_ID''',
   con=cnx)
display(fraud_amounts_by_card)

   
cur.close()  
cnx.close()


# #### Full outer joins
# All rows from the left table and the right table. If no matches, null is returned for each column of the left or right table as needed.
# 
# This is a great query to see if you have missing data so you can address the issue.

# In[ ]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: Make sure we don't have any orphaned data between customers and credit cards

# OUTER JOIN is not supported by sqlite

# customers_cards = pd.read_sql(
#     '''SELECT *
#        FROM customers 
#          FULL OUTER JOIN cards ON customers.Customer_ID = cards.Customer_ID''',
#     con=cnx)

# Equivalent supported query

customers_cards = pd.read_sql(
    '''SELECT customers.*, cards.*
       FROM customers
         LEFT JOIN cards ON customers.Cust_ID = cards.Cust_ID
       UNION ALL
       SELECT customers.*, cards.* 
       FROM cards
         LEFT JOIN customers ON customers.Cust_ID = cards.Cust_ID
       WHERE customers.Cust_ID IS NULL''',
    con=cnx)
display(customers_cards)


# Possible next steps: Display only rows where data is missing

rows_missing_data = pd.read_sql(
    '''SELECT * FROM (
         SELECT customers.*, cards.*
         FROM customers
           LEFT JOIN cards ON customers.Cust_ID = cards.Cust_ID
         UNION ALL
         SELECT customers.*, cards.* 
         FROM cards
           LEFT JOIN customers ON customers.Cust_ID = cards.Cust_ID
         WHERE customers.Cust_ID IS NULL) as joined_table
       WHERE Cust_ID is NULL OR Card_Number is NULL''',
    con=cnx)
display(rows_missing_data)

# Note: If you only want rows with missing data from one table, a simple left join will suffice. 
# For example to show only customers with no card data

cur.close()  
cnx.close()


# #### Inner joins
# Show only rows with matches from both tables
# 
# Inner join can be replaced with a left join with where clauses to filter out rows without matches.

# In[ ]:


cnx = sqlite3.connect('data/credit_card_data/credit_card_data.sqlite')
cur = cnx.cursor()

# Request: Show me all fraudulent transactions

fraudulent_transactions = pd.read_sql(
    '''SELECT transactions.* 
       FROM transactions 
         INNER JOIN frauds ON transactions.Transaction_ID = frauds.Transaction_ID''',
    con=cnx)
display(fraudulent_transactions)

# Possible next steps: Show all the customers with fraudulent transactions

defrauded_customers = pd.read_sql(
    '''SELECT DISTINCT customers.*
       FROM transactions
         INNER JOIN frauds ON transactions.Transaction_ID = frauds.Transaction_ID
         INNER JOIN cards ON cards.Card_Number = transactions.Credit_Card_ID
         INNER JOIN customers ON customers.Cust_ID = cards.Cust_ID''',
    con=cnx)
display(defrauded_customers)  

   
cur.close()  
cnx.close()

