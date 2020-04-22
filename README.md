# fastcounting
Fastcounting is a python module which helps you to understand your accounting data.
Accounting Software is localized and comes with much functionality.

We take a step back and only implement the ground truth.
And we make it easy for you to ask your own questions.

The basic use case right now is make queries against our views.
Our backend store should allow us to easily introduce new functionality when needed.

# dependencies
redis 6 beta
pyredis 4.3.2

plotly
pandas

# memory usage of redisdb in my local ram


type | memory used | info
--- | --- | ---
csv | 2.6 MB | the file we import
csv rows | 61 245 rows | ~ number of transactions(multi tax = 1 transaction)
number atomic | 181 224 entries | number of atomic transactions
accountsystem | 0.16 MB | we join for more meaningful output
**without views** | 108 MB | core db
atomic hashes | 181 224 | -
general hashes | ~ 65 000 | -
sorted sets | 4 with a length of 181 224 | -
**with views** | 186 MB | core db and views
same as without views | "" | ""
atomic view | 181224 entries | big stream with fields joined
account view | 90 with total of 181 224 entries | we distribute data over accounts




# What is the minimum viable data and the relations of it?

**Account** is a collection of related accounting entries

**Accounting transaction** links two or more entries together, so the total of all entries is zero.

Our naming convention for 'entries' is: atomic.
And our naming convention for 'accounting transaction' is: general.

# How do we handle amounts
We transfer every number to the balance side, where this very account is increasing.
That means you don't need to look at the balance side, you will see just by the value.
If the account is increasing or decreasing.

To do this we need to categorize all accounts with the help of our standard accounting frame.
E.g. in Germany we have SKR04, there we have 8 main classes 0-7. Only in the 8th and last class we have mixed accounts.  
0-1 : active accounts (incr left)  
2-3 : passive accounts (incr right)  
4 : revenues (incr right)  
5-6 : expenditures (incr left)  
7 : revenues and expenditures  
So we need more information for 7. There is a category column in our accounting frame.
We have only 3 groups for 7. Einnahmen = revenue and Abschreibungen, Betriebsausgaben = expenditures.
Like this we matched all accounts which are part of your final reporting.

# efficient queries
underlying dimensions of accounting:
- date
- account
- relations  
For now we focused on the first Two.

Backup queries:
- aggregation -> we leverage lua scripts and one dimensional arrays to combine filtering and lookup in lua.
- atomic -> we seperate and make filter easy to use and we have one lookup function: atomics in and details out
  alternative atomic
Most data stays in redis.

View queries:
Build on top of our Backend very easy to create and delete and to query against.
- atomic view -> all the data you can filter by date
- account views -> for every account -> a stream you can filter by date too

Batch processing:
We have some capabilities in our backend, but we won't implement it for now.
We want to have easy remove and rollback logic, useful if you want to do some trial and error analysis.
Also to get meaningful Diff Views would be very nice.

# language barrier
From my understanding accounting terms are in the official language for each country.
I still have to replace some german and maintain a dictionary.

# Features
- Event driven
- data rebuild-able
- simplicity
- fast read
- multi state diffs
- useful queries
- data validation

# Data Structure
![Alt redis data structure](store.png)
