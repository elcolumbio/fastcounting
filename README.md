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

# efficient queries
filter dimensions:
- date
- account

Type of queries:
- aggregation -> we leverage lua scripts and one dimensional arrays to combine filtering and lookup in lua.
- atomic -> we seperate and make filter easy to use and we have one lookup function: atomics in and details out
  alternative atomic -> filter: we create temporary sets and do redis buildin joins on them.
Most data stays in redis.
We have easy remove and rollback logic, useful if you want to do some trial and error analysis.

If we have more experience we maybe add multiple flatten views for querying and use the existing data structure as core we build on.

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
