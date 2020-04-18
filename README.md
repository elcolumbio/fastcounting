# fastcounting
Fastcounting is a python module which helps you to understand your accounting data.
Accounting Software is localized and comes with much functionality.

We take a step back and only implement the ground truth.
And we make it easy for you to ask your own questions.

# dependencies
since we just get started we depend on some latest versions.

redis latest (because we are not using the deprecated hmset, its not part of redis 4.3.1)
plotly
pandas 1.0


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
