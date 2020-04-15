# fastcounting
Fastcounting is a python module which helps you to understand your accounting data.
Accounting Software is localized and comes with much functionality.

We take a step back and only implement the ground truth.
And we make it easy for you to ask your own questions.

# What is the minimum viable data and the relations of it?

**Account** is a collection of related accounting entries

**Accounting transaction** links two or more entries together, so the total of all entries is zero.

Our naming convention for 'entries' is: atomic.
And our naming convention for 'accounting transaction' is: general.

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
