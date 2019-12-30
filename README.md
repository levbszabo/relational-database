# Mini Relational Database with Order

Most of the major database functions are implemented including Select, Join and Groupby (with aggregates). Only the barebones
of Python are used (no MySQL or Pandas). All dataframes are stored as a Table class which is a list of lists with additional meta
data (indexes, headers, etc). Indexing is supported but only on 1 numeric feature. The indexes are stored as Hash sets (dictionary) or a BTree. The sales1 and sales2 files are initial tables which can be read from IO or as fileinput and serve as the basis for initial queries. Every query is given a name and inserted into a dictionary. All queries are written to AllOperations.txt file, and the outputtofile function can be used to write to file a specific query. Every query is timed and the elapsed time is written to IO.


Additional descriptions of functions in main.py can be found in Database_Description.txt

## Setup
Python 3.5 is used with the following packages

sys, re, numpy, time, operator.itemgetter, BTrees.OOBTree

## Running database
inputFile.txt is given as an example of potential user queries. But commands from IO can be given as well.

a) python main.py inputFile.txt

b) python main.py  (then give input with empty line causing exit)


Note: All work was done as a final project for the Fall 2019 NYU Database Systems graduate course. Special thanks to Dennis Shasha for running an amazing course


