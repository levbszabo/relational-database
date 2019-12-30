# Mini Relational Database with Order

Most of the major database functions are implemented including Select, Join and Groupby (with aggregates). Only the barebones
of the Python are used (no MySQL or Pandas). All dataframes are stored as a Table class which is a list of lists with additional meta
data (indexes, headers, etc). Indexing is supported but only on 1 numeric feature. The indexes are stored as Hash sets (dictionary) or a BTree.

## Prerequisites
Python 3.5 is used with the following packages
sys, re, numpy, time, operator.itemgetter, BTrees.OOBTree

## Function Descriptions
class Table:
	maintains all information about a table
	variables: name(str), header(list[str]), table(list[list[]]),
	index(dict{int: list[int]}), index_att(str)
	The index and index_att are a dictionary and corresponding 
	attribute name which give us the indices for a given value of index_att

method Input:
	input: str filename, list[str] line
	ouput: void
	Reads a ("|") seperated text file and saves into tables

method split_str:
	input: str string, list[str] delimit
	output: delimit, list[str]
	Determins the delimit for a given string, useful in cleaning code which
	splits on multiple arithmetic or relation operations

method std_select:
	input: str table, list[str] conditions
	output: list[list[str]] std_conditions
	Given the multiple ways in which a select statement can be given 
	std_select attempts to standardize the statements into an equivalent 
	form, namely [att,aritOP,constant2,relationOP,constant1]
	so ideally each condition is in the form 
	time - 2 >= 5
	When the statement is reversed we map the relation operation to its
	opposite, so that 2 <= time - 5  ->  time-5 >= 2

method std_join:
	input: list[str] conditions
	output: list[list[str]] std_conditions
	The structure is simpler than std_select since the form is generally
	the same, we put into std form as 
	[t1,t1_att, arit1, c1, rel, t2,t2_att, arit2, c2] 
	Ex.
	[R, qty, +, 2, >=, S, time,* ,3]

method select_helper:
	input: list[str] condition
	output boolean
	During select we replace the attribute with a value, so our input
	 condition will be something like
	[3,-,2,>=,5] 
	The select helper goes through all components and including relation
	and arithmetic ops to resolve and determine if cond is true

method select:
	input: str table_name, list[list[str]] std_condtion, str conjunction,
	str new_table_name
	output:void
	Creates a new_table Table and adds into tables dict. 
	Then determines if there is an index on orig. table and whether or not
	the index is on an attribute that is contained in the conditions list
	If we do have an index and relation is "=" then use index

	If no index exists then simply loop through each row and determine if
	it passes all conditions

method structured_array:
	input: str table_name
	output: np.array with structured tuples as rows
	Used for finding the columns of a table, np structured arrays can handle
	2D datasets with differing data types, first we determine the dtype of each
	column and then construct an np structured array.This can be returned and 
	sliced "vertically" down the column to provide better structure than list 
	of lists

method project:
	input: str table_name, list[str] att_list, str new_table_name
	output: void
	creates a np structured array from orig table and slices by the given
	attributes
	
method avg:
	input: str table_name, list[str] att_list, str new_table_name
	output: void
	creates a np structured array and calculates the mean of the slice
	by attribute list (single attribute)

All groupby use the same structure of essentially constructing a multi index dictionary
Suppose we wish to sumgroup(att1, att2, att3) 
then we construct dict{att2: list[int]} and for each att2 value we find all its
corresponding att3 values and then make new_dict{(att2,att3): list[int]} 
This is done until all attributes are members of the key in our final dictionary
In this case we can apply our aggregates to att1 (count, sum , etc.) 

