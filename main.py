import sys
import re
import numpy as np
import time
from operator import itemgetter
from BTrees.OOBTree import OOBTree
tables = {}
class Table:
    def __init__(self,name,header):
        self.name = name
        self.header = header
        self.table = []
        self.n = 0
        self.index = None
        self.index_att = None
    def insert(self,row):
        newrow = []
        for i in range(len(row)):
            try:
                x = float(row[i])
            except ValueError:
      	        x = str(row[i])
            newrow.append(x)    
        self.table.append(newrow)
        self.n = self.n + 1
def input(filename,line):
	f = open(filename,"r")
	table = str(line[0])
	header = f.readline().split("\n",1)[0]
	header_array = header.split("|")
	tables[table] = Table(table,header_array)
	for row in f:
		row = row.split("|")
		row[-1] = row[-1][:-1]
		tables[table].insert(row)

#split str given delimit
#input: list of delimiters ["d1","d2","d3"..] 
#output: delim, string split on given delim
def split_str(string,delimit): 
	for delim in delimit:
        	if delim in string:
                    return delim,string.split(delim,1)

# std_select standardizes conditions for use in a select statement
# input: string name of database and conditions list
# output: standardized condition list where each entry is of the form
# std_cond = [attribute,arithmetic_op,constant2,relation_op,constant1]
# if no arithmetic_op and constant 2 given (such as qty < 5) then
# set arit = "+" and constant2 = 0.0
# example of input condition:
#	5 != time - 2
# output:
#	attr: time, arit_op: -, constant2: 2, rel_op:!= constant1: 5  
def std_select(table,conditions):
        attributes = tables[table].header 
        relop = ["!=",">=","<=","<",">","="]
        opposite_rel = {"!=":"!=","=":"=",">":"<","<":">",">=":"<=","<=":">="}
        arithop = ["+","-","*","/"]
        std_conditions = []
        for condition in conditions:
        	attr,attr_split = split_str(condition,attributes)
        	if attr_split[0].strip() == "":
        		rel,rel_split = split_str(attr_split[1],relop)
       			if rel_split[0].strip()=="":
        			try:
        				c1 = float(rel_split[1].strip())
        			except ValueError:
        				c1 = str(rel_split[1].strip())
        			arit = "+"
        			c2 = 0.0
        		else:
             			try:
             				c1 = float(rel_split[-1].strip())
             			except ValueError:
             				c1 = str(rel_split[-1].strip())
             			arit,arit_split = split_str(rel_split[0].strip(),arithop)
             			c2 = float(arit_split[-1].strip())
        	else: ##these are in flipped order and relation needs to be swapped
        		rel,rel_split = split_str(attr_split[0],relop)
        		try:
        			c1 = float(rel_split[0].strip())
        		except ValueError:
        			c1 = str(rel_split[0].strip())
        		if attr_split[1].strip() == "":
        	    		arit = "+"
        	    		c2 = 0.0
        		else:
            			arit,arit_split = split_str(attr_split[1],arithop)
            			c2 = float(arit_split[-1].strip())	
        		rel = opposite_rel[rel]
       		std_conditions.append([attr,arit,c2,rel,c1])
        return std_conditions
#std_join puts each condition into a standardized form
#where std form is a list of the form 
# [Table 1, table 1 atribute,arit1,c1, relop, table 2, table 2 attribute,arit2,c2]
#input: table name and condition list
#output: list of standardized conditions
def std_join(conditions):
	relop = ["!=",">=","<=","<",">","="]
	arithop = ["+","-","*","/"]
	std_conditions = []
	
	for condition in conditions:
		foundSplit = False
		for rel in relop:
			if rel in condition and not foundSplit:
				temp = condition.split(rel,1)
				t1 = temp[0].split(".")[0].strip()
				t1_att = temp[0].split(".")[1].strip()
				arit1 = "+"
				c1 = 0
				t2 = temp[1].split(".")[0].strip()
				t2_att = temp[1].split(".")[1].strip()
				arit2 = "+"
				c2 = 0
				for arit in arithop:
					if arit in temp[0]:
						arit1 = arit
						temp1 = temp[0].split(arit,1)
						t1 = temp1[0].split(".")[0].strip()
						t1_att = temp1[0].split(".")[1].strip()
						c1 = float(temp1[1].strip())
					if arit in temp[1]:
						arit2 = arit	
						temp2 = temp[1].split(arit,1)
						t2 = temp2[0].split(".")[0].strip()
						t2_att = temp2[0].split(".")[1].strip()
						c2 = float(temp2[1].strip())
				std = [t1,t1_att,arit1,c1,rel,t2,t2_att,arit2,c2]
				std_conditions.append(std)
				foundSplit = True
	return std_conditions
	
#select(table,std_condition,conjunction)
#given  a condition in standardized form with attribute replaced by actual entry value
#determine if it is true
#input: standardized condition 
#output: boolean
def select_helper(condition):
	if isinstance(condition[4],str):
		if condition[3] == "=":
			return condition[0] == condition[4]
		else:
			return condition[0] != condition[4]
	lhs = condition[0]
	if condition[1] == "+":
		lhs = lhs + condition[2]
	if condition[1] == "-":	
		lhs = lhs - condition[2]
	if condition[1] == "*":
		lhs = lhs * condition[2]
	if condition[1] == "/":
		lhs = lhs / condition[2]
	if condition[3] == "=":
		return lhs == condition[4]
	if condition[3] == "!=":
		return lhs != condition[4]
	if condition[3] == ">":
		return lhs > condition[4]
	if condition[3] == "<":
		return lhs < condition[4]
	if condition[3] == ">=":
		return lhs >= condition[4]
	if condition[3] == "<=":
		return lhs <= condition[4]
	else:
		pass
def select(table_name, std_condition, conjunction ,new_table_name):
	#print(table_name, std_condition, conjunction ,new_table_name)
	T = tables[table_name]
	header = T.header
	new_T = Table(new_table_name,header)
	header_idx = {k: v for v,k in enumerate(header)}
	if T.index_att is not None:
		contains_att = False
		condition_idx = 0
		#check if we have an indexed attribute
		for i in range(len(std_condition)):
			if (std_condition[i][0] == T.index_att) and (std_condition[i][3] == "="):
				contains_att = True
				condition_idx = i
		if contains_att:
			#std_condition[condition_idx] gives the condition containing indexed attr.
			#the above value at -1 is the equality we are testing (ex. qty = 5)
			idx = T.index[std_condition[condition_idx][-1]]
			for i in idx:
				if conjunction == "and":
					contains = True
					for j in std_condition:
						if j == condition_idx:
							continue
						else:
							#test each condition
							value = std_condition[j][:]
							att = value[0]
							att_idx = header_idx[att]
							val0 = T.table[i][att_idx]
							bool = select_helper(value)
							if not bool:
								contains = False
				else: #conjunction == "or" or None meaning it has passed at least on condition
					contains = True
				if contains:
					new_T.table.append(T.table[i])
			tables[new_table_name] = new_T		
			return
	for entry in T.table:
		if conjunction=="and":
			contains = True
			for condition in std_condition:
				value = condition[:]
				att = value[0]
				att_idx = header_idx[att]
				val0 = entry[att_idx]
				value[0] = val0
				bool = select_helper(value)
				if bool:
					pass
				else:
					contains = False
		else:#check for >=1 condition for "or" or single cond.
			contains = False
			for condition in std_condition:
				value = condition[:]
				att = value[0]
				att_idx = header_idx[att]
				val0 = entry[att_idx]
				value[0] = val0
				bool = select_helper(value)
				if bool:
					contains = True
				else:
					pass
		if contains:
			new_T.table.append(entry)		
	tables[new_table_name] = new_T		
# return a structured array, a list of tuples with given float 
# or string datatypes and attribute name
# input: table name
# output: list of tuples with specified value,datatype,attr name
# allows column slicing
def structured_array(table_name): 	
	T1 = tables[table_name]
	header = T1.header
	T1_tuple = [tuple(l) for l in T1.table]
	header_idx = {k: v for v,k in enumerate(header)}
	types = []
	entry = T1_tuple[0]
	for attribute in header:
		if isinstance(entry[header_idx[attribute]],str):
			types.append("U25")
		else:
			types.append("f")
	dtype_tuples = [(header[i],types[i]) for i in range(len(header))]
	dtype = np.dtype(dtype_tuples)
	T1_structured = np.array(T1_tuple,dtype)
	return T1_structured	
def project(table_name,att_list,new_table_name):
	tables[new_table_name] = Table(new_table_name,att_list)
	T1_structured = structured_array(table_name)
	projection = [list(l) for l in T1_structured[att_list]]
	tables[new_table_name].table = projection
def avg(table_name,att_list,new_table_name):
	new_header = [att_list[0]+".avg"]
	tables[new_table_name] = Table(new_table_name,new_header)
	T2_struct = structured_array(table_name)[att_list]
	avg_out = np.mean([list(l) for l in T2_struct])
	tables[new_table_name].table = [[avg_out]]
def sum(table_name,att_list,new_table_name):
	new_header = [att_list[0]+".sum"]
	tables[new_table_name] = Table(new_table_name,new_header)
	T2_struct = structured_array(table_name)[att_list]
	sum_out = np.sum([list(l) for l in T2_struct])
	tables[new_table_name].table = [[sum_out]]
def count(table_name,att_list,new_table_name):
	new_header = [att_list[0]+".count"]
	tables[new_table_name] = Table(new_table_name,new_header)
	T2_struct = structured_array(table_name)[att_list]
	count_out = len([list(l) for l in T2_struct])
	tables[new_table_name].table = [[count_out]]
def movavg(table_name,attr,value,new_table_name):
	new_header = [attr+".mov_avg"]
	tables[new_table_name] = Table(new_table_name,new_header)
	T2_struct = structured_array(table_name)[[attr]]
	table = [list(l) for l in T2_struct]
	i = 0
	moving_avg = []
	for j in range(value-1):
		j = j+1
		avg = np.mean(table[i:i+j])
		moving_avg.append([avg])
	while i+value <= len(table):
		avg = np.mean(table[i:i+value])
		moving_avg.append([avg])
		i= i+1
	tables[new_table_name].table = moving_avg
def movsum(table_name,attr,value,new_table_name):
	new_header = [attr+".mov_sum"]
	tables[new_table_name] = Table(new_table_name,new_header)
	T2_struct = structured_array(table_name)[[attr]]
	table = [list(l) for l in T2_struct]
	i=0
	moving_sum = []
	for j in range(value-1):
		j=j+1
		sum= np.sum(table[i:i+j])
		moving_sum.append([sum]) 
	while i+value<=len(table):
		sum = np.sum(table[i:i+value])
		moving_sum.append([sum])
		i=i+1
	tables[new_table_name].table = moving_sum
def index(table_name,attr,data_structure):
	if data_structure == "Btree":
		tree = OOBTree()
		T = tables[table_name]
		indexes = np.arange(len(T.table))
		header = T.header
		header_idx = {k: v for v,k in enumerate(header)}
		attr_idx = header_idx[attr]
		T_table = T.table
		for i in range(len(T_table)):
			value = T_table[i][attr_idx]
			if tree.has_key(value):
				current_idxs = tree[value]
				current_idxs.append(i)
				tree.update({value:current_idxs})
				
			else:
				tree.update({value:[i]})
		tables[table_name].index = tree		
		tables[table_name].index_att=attr	
	else:
		dict = {}
		T = tables[table_name]
		indexes = np.arange(len(T.table))
		header = T.header
		header_idx = {k: v for v,k in enumerate(header)}
		attr_idx = header_idx[attr]
		T_table = T.table
		for i in range(len(T_table)):
			value = T_table[i][attr_idx]
			dict.setdefault(value, []).append(i)
		tables[table_name].index = dict		
		tables[table_name].index_att=attr
# [Table 1, table 1 atribute,arit1,c1, relop, table 2, table 2 attribute,arit2,c2]

def joinHelper(value1,relop,value2):
	if relop == "=":
		return value1==value2
	if relop == "!=":
		return value1 != value2
	if relop == ">=":
		return value1>=value2
	if relop == "<=":
		return value1<=value2
	if relop == ">":
		return value1>value2
	if relop == "<":
		return value1<value2
	else:
		return False
	
def join(T1, T2, std_cond,conjunction, new_table_name):
	T1 = tables[T1]
	T2 = tables[T2]
	T3 = []
	T3_header = []
	header_idx1 = {k: v for v,k in enumerate(T1.header)}
	header_idx2 = {k: v for v,k in enumerate(T2.header)}
	for att in T1.header:
		T3_header.append(T1.name+"_"+att)
	for att in T2.header:
		T3_header.append(T2.name+"_"+att)
	tables[new_table_name] = Table(new_table_name,T3_header)
	#first find there is an index in std_cond
# [Table 1, table 1 atribute,arit1,c1, relop, table 2, table 2 attribute,arit2,c2]
	for i in range(len(T1.table)):
		for j in range(len(T2.table)):
			if conjunction == "and":
				passes = True
			else:
				passes = False
			row1 = T1.table[i]
			row2 = T2.table[j]
			for cond in std_cond:
				att1_idx = header_idx1[cond[1]]
				value1 = row1[att1_idx]
				if isinstance(value1,str):
					pass
				else:
					if cond[2] == "+":
						value1 =value1+cond[3]
					if cond[2] == "-":
						value1 = value1-cond[3]
					if cond[2] == "*":
						value1 =value1*cond[3]
					if cond[2] == "/":
						value1 = value1/cond[3]
				att2_idx = header_idx2[cond[6]]
				value2 = row2[att2_idx]
				if isinstance(value2,str):
					pass
				else:
					if cond[7] == "+":
						value2 =value2+cond[-1]
					if cond[7] == "-":
						value2 = value2-cond[-1]
					if cond[7] == "*":
						value2 =value2*cond[-1]
					if cond[7] == "/":
						value2 = value2/cond[-1]
				isTrue = joinHelper(value1,cond[4],value2)
				if (not isTrue) and (conjunction == "and"):
					passes = False
				if isTrue and (conjunction != "and"):
					passes = True
			if passes:
				newList = row1+row2
				T3.append(newList)
	tables[new_table_name].table = T3

#input: table name with list of attributes on which to group by
#output: returns a dictionary mapping tuple of given attributes to list of indices 
#where table has given attribute values
#this method is used as a helper function for sumgroup,avggroup and countgroup
#since once the dictionary is derived the aggregate calculations of the initial
#attribute are simple
def groupby(table_name,att_list):
	dic = {}
	T = tables[table_name]
	T1 = tables[table_name].table
	header = T.header
	header_idx = {k: v for v,k in enumerate(header)}
	for i in range(len(T1)):
		att_idx = header_idx[att_list[0]]
		value = T1[i][att_idx]
		k = (value,)
		dic.setdefault(k, []).append(i)
	if len(att_list) == 1:
		return dic
	else:
		hash_able = att_list[1:] 
		for new_att in hash_able:
        		new_att_idx = header_idx[new_att]
        		new_dic = {}
        		for i in dic:
            			index_list = dic[i] # gives us everyting for given value of (att1..att_i-1)
            			for idx in index_list:
                			value_at_new_att = T1[idx][new_att_idx]
                			lst = list(i)
                			lst.append(value_at_new_att)
                			new_key = tuple(lst)
                			new_dic.setdefault(new_key,[]).append(idx)
        		dic = new_dic
		return dic
def countgroup(table_name, att_list, new_table_name):
	dic = groupby(table_name,att_list[1:])
	new_header = att_list[1:]
	new_header.append(att_list[0]+"_count")
	tables[new_table_name] = Table(new_table_name,new_header)
	new_T = []
	for i in dic:
		lst = list(i)
		count = len(dic[i])
		lst.append(count)
		new_T.append(lst)
	tables[new_table_name].table = new_T
def avggroup(table_name,att_list,new_table_name):
	dic = groupby(table_name,att_list[1:])
	new_header = att_list[1:]
	new_header.append(att_list[0]+"_avg")
	tables[new_table_name] = Table(new_table_name,new_header)
	new_T = []
	T1 = tables[table_name]
	header_idx = {k: v for v,k in enumerate(T1.header)}
	avg_att_idx = header_idx[att_list[0]]
	for i in dic:
		lst = list(i)
		values = []
		for idx in dic[i]:
			values.append(T1.table[idx][avg_att_idx])
		avg = np.mean(values)
		lst.append(avg)
		new_T.append(lst)
	tables[new_table_name].table = new_T
def sumgroup(table_name,att_list,new_table_name):
	dic = groupby(table_name,att_list[1:])
	new_header = att_list[1:]
	new_header.append(att_list[0]+"_sum")
	tables[new_table_name] = Table(new_table_name,new_header)
	new_T = []
	T1 = tables[table_name]
	header_idx = {k: v for v,k in enumerate(T1.header)}
	sum_att_idx = header_idx[att_list[0]]
	for i in dic:
		lst = list(i)
		values = []
		for idx in dic[i]:
			values.append(T1.table[idx][sum_att_idx])
		sum = np.sum(values)
		lst.append(sum)
		new_T.append(lst)
	tables[new_table_name].table = new_T
def concat(T1, T2, new_table_name):
	T3 = tables[T1].table + tables[T2].table
	tables[new_table_name] = Table(new_table_name,tables[T1].header)
	tables[new_table_name].table = T3
def sort(table_name, att_list, new_table_name):
	new_header = tables[table_name].header
	tables[new_table_name] = Table(new_table_name,new_header)
	header_idx = {k: v for v,k in enumerate(new_header)}
	T2 = tables[table_name].table[:]
	sorting_order = att_list[::-1] #we need to go in reverse order because most sig bit need be sorted last 
	for att in sorting_order:
		att_idx = header_idx[att]
		T2.sort(key = itemgetter(att_idx))
	tables[new_table_name].table = T2
file_oper = open("ls5122_AllOperations","w")
def all_oper(table_name,line):
	line = str(line)
	T = tables[table_name].table
	header = tables[table_name].header
	file_oper.write(line + "\n")
	for i in range(len(header)):
		file_oper.write(header[i])
		if i < len(header)-1:
			file_oper.write("|")
		else:
			file_oper.write("\n")
	for i in range(len(T)):
		row = T[i]
		for j in range(len(row)):
			file_oper.write(str(row[j]))
			if j < len(row)-1:
				file_oper.write("|")
			else:	
				file_oper.write("\n")
	file_oper.write("\n")
if len(sys.argv) == 2:
	inf = open(sys.argv[1])
else:
	inf = sys.stdin
for line in inf:
	start = time.time()
	line = line.rstrip()
	outline = line
	if line == "":
		sys.exit()
	if line.split("(")[0] == "outputtofile":
		table_name = line.split("(")[1].split(",")[0].strip()
		filename = line.split("(",1)[1].split(",",1)[1].split(")",1)[0].strip()
		T_header = tables[table_name].header
		T_out = tables[table_name].table
		file_out = open("ls5122_"+filename,"w")
		for i in range(len(T_header)):
			file_out.write(T_header[i])
			if i < len(T_header)-1:
				file_out.write("|")
			else:
				file_out.write("\n")
		for i in range(len(T_out)):
			row = T_out[i]
			for j in range(len(row)):
				file_out.write(str(row[j]))
				if j < len(row)-1:
					file_out.write("|")
				else:
					file_out.write("\n")
		file_out.close()
	if line.split("(")[0] == "Btree":
		table_name = line.split("(")[1].split(",")[0].strip()
		attr = line.split("(",1)[1].split(",",1)[1].split(")",1)[0].strip()
		index(table_name,attr,"Btree")
		ind = tables[table_name].index
		ind_att = tables[table_name].index_att
		print(ind_att)
		continue		
	if line.split("(")[0] == "Hash":
		table_name = line.split("(")[1].split(",")[0].strip()
		attr = line.split("(",1)[1].split(",",1)[1].split(")",1)[0].strip()
		index(table_name,attr,"Hash")
		ind = tables[table_name].index
		continue
	if len(line.split(":=")) == 1:
		continue;
	line = line.split("//",1)[0]
	line = line.split(":=",1)
	line[0] = line[0].strip()  #new table name
	line[1] = line[1].strip()
	cmd = line[1].split("(",1)[0]
	if cmd == "inputfromfile":
		temp1 = line[1].strip().split("(",1)[1]
		filename = temp1.split(")",1)[0].strip()
		input(filename,line)
	if cmd == "project":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		attr_list = [input_array[i+1].strip() for i in range(len(input_array)-1)]
		project(table_name,attr_list,line[0])
		all_oper(line[0],outline)
	if cmd == "avg":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		attr_list = [input_array[1].strip()]
		avg(table_name,attr_list,line[0])
		all_oper(line[0],outline)
	if cmd == "movavg":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		attr = input_array[1].strip()
		mov_avg_val = int(input_array[2].strip())
		movavg(table_name,attr,mov_avg_val,line[0])
		all_oper(line[0],outline)
	if cmd == "movsum":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		attr = input_array[1].strip()
		mov_sum_val = int(input_array[2].strip())
		movsum(table_name,attr,mov_avg_val,line[0])
		all_oper(line[0],outline)	
	if cmd == "sum":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		attr_list = [input_array[1].strip()]
		sum(table_name,attr_list,line[0])
		all_oper(line[0],outline)
	if cmd == "count":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		attr_list = [input_array[1].strip()]
		count(table_name,attr_list,line[0])
		all_oper(line[0],outline)
	if cmd == "countgroup":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		att_list = [i.strip() for i in input_array[1:]]
		countgroup(table_name,att_list,line[0])	
		all_oper(line[0],outline)
	if cmd == "avggroup":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		att_list = [i.strip() for i in input_array[1:]]
		avggroup(table_name,att_list,line[0])
		all_oper(line[0],outline)
	if cmd == "sumgroup":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		att_list = [i.strip() for i in input_array[1:]]
		sumgroup(table_name,att_list,line[0])
		all_oper(line[0],outline)
	if cmd == "sort":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table_name = input_array[0].strip()
		att_list = [i.strip() for i in input_array[1:]]
		all_oper(line[0],outline)
	if cmd == "concat":
		input_form1 = line[1].split("(",1)[1].strip().split(")",1)[0]
		input_array = input_form1.split(",")
		table1_name = input_array[0].strip()
		table2_name = input_array[1].strip()
		concat(table1_name, table2_name,line[0])
		all_oper(line[0],outline)
	if cmd == "join":
		new_table_name = line[0]
		T1 = line[1].split(",")[0].split("(")[1].strip()
		T1 = str(T1)
		T2 = line[1].split(",")[1].strip()
		T2 = str(T2)
		conditions = line[1].split(",")[2].strip().rsplit(")",1)[0].strip()
		conditionsARRAY = []
		conjunction = ""
		if "or" in conditions:
			conjunction = "or"
			conditionsOR = conditions.split("or")
			for i in range(len(conditionsOR)):
				condition = conditionsOR[i].strip("( )")
				conditionsARRAY.append(condition)
		elif "and" in conditions:
			conjunction = "and"
			conditionsAND = conditions.split("and")
			for i in range(len(conditionsAND)):
				condition = conditionsAND[i].strip("( )")
				conditionsARRAY.append(condition)
		else:
			conditionsARRAY.append(conditions)
		std_cond = std_join(conditionsARRAY)
		join(T1,T2,std_cond,conjunction,line[0])
		all_oper(line[0],outline)
	if cmd == "select":
		new_table_name = line[0]
		table_name = line[1].split(",",1)[0].split("(")[1].strip()
		table_name = str(table_name)
		conditions = line[1].split(",",1)[1].strip().rsplit(")",1)[0].strip()
		conditionARRAY = []
		conjunction = ""
		if "or" in conditions :
			conjunction = "or"
			conditionsOR = conditions.split("or")
			for i in range(len(conditionsOR)):
				condition = conditionsOR[i].strip("( )")
				conditionARRAY.append(condition)
		elif "and" in conditions :
			conjunction = "and"	
			conditionsAND = conditions.split("and")	
			for i in range(len(conditionsAND)):
				condition = conditionsAND[i].strip("( )")
				conditionARRAY.append(condition)
		else:
			conditionARRAY.append(conditions.split("and")[0])
		std_conditions = std_select(table_name,conditionARRAY)  #gives list of standardized conditions
		select(table_name,std_conditions,conjunction,new_table_name)	
		all_oper(line[0],outline)
	end = time.time()
	print("time elapsed:",end-start)
file_oper.close()