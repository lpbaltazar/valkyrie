import warnings
warnings.filterwarnings("ignore")

import time
import numpy as np
import pandas as pd


def getRows(data):

	return len(data)

def getNullDescription(data):

	s = time.time()

	total = getRows(data)
	data = data.isnull().sum(axis = 0).to_frame("num_null")
	data.index.name = "colnames"
	data["not_null"] = total - data["num_null"]

	e = time.time()
	print("Runtime getNullDescription: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))

	return data.reset_index()

def getMin(data, cols):

	s = time.time()

	data = data[cols].min(axis = 0).to_frame("min")
	data.index.name = "colnames"

	e = time.time()
	print("Runtime getMin: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))

	return data.reset_index()

def getMax(data, cols):

	s = time.time()

	data = data[cols].max(axis = 0).to_frame("max")
	data.index.name = "colnames"

	e = time.time()
	print("Runtime getMax: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


	return data.reset_index()

def getMean(data, cols):

	s = time.time()

	data = data[cols].mean(axis = 0).to_frame("mean")
	data.index.name = "colnames"

	e = time.time()
	print("Runtime getMean: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


	return data.reset_index()


def getMedian(data, cols):

	s = time.time()

	data = data[cols].median(axis = 0).to_frame("median")
	data.index.name = "colnames"

	e = time.time()
	print("Runtime getMedian: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


	return data.reset_index()

def getMode(data, cols):

	s = time.time()

	new_df = pd.DataFrame(index = cols, columns = ["mode"])
	for col in cols:
		new_df.loc[col, "mode"] = getModeCol(data[col])

	e = time.time()
	print("Runtime getMode: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


	return new_df

def getModeCol(data):

	mode = data.mode()

	if len(mode) == 1:
		return mode.iloc[0]
	else:
		return(list(mode))

def getUniqueValue(data, col):

	s = time.time()

	data = data.reset_index().groupby(col)["index"].nunique().to_frame("value")
	data.index.name = "variable"
	data["colname"] = col
	data.reset_index(inplace = True)

	e = time.time()
	print("Runtime getUniqueValue: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))

	return data[["colname", "variable", "value"]]