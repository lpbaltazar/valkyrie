import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd


def getRows(data):

	return len(data)

def getNullDescription(data):

	total = getRows(data)
	data = data.isnull().sum(axis = 0).to_frame("null")
	data.index.name = "colnames"
	data["not_null"] = total - data["null"]

	return data.reset_index()

def getMin(data, cols):

	data = data[cols].min(axis = 0).to_frame("min")
	data.index.name = "colnames"

	return data.reset_index()

def getMax(data, cols):

	data = data[cols].max(axis = 0).to_frame("max")
	data.index.name = "colnames"

	return data.reset_index()

def getMean(data, cols):

	data = data[cols].mean(axis = 0).to_frame("mean")
	data.index.name = "colnames"

	return data.reset_index()


def getMedian(data, cols):

	data = data[cols].median(axis = 0).to_frame("median")
	data.index.name = "colnames"

	return data.reset_index()

def getMode(data, cols):

	new_df = pd.DataFrame(index = cols, columns = ["mode"])
	for col in cols:
		new_df.loc[col, "mode"] = getModeCol(data[col])

	return new_df

def getModeCol(data):

	mode = data.mode()

	if len(mode) == 1:
		return mode.iloc[0]
	else:
		return(list(mode))

def getUniqueValue(data, col):

	data = data.reset_index().groupby(col)["index"].nunique().to_frame("value")
	data.index.name = "variable"
	data["colname"] = col
	data.reset_index(inplace = True)

	return data[["colname", "variable", "value"]]