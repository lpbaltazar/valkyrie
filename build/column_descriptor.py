import warnings
warnings.filterwarnings("ignore")

import datetime as dt
import os
import sys
import time

import pandas as pd
import re

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from utils.utils import readCSV, readCSVAsArray, get_day, toCSV, mergeDF, convert_string_date
from utils.statistics import getUniqueValue, getMean, getMedian, getMode, getMin, getMax


def categorical_day_column_description(data, categorical_cols):

	res = pd.DataFrame()

	for i, col in enumerate(categorical_cols):
		tmp = getUniqueValue(data, col)

		res = res.append(tmp)

	return res


def cardinal_day_column_description(data, cardinal_cols):

	res = pd.DataFrame(cardinal_cols, columns=["colnames"])

	res = mergeDF(res, getMean(data, cardinal_cols), "colnames")
	res = mergeDF(res, getMedian(data, cardinal_cols), "colnames")
	res = mergeDF(res, getMode(data, cardinal_cols), "colnames")
	res = mergeDF(res, getMin(data, cardinal_cols), "colnames")
	res = mergeDF(res, getMax(data, cardinal_cols), "colnames")

	res = pd.melt(res, id_vars="colnames", value_vars=["mean", "median", "mode", "max", "min"])

	return res


def cardinal_week_column_description(data, cardinal_cols):
	res = pd.DataFrame(cardinal_cols, columns=["colnames"])

	res = mergeDF(res, getMean(data, cardinal_cols), "colnames")
	res = mergeDF(res, getMedian(data, cardinal_cols), "colnames")
	res = mergeDF(res, getMode(data, cardinal_cols), "colnames")

	res = pd.melt(res, id_vars="colnames", value_vars=["mean", "median", "mode"])

	return res


def get_weekly_cardinal(data, var):
	data = data.loc[data.variable==var]

	tmp = data \
		.groupby(["filter_value", "colnames", "variable"]) ["value"] \
		.agg(var) \
		.rename("value") \
		.pipe(pd.DataFrame) \
		.reset_index()

	return tmp


def add_additional_cols(df, cols_dict):

	for key, val in cols_dict.items():
		df.loc[:, key] = val

	return df


def organized_files_weekly(data_files):
	
	published_dates = [convert_string_date(file) for file in data_files]

	arr = list(zip(data_files, published_dates))

	dates_df = pd.DataFrame(arr, columns=["filename", "published_date"])
	dates_df.loc[:, "week"] = dates_df.published_date.apply(lambda x: (x + dt.timedelta(days=1)).week)

	return dates_df



def colDailyDesc(data_type):
	start_time = time.time()

	order_cols = ["published_date", "filter_type", "filter_value", "data_type", "colnames", "variable", "value"]

	cols_dict = {}
	cols_dict["filter_type"] = "day"
	cols_dict["data_type"] = data_type

	cols = readCSVAsArray("../data/"+data_type+"_columns.csv")

	source_path = "../../dummy/09"
	data_files = os.listdir("../../dummy/09/") 									# Change to use real data

	for i, file in enumerate(data_files):
		print("Calculating ", i, file)

		file_path = os.path.join(source_path, file)

		data = readCSV(file_path, cols)

		if data_type == "categorical":
			res = categorical_day_column_description(data, cols)

		else:
			res = cardinal_day_column_description(data, cols)

		cols_dict["published_date"] = convert_string_date(file)
		cols_dict["filter_value"] = get_day(file)

		res = add_additional_cols(res, cols_dict)

		toCSV(res[order_cols], "../results/column_"+data_type+"_daily_description.csv")

	print("RUNTIME: ", time.time() - start_time)


def colWeeklyDesc(data_type):
	start_time = time.time()

	order_cols = ["filter_type", "filter_value", "data_type", "colnames", "variable", "value"]

	cols_dict = {}
	cols_dict["filter_type"] = "week"
	cols_dict["data_type"] = data_type

	file = "../results/column_" + data_type + "_daily_description.csv"
	data = pd.read_csv(file, sep=",", parse_dates=["published_date"])

	data.loc[:, "filter_value"] = data.published_date.apply(lambda x: (x + dt.timedelta(days=1)).week)

	if data_type == "categorical":
		
		res = data \
			.groupby(["filter_value", "colnames", "variable"]) ["value"] \
			.agg("sum") \
			.rename("value") \
			.pipe(pd.DataFrame) \
			.reset_index()

	elif data_type == "cardinal":

		res = get_weekly_cardinal(data, "max")
		res = res.append(get_weekly_cardinal(data, "min"))
	
	res = add_additional_cols(res, cols_dict)

	toCSV(res[order_cols], "../results/column_" + data_type + "_weekly_description.csv")


def colWeeklyStats():
	start_time = time.time()	

	order_cols = ["filter_type", "filter_value", "data_type", "colnames", "variable", "value"]

	cols_dict = {}
	cols_dict["filter_type"] = "week"
	cols_dict["data_type"] = "cardinal"

	cardinal_cols = readCSVAsArray("../data/cardinal_columns.csv")

	source_path = "../../dummy/09"
	data_files = os.listdir("../../dummy/09/") 

	dates = organized_files_weekly(data_files)

	for week, group in dates.groupby(["week"])["filename"]:
		print(week)

		total_df = pd.DataFrame()

		for i, file in enumerate(group.values):
			print(i, file)

			file_path = os.path.join(source_path, file)

			data = readCSV(file_path, cardinal_cols)

			total_df = total_df.append(data)


		res = cardinal_week_column_description(total_df, cardinal_cols)

		cols_dict["filter_value"] = week

		res = add_additional_cols(res, cols_dict)

		toCSV(res[order_cols], "../results/column_MeanMedianMode_weekly_description.csv")

	print("RUNTIME: ", time.time() - start_time)
