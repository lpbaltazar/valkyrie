import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")

import os
import time
import pandas as pd
import numpy as np

import utils.statistics as stats
import utils.utils as utils

data_dir = "../../data/09/"

def nullDailyDesc():
	
	s = time.time()
	for f in sorted(os.listdir(data_dir)):
		if f.endswith(".csv"):
			data = pd.read_csv(os.path.join(data_dir, f), nrows = 100)

			data = utils.addWeekDay(data, f)

			df = stats.getNullDescription(data)

			df = pd.melt(df, id_vars = ["colnames"], value_vars = ["num_null", "not_null"])
			df["filter_type"] = "day"
			df["filter_value"] = data.iloc[0]["day"]
			df["week"] = data.iloc[0]["week"]

			df = df[["filter_type", "filter_value", "colnames", "variable", "value", "week"]]
			print(df.head())

			utils.toCSV(df, "../../results/daily_null_description.csv")

	e = time.time()
	print("Runtime nullDailyDesc: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


def nullWeeklyDesc():
	
	s = time.time()

	data = utils.readCSV("../../results/daily_null_description.csv")
	data = data.groupby(["week", "colnames", "variable"]).sum().reset_index()
	
	data.drop("filter_value", axis = 1, inplace = True)
	
	data["filter_type"] = "week"
	data.rename(columns = {"week":"filter_value"}, inplace = True)

	data = data[["filter_type", "filter_value", "colnames", "variable", "value"]]

	utils.toCSV(data, "../../results/weekly_null_description.csv")

	e = time.time()
	print("Runtime nullWeeklyDesc: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


def nullDesc():

	s = time.time()

	nullDailyDesc()
	nullWeeklyDesc()

	dfs = []
	daily = utils.readCSV("../../results/daily_null_description.csv")
	weekly = utils.readCSV("../../results/weekly_null_description.csv")

	dfs.append(daily.drop("week", axis = 1))
	dfs.append(weekly)
	
	data = utils.concatDF(dfs)
	
	utils.toCSV(data, "../../results/null_description.csv")

	e = time.time()
	print("Runtime nullDesc: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


def count_is_logged_in():

	s = time.time()

	for f in sorted(os.listdir(data_dir)):
		if f.endswith(".csv"):
			data = pd.read_csv(os.path.join(data_dir, f), nrows = 200000)

			data = utils.addWeekDay(data, f)
			data = utils.addIsLoggedIn(data)

			new_df = pd.DataFrame(index = [data.iloc[0]["day"]], data = data["isloggedin"].sum(), columns = ['isloggedin'])
			new_df.index.name = 'day'
			new_df["week"] = data.iloc[0]["week"]

			utils.toCSV(new_df.reset_index(), "../../results/daily_logged_in.csv")


	data = utils.readCSV("../../results/daily_logged_in.csv")
	data = data.groupby("week")["isloggedin"].sum().to_frame()
	utils.toCSV(data.reset_index(), "../../results/weekly_logged_in.csv")

	e = time.time()
	print("Runtime count_is_logged_in: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))


def summarizeDailyWeekly(filter_type):

	s = time.time()

	if filter_type == "day":
		data = utils.readCSV("../../results/daily_null_description.csv")
		loggedin = utils.readCSV("../../results/daily_logged_in.csv")
		loggedin.drop("week", axis = 1, inplace = True)
	elif filter_type == "week":
		data = utils.readCSV("../../results/weekly_null_description.csv")
		loggedin = utils.readCSV("../../results/weekly_logged_in.csv")
	else:
		print("filter_type must be day or week")

	data.drop_duplicates(subset = ["filter_value", "variable"], inplace = True)

	data = data.groupby(["filter_value"])["value"].sum().to_frame("num_transactions")
	data.index.name = filter_type

	data = utils.mergeDF(data.reset_index(), loggedin, filter_type)

	e = time.time()
	print("Runtime summarizeDailyWeekly: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))

	return data


def summarize():

	s = time.time()
	
	count_is_logged_in()
	
	data = summarizeDailyWeekly("day")
	utils.toCSV(data, "../../results/daily_summary.csv")

	data = summarizeDailyWeekly("week")
	utils.toCSV(data, "../../results/weekly_summary.csv")

	e = time.time()
	print("Runtime summarize: ", time.strftime("%H:%M:%S", time.gmtime(e-s)))
	
