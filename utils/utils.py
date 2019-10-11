import warnings
warnings.filterwarnings("ignore")

import time
import pandas as pd
import re
import os

from datetime import datetime


def convert_string_date(filename):
	published_date = filename.split("-")[1].split(".")[0]

	published_date = datetime.strptime(published_date, "%Y%m%d")

	return published_date


def readCSVAsArray(filename):
	df = pd.read_csv(filename, sep=",", header=None)
	df = df.values
	df = df.reshape((len(df),))

	return df


def get_day(file):
	published_date = convert_string_date(file)

	return published_date.strftime("%d")


def is_logged_in(gigyaid):
	if gigyaid == "nan":
		return 0

	else:
		return 1


def get_article_type(url):
	home = "https://news.abs-cbn.com/"

	try:
		url = url.split(home)[1]

	except:
		return ""

	else:
		url = re.split(r'[\? |/]', url)[0]
		return url



def readCSV(filename, cols):
	start_time = time.time()

	cols = list(cols)

	if not cols:
		data = pd.read_csv(filename, sep=",")

	else:
		data = pd.read_csv(filename, sep=",", usecols=cols)

	print("Runtime readCSV: ", time.time() - start_time, "\n")

	return data


def addWeekDay(data, filename):
	start_time = time.time()

	published_date = convert_string_date(filename)

	data.loc[:, "week"] = published_date.strftime("%V")
	data.loc[:, "day"] = published_date.strftime("%d")

	print("Runtime addWeekDay: ", time.time() - start_time, "\n")
	
	return data


def addIsLoggedIn(data):
	start_time = time.time()

	data.loc[:, "isloggedin"] = data.gigyaid.map(str).apply(lambda x: is_logged_in(x))

	print("Runtime addIsLoggedIn: ", time.time() - start_time, "\n")

	return data


def addContentType(data):
	start_time = time.time()

	data.loc[:, "article_type"] = data.currentwebpage.map(str).apply(lambda x: get_article_type(x))

	print("Runtime addContentType: ", time.time() - start_time, "\n")

	return data


def mergeDF(df1, df2, key):
	start_time = time.time()

	res = pd.merge(df1, df2, on=key)

	print("Runtime mergeDF: ", time.time() - start_time, "\n")

	return res


def concatDF(df):
	start_time = time.time()

	res = pd.concat(df, ignore_index=True)

	print("Runtime concatDF: ", time.time() - start_time, "\n")

	return res


def toCSV(data, outfile):
	start_time = time.time()

	if os.path.isfile(outfile):
		with open(outfile, "a") as csv:
			data.to_csv(csv, header=False, index=False)

	else:
		data.to_csv(outfile, index=False)

	print("Runtime toCSV: ", time.time() - start_time, "\n")