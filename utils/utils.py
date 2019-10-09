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


def is_logged_in(gigyaid):
	if not gigyaid:
		return 1
	else:
		return 0


def get_article_type(url):
	home = "https://news.abs-cbn.com/"

	try:
		url = url.split(home)[1]

	except:
		return ""

	else:
		url = re.split(r'[\? |/]', url)[0]
		return url



def readCSV(filename):
	start_time = time.time()

	data = pd.read_csv(filename, sep=",")

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

	if outfile.isfile():
		with open(outfile, "a") as csv:
			csv.write(data)

	else:
		print("Make initial outfile")

	print("Runtime toCSV: ", time.time() - start_time, "\n")