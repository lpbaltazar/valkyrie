import warnings
warnings.filterwarnings("ignore")

from build.column_descriptor import colDailyDesc, colWeeklyDesc, colWeeklyStats
from build.build import nullDesc, summarize

if __name__ == "__main__":

	""" DAILY AND WEEKLY NULL DESCRIPTOR
		Builds:
			1. null daily description
			2. null weekly description

		Output:
			1. null_description.csv

	"""
	nullDesc()

	""" DAILY AND WEEKLY COLUMN DESCRIPTOR
		Builds:
			1. column categorical daily description
			2. column cardinal daily description
			3. column categorical weekly description
			4. column cardinal MIN MAX weekly description
			5. column cardinal MEAN MEDIAN MODE weekly description

		Outputs:
		1. column_categorical_daily_description.csv
		2. column_cardinal_daily_description.csv
		3. column_categorical_weekly_description.csv
		3. column_cardinal_weekly_description.csv
		4. column_MeanMediaMode_weekly_description.csv
	"""

	colDailyDesc("categorical")
	colDailyDesc("cardinal")

	colWeeklyDesc("categorical")
	colWeeklyDesc("cardinal")
	colWeeklyStats()

	""" SUMMARY
		Builds:
			1. summarize

		Output:
			1. daily_summary.csv
			2. weekly_summary.csv
	"""

	summrize()





