import pandas as pd
import numpy as np
import datetime

def week_index(date):
    month_dictionary = {"JAN" : 1, "FEB" : 2, "MAR" : 3, "APR" : 4, "MAY" : 5, "JUN" : 6, "JUL" : 7, "AUG" : 8, "SEPT" : 9, "OCT" : 10, "NOV" : 11, "DEC" : 12}
    day = int(date[:2])
    month = int(month_dictionary[str(date[2:5])])
    year = int(date[5:])

    transaction_date = datetime.date(year = year, month = month, day = day)
    old_date = datetime.date(year = 2016, month = 1, day = 4    )

    m1 = old_date - datetime.timedelta(days = old_date.weekday())
    m2 = transaction_date - datetime.timedelta(days = transaction_date.weekday())

    weeks = (m2 - m1).days/7
    return weeks


writer = pd.ExcelWriter("week_index.xlsx", engine = 'xlsxwriter')
workbook = writer.book
worksheet = workbook.add_worksheet(name = 'test')

df0 = pd.read_csv(filepath_or_buffer = "OBS_TXNS.csv", nrows = 10000)
#print(df0.head())

my_dict = {"tran_date_new" : df0["tran_date_new"], "dr_cr" : df0["dr_cr"], "txn_amt" : df0["txn_amt"], "Channel" : df0["Channel"]}
my_df = pd.DataFrame(data = my_dict)
my_df.dropna(axis = 0, inplace = True)

my_df["amount"] = np.where(my_df["dr_cr"] == 'D', -my_df["txn_amt"], my_df["txn_amt"])
my_df["week_index"] = my_df["tran_date_new"].apply(func = lambda x : week_index(x))
max_weeks = int(my_df["week_index"].max())
print(my_df["week_index"])

channels = list(my_df["Channel"].unique())
channels = [x for x in channels if str(x) != 'nan']
channels_dict = {str(channels[i]) : i for i in range(len(channels))}
# print(channels_dict)

worksheet.write(0, 0, "Channels")
worksheet.write(0, 1, "Week Index")
for i in range(len(channels)):
    worksheet.write(i + 2, 0, channels[i])

for i in range(max_weeks):
    worksheet.write(1, i + 1, i)

# print(max_weeks)
# print(my_df[(my_df.week_index == 417 ) & (my_df.Channel == "ATM")])

for i in range(len(channels)):
    sum = 0
    for j in range(max_weeks):
        transactions = my_df[(my_df.week_index == j ) & (my_df.Channel == channels[i])]
        sum += transactions["amount"].sum()

        worksheet.write(i + 2, j + 1, sum)

workbook.close()
exit()


