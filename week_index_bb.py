def week_index(my_date):
    try:
        month_dictionary = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEPT": 9,
                            "OCT": 10, "NOV": 11, "DEC": 12}

        date = int(my_date[:2])
        month = int(month_dictionary[str(my_date[2:5])])
        year = int(my_date[5:])

        transaction_date = datetime.date(year=year, month=month, day=date)
        old_date = datetime.date(year=2016, month=7, day=1)

        monday1 = old_date - datetime.timedelta(days=old_date.weekday())
        monday2 = transaction_date - datetime.timedelta(days=transaction_date.weekday())

        weeks = (monday2 - monday1).days / 7
        return weeks

    except:
        print(my_date)


import findspark

findspark.init("/home/sagnikb/Downloads/spark-2.1.0-bin-hadoop2.7")

import os
from pyspark import SparkConf
from bbcliutils.rztdata.SparkConfCustom import SparkConfCustom
from bbcliutils.rztdata import ExecContexts
from bbcliutils.rztdata.RZTData import RZTData

conf = SparkConf().setAppName("Test2")
conf.set('spark.executor.memory', '4G')
conf = conf.setMaster("spark://127.0.0.1:7077")
spark_conf = SparkConfCustom(conf)

dsconfig = dict()
dsconfig["path"] = "/home/sagnikb/Downloads/OBS_NEW.csv"
rztdata = RZTData(ExecContexts.prod)
df = rztdata.read(dsconfig)
df.print_ui_table()
df.na_drop()

df = df.add_column("+/-", lambda x: -1 if x == 'D' else 1, "dr_cr")
df = df.add_column("Week_index", week_index, "tran_date_new")


def get_amt(x):
    x["Amount"] = x["txn_amt"] * x["+/-"]
    return x


df = df.map(get_amt)
# df.print_ui_table()
df = df.pivot_table(values="Amount", columns="Week_index", index="Channel")

df.print_ui_table()
print(df.cols())

save_path = "/home/sagnikb/Downloads/sess1.csv"
delimiter = ','
df.to_csv(save_path, delimiter=delimiter)
