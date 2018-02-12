def week_index(my_date):
    try:
        month_dictionary = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9,
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


from bbcliutils.rztdata import ExecContexts
from bbcliutils.rztdata.RZTData import RZTData

dsconfig_left = dict()
dsconfig_left["path"] = "/home/sagnikb/Downloads/OBS_NEW.csv"
dsconfig_left["encoding"] = "utf-8"
dsconfig_left["header"] = "True"

rzt_prod = RZTData(ExecContexts.experiment)

df = rzt_prod.read(dsconfig_left)

df.print_ui_table()
df = df.na_resolve(replace_val=0)
df = df.add_column("Week_index", week_index, "tran_date_new")


def get_amt(x):
    if x["dr_cr"] == 'D':
        x["txn_amt"] *= -1
        return x

    else:
        return x


df = df.map(get_amt)
# df.print_ui_table()
df = df.pivot_table(values="txn_amt", columns="Week_index", index="Channel")

df.print_ui_table()
