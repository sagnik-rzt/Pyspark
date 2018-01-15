import pandas as pd
import glob

def open_dataframes(source = "CHURN_15-Jan/DD/*.xlsx"):

    writer = pd.ExcelWriter("Data_dictionary_15-Jan.xlsx", engine='xlsxwriter')
    workbook = writer.book

    filecount = 0
    for filename in glob.glob(source):
        print(filename)

        file = pd.ExcelFile(io = str(filename))
        sheets = file.sheet_names
        print("No. of sheets", len(sheets))

        for m in range(len(sheets)):

            print("sheet ", m)
            if filecount == 0 :
                worksheet = workbook.add_worksheet(name= str(m))
                worksheet.write(0, 0, "Bag1")
                worksheet.write(0, 3, "Bag2")
                worksheet.write(0, 6, "Bag3")
                worksheet.write(0, 9, "Corp1")
                worksheet.write(0, 12, "Corp2")
                worksheet.write(0, 15, "Corp3")
                worksheet.write(0, 18, "Fi1")
                worksheet.write(0, 21, "Fi2")
                worksheet.write(0, 24, "Fi3")


            else :
                worksheet = workbook.get_worksheet_by_name(name = str(m))

            df = pd.read_excel(io = str(filename), header = 2, sheetname = m)
            dict = df.to_dict(orient = 'series')
            dict.pop('Data Type', None)
            df = pd.DataFrame(data = dict)
            dict_keys = list(df.keys())

            print(df)

            i = 0
            for key in dict_keys :

                key_column = pd.Series(data = df[key])
                worksheet.write(1, 1 + i + filecount, key_column.name)

                j = 0
                for element in key_column :
                    worksheet.write(2 + j, 1 + i + filecount, element)
                    j += 1

                i+= 1


        filecount += 3

open_dataframes()