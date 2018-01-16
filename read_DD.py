import pandas as pd
import glob
import queue


def generate_file_queue(source = "CHURN_15-Jan/DD/*.xlsx"):

    queue_array = [filename for filename in glob.glob(source)]
    queue_array.sort()
    file_queue = queue.Queue()

    for i in range(len(queue_array)):
        file_queue.put(item = queue_array[i])

    return file_queue
    

def print_dataframes(file_queue):

    writer = pd.ExcelWriter("Data_dictionary_15-Jan.xlsx", engine='xlsxwriter')
    workbook = writer.book

    filecount = 0
    for _ in range(int(file_queue.qsize())):
        filename = file_queue.get()
        print(filename)

        file = pd.ExcelFile(io = str(filename))
        sheets = file.sheet_names

        for m in range(len(sheets)):

            if filecount == 0 :
                worksheet = workbook.add_worksheet(name= sheets[m])
                worksheet.write(0, 1, "Bag1")
                worksheet.write(0, 3, "Bag2")
                worksheet.write(0, 5, "Bag3")
                worksheet.write(0, 7, "Corp1")
                worksheet.write(0, 9, "Corp2")
                worksheet.write(0, 11, "Corp3")
                worksheet.write(0, 13, "Fi1")
                worksheet.write(0, 15, "Fi2")
                worksheet.write(0, 17, "Fi3")


            else :
                worksheet = workbook.get_worksheet_by_name(name = sheets[m])

            df0 = pd.read_excel(io = str(filename), header = 2, sheetname = m)
            dict0 = df0.to_dict(orient = 'series')
            dict0.pop('Data Type', None)
            dict = {"Column" : dict0["Column Name"], "Missing %" : dict0["Missing Percentage"]}


            df = pd.DataFrame(data = dict)

            print(df)
            dict_keys = list(df.keys())

            i = 0
            for key in dict_keys :

                key_column = pd.Series(data = df[key])
                worksheet.write(1, 1 + i + filecount, key_column.name)

                j = 0
                for element in key_column :
                    worksheet.write(2 + j, 1 + i + filecount, element)
                    j += 1

                i+= 1


        filecount += 2

fq = generate_file_queue()
print_dataframes(fq)
