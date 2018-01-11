import pandas as pd
import glob

def open_dataframes(source = "/home/sagnikb/PycharmProjects/Pyspark/*.csv123", open_df = True):

    file_count = 0
    for filename in glob.glob(source) :

        writer = pd.ExcelWriter(str(file_count) + "excel_file.xlsx", engine='xlsxwriter')
        print(filename)

        if open_df == True :
            df = pd.read_csv(filepath_or_buffer = str(filename))   #Opens the CSV in pandas dataframe format

            summary = df.describe()      #Statistical summary of numerical variables
            summary.to_excel(excel_writer = writer, sheet_name = 'Sheet1')

            workbook = writer.book
            worksheet = workbook.add_worksheet('Sheet2')

            dict = df.to_dict(orient = 'series')    #Gets the dictionary
            dict_keys = list(dict.keys())           #Gets the keys of the dictionary
            n_keys = len(dict_keys)

            for key in range(n_keys):
                key_column = df[dict_keys[key]]
                worksheet.write(int(key), 0, dict_keys[key])

                unique_list = pd.DataFrame(data = key_column.unique())  #Prints the unique elements in each key
                print(unique_list)
                unique_list.to_excel(excel_writer = writer, sheet_name = 'Sheet3')

                column_counts =  key_column.value_counts()
                print(column_counts)
                column_counts.to_excel(excel_writer = writer, sheet_name = 'Sheet4')        #Prints the count of elements under each key

                value_counts = key_column.value_counts()
                print(value_counts)
                value_counts.to_excel(excel_writer = writer, sheet_name = 'Sheet5')         #Prints the counts of each value that the elements under the key have

                NaNs = df.apply(lambda x: sum(x.isnull()), axis=0)
                print(NaNs)
                NaNs.to_excel(excel_writer = writer, sheet_name = 'Sheet6')    #Prints the number of null valued data-points in the dataset

            writer.save()

        file_count += 1

open_dataframes()