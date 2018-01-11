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

            i = 0
            for key in dict_keys:
                key_column = df[key]
                worksheet.write(i, 0, key_column.name)

                unique_list = pd.DataFrame(data = key_column.unique())  #Prints the unique elements in each key
                unique_list.to_excel(excel_writer = writer, sheet_name = 'Sheet3', startcol = (3*i + 1))

                value_counts = key_column.value_counts()
                value_counts.to_excel(excel_writer = writer, sheet_name = 'Sheet4', startcol = (3*i + 1))         #Prints the counts of each value that the elements under the key have

                i += 1

            NaNs = df.apply(lambda x: sum(x.isnull()), axis=0)
            NaNs.to_excel(excel_writer = writer, sheet_name = 'Sheet5', startcol = 0)    #Prints the number of null valued data-points in the dataset


            writer.save()

        file_count += 1


open_dataframes()