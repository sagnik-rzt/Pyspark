import pandas as pd
import os
import glob

def open_dataframes(source = "*.csv123", open_df = True):

    writer = pd.ExcelWriter("excel_file.xlsx", engine='xlsxwriter')
    workbook = writer.book
    worksheet1 = workbook.add_worksheet('Sheet1')
    worksheet1.write(0, 1, "Filenames")
    worksheet1.write(0, 3, "Filesizes in bytes")
    worksheet1.write(0, 5, "No. of rows")
    worksheet1.write(0, 7, "No. of columns")

    file_count = 0
    for filename in glob.glob(source) :
        print(filename)
        filesize = os.path.getsize(filename = str(filename))

        worksheet1.write(1 + file_count, 1, str(filename))
        worksheet1.write(1 + file_count, 3, filesize)

        if open_df == True :

            df = pd.read_csv(filepath_or_buffer = str(filename))   #Opens the CSV in pandas dataframe format

            n_rows = df.count(axis = 0)[0]
            n_columns = df.count(axis = 1)[0]

            worksheet1.write(1 + file_count, 5, n_rows)
            worksheet1.write(1 + file_count, 7, n_columns)

            worksheet2 = workbook.add_worksheet('Sheet2' + str(file_count))
            worksheet2.write(0, 3, "Fraction of unique elements in given key")
            worksheet2.write(0, 5, "Fraction of null elements")
            worksheet2.write(0, 7, "Number of unique elements in given key")
            worksheet2.write(0, 9, "Total number of elements in given key")

            dict = df.to_dict(orient = 'series')    #Gets the dictionary
            dict_keys = list(dict.keys())           #Gets the keys of the dictionary
            n_keys = len(dict_keys)

            i = 0
            for key in dict_keys:
                key_column = df[key]
                worksheet2.write(i + 1, 0, key_column.name)
                n_elements = key_column.size

                unique_list = key_column.unique()
                n_unique_elements = len(unique_list)
                unique_fraction = n_unique_elements/ n_elements

                worksheet2.write(i + 1, 3, unique_fraction)   #Prints the fraction of unique elements in each key

                n_NaNs = key_column.isnull().sum()
                NaN_fraction = n_NaNs/n_elements
                worksheet2.write(i + 1, 5, NaN_fraction)

                worksheet2.write(i + 1, 7, n_unique_elements)
                worksheet2.write(i + 1, 9, n_elements)


                i += 1

            #Prints the number of null valued data-points in the dataset

            writer.save()

        file_count += 1


open_dataframes()