import pandas as pd
import glob

def open_dataframes(source = "/home/sagnikb/PycharmProjects/Pyspark/*.csv123", open_df = True):

    writer = pd.ExcelWriter("excel_file.xlsx", engine='xlsxwriter')
    workbook = writer.book
    worksheet1 = workbook.add_worksheet('Sheet1')

    file_count = 0
    for filename in glob.glob(source) :
        print(filename)

        worksheet1.write(0, 2, "Filenames")
        worksheet1.write(2, 2 + file_count, str(filename))

        if open_df == True :

            df = pd.read_csv(filepath_or_buffer = str(filename))   #Opens the CSV in pandas dataframe format

            worksheet2 = workbook.add_worksheet('Sheet2' + str(file_count))
            worksheet2.write(0, 4, "Fraction of unique elements in given key")
            worksheet2.write(0, 9, "Fraction of null elements")
            worksheet2.write(0, 14, "Number of unique elements in given key")
            worksheet2.write(0, 19, "Total number of elements in given key")

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

                worksheet2.write(i + 1, 4, unique_fraction)   #Prints the fraction of unique elements in each key

                n_NaNs = key_column.isnull().sum()
                NaN_fraction = n_NaNs/n_elements
                worksheet2.write(i + 1, 9, NaN_fraction)

                worksheet2.write(i + 1, 14, n_unique_elements)
                worksheet2.write(i + 1, 19, n_elements)


                i += 1

            #Prints the number of null valued data-points in the dataset

            writer.save()

        file_count += 1


open_dataframes()