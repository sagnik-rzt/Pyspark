import pandas as pd
import glob

def open_dataframes(source = "/home/sagnikb/PycharmProjects/Pyspark/*.csv123", open_df = True):

    for filename in glob.glob(source):
        print(filename)

        if open_df == True :
            df = pd.read_csv(filepath_or_buffer = str(filename))   #Opens the CSV in pandas dataframe format
            print(df.head(), '\n')      #The head of the dataframe
            print(df.describe())      #Statistical summary of numerical variables

            dict = df.to_dict(orient = 'series')    #Gets the dictionary
            dict_keys = list(dict.keys())           #Gets the keys of the dictionary

            n_keys = len(dict_keys)

            for key in range(n_keys):
                this_key = df[dict_keys[key]]
                print(this_key.count())     #Prints the count of elements under each key
                print(this_key.value_counts()) #Prints the counts of each value that the elements under the key have
                print(this_key.unique())   #Prints the unique elements in each key
                print(df.apply(lambda x: sum(x.isnull()), axis=0))    #Prints the number of null valued data-points in the dataset

open_dataframes()