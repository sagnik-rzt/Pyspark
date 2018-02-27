import pandas as pd
import glob
import numpy as np
import queue


def generate_queue(source):
    files = [file for file in glob.glob(source)]
    files.sort()
    file_queue = queue.Queue()

    for i in range(len(files)):
        file_queue.put(item=files[i])

    return file_queue


folder_queue = generate_queue(source="/home/sagnikb/Downloads/Savings_data/SAMPLE_*")

for _ in range(int(folder_queue.qsize())):
    foldername = folder_queue.get()
    _, _, _, _, _, local_foldername = foldername.split("/")
    print(foldername)
    filequeue = generate_queue(source=foldername + "/*.csv")

    for _ in range(int(filequeue.qsize())):
        filename = filequeue.get()
        _, _, _, _, _, _, local_filename = filename.split("/")
        print(filename)
        try:
            df = pd.read_csv(filepath_or_buffer=filename, usecols=['NATURE_QUERY', 'CLIENT_SUBFUNCTION'])
            for column in list(df.columns):
                print(column)
                counts = df[column].value_counts(normalize=True)
                print(counts)
                counts.to_csv(
                    path_or_buf="/home/sagnikb/Downloads/Savings_data/" + column + local_filename + local_foldername + ".csv",
                    sep=',')
                print("\n")


        except:
            print("columns not found")

        else:
            df = pd.read_csv(filepath_or_buffer=filename, usecols=['NATURE_QUERY'])
            for column in list(df.columns):
                print(column)
                counts = df[column].value_counts(normalize=True)
                print(counts)
                counts.to_csv(
                    path_or_buf="/home/sagnikb/Downloads/Savings_data/" + column + local_filename + local_foldername + ".csv", sep=',')
