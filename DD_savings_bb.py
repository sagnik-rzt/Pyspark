import pandas as pd
import glob
import queue
from bbcliutils.rztdata import ExecContexts
from bbcliutils.rztdata.RZTData import RZTData


def generate_queue(source):
    files = [file for file in glob.glob(source)]
    files.sort()
    file_queue = queue.Queue()

    for i in range(len(files)):
        file_queue.put(item=files[i])

    return file_queue


def dd_details():
    folder_queue = generate_queue(source = "/home/sagnikb/sa_model_data_all/SAMPLE_*")

    for _ in range(int(folder_queue.qsize())):
        folder_name = folder_queue.get()
        sample_file_queue = generate_queue(source = str(folder_name) + "/*.csv")
        print(folder_name)

        for _ in range(int(sample_file_queue.qsize())):
            filename = sample_file_queue.get()

            dsconfig_left = dict()
            dsconfig_left["path"] = str(filename)
            dsconfig_left["encoding"] = "utf-8"
            dsconfig_left["header"] = "True"

            rzt_prod = RZTData(ExecContexts.prod)

            df = rzt_prod.read(dsconfig_left)
            columns = df.cols()
            nas = df.na_info()

            for key in columns:
                uniques = df.unique(key = str(key))
                unique_fraction = len(uniques) / df.count()
                nas_count = nas[str(key)]["count"]
                nas_fraction = nas_count / df.count()

                print((str(key), unique_fraction, nas_fraction))