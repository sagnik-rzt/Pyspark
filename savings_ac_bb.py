import numpy as np
import glob
import os
import queue
from bbcliutils.rztdata import ExecContexts
from bbcliutils.rztdata.RZTData import RZTData


def generate_queue(source):
    queue_array = [filename for filename in glob.glob(source)]
    queue_array.sort()
    file_queue = queue.Queue()

    for i in range(len(queue_array)):
        file_queue.put(item=queue_array[i])

    return file_queue


def print_data():
    folder_queue = generate_queue(source="/home/sagnikb/sa_model_data_all/SAMPLE_*")

    for _ in range(int(folder_queue.qsize())):

        foldername = folder_queue.get()
        file_queue = generate_queue(source=str(foldername) + "/*.csv")

        for _ in range(int(file_queue.qsize())):
            filename = file_queue.get()
            base_name = os.path.basename(str(filename))

            ds_config = dict()
            filesize = os.path.getsize(str(filename)) / (1024 * 1024)

            ds_config["path"] = str(filename)
            ds_config["encoding"] = "utf-8"
            ds_config["header"] = "True"

            rzt_prod = RZTData(ExecContexts.prod)
            df = rzt_prod.read(ds_config)

            n_rows = df.count()
            n_cols = len(df.cols())

            if "CUST_ID" in list(df.cols()):
                uniques_custid = len(df.unique(key="CUST_ID"))

            else:
                uniques_custid = 0

            if "FORACID" in list(df.cols()):
                uniques_foracid = len(df.unique(key="FORACID"))

            else:
                uniques_foracid = 0

            if "CUST_ID" in list(df.cols()):
                nas_custid = df["CUST_ID"].na_info()

            else:
                nas_custid = 0

            if "FORACID" in list(df.cols()):
                nas_foracid = df["FORACID"].na_info()

            else:
                nas_foracid = 0

            print(base_name, filesize, n_rows, n_cols, uniques_custid, uniques_foracid, nas_custid, nas_foracid)


print_data()