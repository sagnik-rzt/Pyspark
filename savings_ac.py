import pandas as pd
import glob
import queue

def generate_queue(source) :

    files = [file for file in glob.glob(source)]
    files.sort()
    file_queue = queue.Queue()

    for i in range(len(files)) :
        file_queue.put(item=files[i])

    return file_queue

def print_dd_details() :

    writer = pd.ExcelWriter("Savings_ac_DD.xlsx", engine = 'xlsxwriter')
    workbook = writer.book

    folder_queue = generate_queue(source = "/home/sagnikb/savings_sampled/sample_*")

    widthspace = 0
    folder_count = 0
    for _ in range(int(folder_queue.qsize())) :
        folder_name = folder_queue.get()
        sample_file_queue = generate_queue(source = str(folder_name) + "/*.csv")
        print(folder_name)

        file_count = 0
        for _ in range(int(sample_file_queue.qsize())) :
            filename = sample_file_queue.get()
            print(filename)

            if folder_count == 0 :
                worksheet = workbook.add_worksheet(name = str(file_count))
                worksheet.write(0, 1, "Sample_1")
                worksheet.write(0, 5, "Sample_2")
                worksheet.write(0, 9, "Sample_3")

            else :
                worksheet = workbook.get_worksheet_by_name(name = str(file_count))

            df = pd.read_csv(filepath_or_buffer=str(filename))
            keys = list(df.keys())
            worksheet.write(1, 1 + widthspace, "Columns")
            worksheet.write(1, 2 + widthspace, "Data_type")
            worksheet.write(1, 3 + widthspace, "Unique_fraction")
            worksheet.write(1, 4 + widthspace, "Null_fraction")

            i = 0
            for key in keys:
                key_column = pd.Series(data = df[key])
                worksheet.write(2 + i, 1 + widthspace, key_column.name)

                data_type = str(key_column.dtype)
                worksheet.write(2 + i, 2 + widthspace, data_type)

                uniques = key_column.unique()
                unique_fraction = len(uniques)/key_column.size
                worksheet.write(2 + i, 3 + widthspace, unique_fraction)

                nulls = key_column.isnull().sum()
                null_fraction = nulls/key_column.size
                worksheet.write(2 + i, 4 + widthspace, null_fraction)

                i += 1

            file_count += 1

        folder_count += 1
        widthspace += 4

    workbook.close()

print_dd_details()