import os
from datetime import datetime, timedelta
import pytz
import requests
import csv

import db_connect

base_url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"
root_dir = r'C:/Users/kevin/Desktop/GEOG797/CapstoneProject/main/data_processing/airquality_csv/'

#! dont add empty rows to db
#! dont add double quote character in every row's first element


def main():
    get_last_48hours()


def get_last_48hours():

    DATE_TIME_STRING_FORMAT = "%Y%m%d%H"

    now = datetime.now(tz=pytz.utc)
    date_time = now - timedelta(days=2)
    date_times_arr = [date_time.strftime(DATE_TIME_STRING_FORMAT)]

    while date_time < now:
        date_time += timedelta(hours=1)
        date_times_arr.append(date_time.strftime(DATE_TIME_STRING_FORMAT))

    for time in date_times_arr:
        print("Downloading file for ", time)
        download_airnow_data(time)

    delete_older_files(date_times_arr)


def download_airnow_data(timestamp):
    airnow_file_location = timestamp[0:4] + "/" + \
        timestamp[0:8] + "/HourlyAQObs_" + timestamp + ".dat"
    complete_url = base_url + airnow_file_location
    output_file = root_dir + timestamp + ".csv"
    if not os.path.exists(output_file):
        aq_file = requests.get(complete_url)
        print(complete_url)
        aq_file_string = aq_file.content.decode('utf-8')
        if aq_file.status_code == 404:
            print("File not found:", output_file)
        else:
            print("Writing", output_file)
            # col_index = [0, 1, 2, 4, 5, 6, 9, 10, 11, 14, 15, 16, 17]
            with open(output_file, 'w') as file:
                aq_writer = csv.writer(
                    file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL, skipinitialspace=True)
                for line in aq_file_string.splitlines():
                    line_split = line.split("\",\"")
                    if (line_split[8] == "US"):
                        str_val_list = [line_split[0], line_split[1], line_split[2],
                                        line_split[9], line_split[10], line_split[11]]
                        num_val_list = [line_split[4], line_split[5], line_split[6],
                                        line_split[14], line_split[15], line_split[16], line_split[17]]

                        str_val_list[0] = str_val_list[0][1:]
                        for i in range(len(num_val_list)):
                            if num_val_list[i] == '':
                                num_val_list[i] = '-9999'

                        final_list = [str_val_list[0], str_val_list[1], str_val_list[2], num_val_list[0],
                                      num_val_list[1], num_val_list[2], str_val_list[3], str_val_list[4],
                                      str_val_list[5], num_val_list[3], num_val_list[4], num_val_list[5],
                                      num_val_list[6]]

                        aq_writer.writerow(final_list)

            print("Status Code: ", aq_file.status_code)
            print("Finished writing " + output_file)

            read_csv(output_file)
    else:
        print("Already exists -->", output_file)


def read_csv(csv_file):
    with open(csv_file, newline='') as csvfile:
        aq_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        aq_reader_edited = []
        for row in aq_reader:
            if len(row) > 0:
                aq_reader_edited.append(row)
        db_connect.insert_rows_to_db(aq_reader_edited)


def delete_older_files(date_array):
    filename_array = []
    for time in date_array:
        out_file = time + ".csv"
        filename_array.append(out_file)

    existing_files = os.listdir(root_dir)
    for existing_file in existing_files:
        if existing_file not in filename_array:
            if os.path.exists(root_dir + existing_file):
                print("Deleting", existing_file)
                os.remove(root_dir + existing_file)
            else:
                print("File doesn't exist: ", existing_file)
        else:
            print("Required file is already downloaded:", existing_file)


if __name__ == "__main__":
    main()
