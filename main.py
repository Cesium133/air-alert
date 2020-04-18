import os
from datetime import datetime, timedelta
import pytz
import requests
import csv

base_url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"
root_dir = r'C:/Users/kevin/Desktop/GEOG797/CapstoneProject/data-download/sample-download/'


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
            print("File not found: ", output_file)
        else:
            print("Writing", output_file)
            col_index = [0, 1, 2, 4, 5, 6, 9, 10, 11, 14, 15, 16, 17]

            with open(output_file, 'w') as file:
                aq_writer = csv.writer(
                    file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for line in aq_file_string.splitlines():
                    line_split = line.split(",")
                    aq_writer.writerow([line_split[ind] for ind in col_index])

            print("Status Code: ", aq_file.status_code)
            print("Finished downloading " + output_file)
    else:
        print("Already exists -->", output_file)


def delete_older_files(date_array):
    filename_array = []
    for time in date_array:
        out_file = time + ".csv"
        filename_array.append(out_file)

    existing_files = os.listdir(root_dir)
    for existing_file in existing_files:
        print("Deleting", existing_file)
        if existing_file not in filename_array:
            os.remove(existing_file)


if __name__ == "__main__":
    main()
