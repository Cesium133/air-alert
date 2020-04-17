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
    # delete files with timestamps older than time
    delete_older_files(date_times_arr[0])

    while date_time < now:
        date_time += timedelta(hours=1)
        date_times_arr.append(date_time.strftime(DATE_TIME_STRING_FORMAT))

    for time in date_times_arr:
        download_airnow_data(time)
        break


def delete_older_files(time):
    # if there are files that exist in root_dir that are older than time, delete them
    # use timedelta to see if there are any matching files for time parameter minus timedelta(days=2) AKA two days before the last two days


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
                for line in aq_file_string.splitlines():  # ! check issue here NEED to pass in a list of strings?
                    line_split = line.split(",")
                    aq_writer.writerow([line_split[ind] for ind in col_index])

            print("Status Code: ", aq_file.status_code)
            print("Finished downloading " + output_file)
    else:
        print("Already exists -->", output_file)

    # TODO: edit csv to delete fields I don't want, data reduction
    # TODO: update postgres db with records.


if __name__ == "__main__":
    main()
