import os
from datetime import datetime, timedelta
import pytz
import requests
import csv


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
        break


def download_airnow_data(timestamp):
    base_url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/"
    airnow_file_location = timestamp[0:4] + "/" + \
        timestamp[0:8] + "/HourlyAQObs_" + timestamp + ".dat"
    complete_url = base_url + airnow_file_location
    output_file = r'C:/Users/kevin/Desktop/GEOG797/CapstoneProject/data-download/sample-download/' + timestamp + ".csv"
    if not os.path.exists(output_file):
        aq_file = requests.get(complete_url)
        print(aq_file.content)
        if aq_file.status_code == 404:
            print("File not found: ", output_file)
        else:
            print("Starting", output_file)
            with open(output_file, 'wb') as file:
                aq_writer = csv.writer(
                    file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for line in aq_file.content:  # ! check issue here NEED to pass in a list of strings?
                    aq_writer.writerow(line)
                # file.write(aq_file.content)
            print("Status Code: ", aq_file.status_code)
            print("Finished downloading " + output_file)
    else:
        print("Already exists -->", output_file)

    # TODO: edit csv to delete fields I don't want, data reduction
    # TODO: update postgres db with records.


if __name__ == "__main__":
    main()
