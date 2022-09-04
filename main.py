import logging
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
import urllib.request
import json
import time
import os
import git  # First create a Github instance:


load_dotenv()

logging.basicConfig(filename='log.log', level=logging.INFO)

url = "https://covid.ourworldindata.org/data/owid-covid-data.json"

unwanted_keys = [
    "OWID_AFR", "OWID_ASI", "OWID_EUR", "OWID_EUN", "OWID_HIC", "OWID_INT",
    "OWID_KOS", "OWID_LIC", "OWID_LMC", "OWID_NAM", "OWID_CYN", "OWID_OCE",
    "OWID_SAM", "OWID_UMC"]


def upload_file(file):
    """Upload a file to an S3 bucket

    :param file: File to upload
    :return: True if file was uploaded, else False
    """

    # Upload the file
    s3 = boto3.client(
        "s3", region_name=os.environ['S3_BUCKET_REGION'],
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    try:
        response = s3.upload_file(
            file, os.environ['S3_BUCKET_NAME'], file)
    except ClientError as e:
        logging.error(get_time() + " - " + e)
        return False
    return True

def get_data(url):
    """Get raw data from url

    :param url: Url to get data from
    :return: data as json
    """
    with urllib.request.urlopen(url) as url:
        data = json.loads(url.read().decode())
        return data

def parse_data(raw_data):
    """Parse raw data into countries and data

    :param raw_data: Raw data as json
    :return: countries as json and data as json
    """
    countries = {}
    data = {}
    for iso in raw_data.keys():
        if iso not in unwanted_keys:
            iso_code = "WRL" if iso == "OWID_WRL" else iso
            countries[iso_code] = {
                "isoCode": iso_code, "location": raw_data[iso]["location"],
                "continent": raw_data[iso]["continent"]
                if "continent" in raw_data[iso] else "Other",
                "population": int(raw_data[iso]["population"]), }
            data[iso_code] = []
            for i in range(len(raw_data[iso]["data"])):
                data[iso_code].append(
                    {"date": raw_data[iso]["data"][i]["date"],
                     "new_cases": int(raw_data[iso]["data"][i]["new_cases"]) if "new_cases" in raw_data[iso]["data"][i] else 0,
                     "new_deaths": int(raw_data[iso]["data"][i]["new_deaths"]) if "new_deaths" in raw_data[iso]["data"][i] else 0,
                     "reproduction_rate": float(raw_data[iso]["data"][i]["reproduction_rate"]) if "reproduction_rate" in raw_data[iso]["data"][i] else 0.0,
                     "people_vaccinated": int(raw_data[iso]["data"][i]["people_vaccinated"]) if "people_vaccinated" in raw_data[iso]["data"][i] else 0,
                     "people_fully_vaccinated": int(raw_data[iso]["data"][i]["people_fully_vaccinated"]) if "people_fully_vaccinated" in raw_data[iso]["data"][i] else 0,
                     "total_boosters": int(raw_data[iso]["data"][i]["total_boosters"]) if "total_boosters" in raw_data[iso]["data"][i] else 0, })
    return countries, data

def upload_data(countries, data):
    """Upload data to S3 bucket

    :param countries: Countries as json
    :param data: Data as json
    :return:
    """
    logging.info("Uploading countries to S3 bucket")
    with open("countries.json", "w") as outfile:
        json.dump(countries, outfile)
    # upload_file("countries.json")
    # logging.info(get_time() + " - Uploaded countries.json")
    # os.remove("countries.json")
    # logging.info(get_time() + " - Deleted countries.json")

    for iso in countries.keys():
        with open(iso + ".json", "w") as outfile:
            json.dump(data[iso], outfile)
        # upload_file(iso + ".json")
        # logging.info(get_time() + " - Uploaded " + iso + ".json")
        # os.remove(iso + ".json")
        # logging.info(get_time() + " - Deleted " + iso + ".json")

    # logging.info(get_time() + " - Uploaded data to S3 bucket")

def get_time():
    h = int(time.gmtime(time.time()).tm_hour)
    h = "0" + str(h) if h < 10 else str(h)

    m = int(time.gmtime(time.time()).tm_min)
    m = "0" + str(m) if m < 10 else str(m)

    s = int(time.gmtime(time.time()).tm_sec)
    s = "0" + str(s) if s < 10 else str(s)

    return h + ":" + m + ":" + s

def main():
    ''' This is the main function '''
    raw_data = get_data(url)
    countries, data = parse_data(raw_data)
    upload_data(countries, data)


if __name__ == "__main__":
    logging.info(get_time() + " - Starting program")
    raw_data = get_data(url)
    countries, data = parse_data(raw_data)
    upload_data(countries, data)
    logging.info(get_time() + " - Program finished")
    commit_message = get_time() + " - Updated data"
    git.Repo().git.add(".")
    git.Repo().git.commit("-m", commit_message)
    git.Repo().git.push()

    # while True:
    #     if time.gmtime(
    #             time.time()).tm_hour == 0 and time.gmtime(
    #             time.time()).tm_min == 0:
    #         logging.info(get_time() + " - Starting program")
    #         g = Github(os.environ['GITHUB_TOKEN'])
    #         repo = g.get_repo("owid/covid-19-data")
    #         main()
    #         commit_message = "Update data"
    #         repo.update_file(contents.path, "Update data", data,
    #                          contents.sha, branch="master")
    #         logging.info(get_time() + " - Program finished")
