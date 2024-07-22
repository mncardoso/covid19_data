import urllib.request
import json
import time
import os
import git


url = "https://covid.ourworldindata.org/data/owid-covid-data.json"
path = "path_to_repo"


unwanted_keys = [
    "OWID_AFR", "OWID_ASI", "OWID_EUR", "OWID_EUN", "OWID_HIC", "OWID_INT",
    "OWID_KOS", "OWID_LIC", "OWID_LMC", "OWID_NAM", "OWID_CYN", "OWID_OCE",
    "OWID_SAM", "OWID_UMC"]


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

def create_data(countries, data):
    with open(path + "data/countries.json", "w") as outfile:
        json.dump(countries, outfile)

    for iso in countries.keys():
        with open(path + "data/" + iso + ".json", "w") as outfile:
            json.dump(data[iso], outfile)

def get_time():
    h = int(time.gmtime(time.time()).tm_hour)
    h = "0" + str(h) if h < 10 else str(h)

    m = int(time.gmtime(time.time()).tm_min)
    m = "0" + str(m) if m < 10 else str(m)

    s = int(time.gmtime(time.time()).tm_sec)
    s = "0" + str(s) if s < 10 else str(s)

    return h + ":" + m + ":" + s

def get_date():
    d = int(time.gmtime(time.time()).tm_mday)
    d = "0" + str(d) if d < 10 else str(d)

    m = int(time.gmtime(time.time()).tm_mon)
    m = "0" + str(m) if m < 10 else str(m)

    y = int(time.gmtime(time.time()).tm_year)

    return d + "." + m + "." + str(y)


if __name__ == "__main__":
    # git.Repo(path).remotes.origin.pull()
    raw_data = get_data(url)
    countries, data = parse_data(raw_data)
    create_data(countries, data)
    # commit_message = "bot update -> " + get_date() + " - " + get_time()
    # git.Repo(path).git.add(".")
    # git.Repo(path).git.commit("-m", commit_message)
    # git.Repo(path).git.push()
