#!/usr/bin/python
"""
UNESCO:
------

Reads UNESCO bulk files and creates datasets.

"""

import logging
import re
from os import remove, rename
from os.path import exists, join, split
from shutil import copyfileobj
from urllib.request import urlretrieve
from zipfile import ZipFile

from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import default_date, default_enddate, parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add, dict_of_sets_add

logger = logging.getLogger(__name__)

hxltags = {
    "indicator_id": "#indicator+code",
    "indicator_label_en": "#indicator+name",
    "country_id": "#country+code",
    "year": "#date+year",
    "value": "#indicator+value+num",
    "type": "#description+type",
    "metadata": "#description",
}


def download_indicatorsets(
    base_url, folder, indicatorsetcodes, urlretrieve=urlretrieve
):
    indicatorsets = dict()
    for indicatorsetcode in indicatorsetcodes:
        filename = f"{indicatorsetcode}.zip"
        path = join(folder, filename)
        statusfile = join(folder, f"{indicatorsetcode}.txt")
        if exists(path):
            if exists(statusfile):
                with open(statusfile) as f:
                    status = f.read()
                    if status == "OK":
                        indicatorsets[indicatorsetcode] = path
                        continue
                remove(statusfile)
            remove(path)
        url = f"{base_url}{filename}"
        path, headers = urlretrieve(url, path)
        if "zip" not in headers.get_content_type():
            raise OSError(f"Problem with {path}!")
        with open(statusfile, "w") as f:
            f.write("OK")
            indicatorsets[indicatorsetcode] = path
    return indicatorsets


def get_filepath(zipfile, inputfile, outputfolder, indicatorsetcode):
    folder, filename = split(inputfile)
    origfolder = join(outputfolder, indicatorsetcode)
    if folder:
        zipfolder = outputfolder
    else:
        zipfolder = origfolder
    inputpath = zipfile.extract(inputfile, path=zipfolder)
    origpath = join(origfolder, f"orig_{filename}")

    rename(inputpath, origpath)
    with open(origpath, mode="rt") as inputfp:
        line = inputfp.readline().lower()
        with open(inputpath, "w") as outputfp:
            outputfp.write(line)
            copyfileobj(inputfp, outputfp)
    return inputpath


def get_countriesdata(indicatorsets, downloader, folder):
    indheaders = None
    countriesset = set()
    datafiles = dict()
    indicatorsetsdates = dict()
    indicatorsetsindicators = dict()
    for indicatorsetcode in indicatorsets:
        path = indicatorsets[indicatorsetcode]
        indfile = None
        cntfile = None
        metadatafile = None
        datafile = None
        with ZipFile(path, "r") as zipfile:
            for filename in zipfile.namelist():
                if "README" in filename:
                    fuzzy = dict()
                    parse_date_range(filename.replace("_", " "), fuzzy=fuzzy)
                    indicatorsetsdates[indicatorsetcode] = "".join(fuzzy["date"])
                if "LABEL" in filename:
                    indfile = filename
                if "COUNTRY" in filename:
                    cntfile = filename
                if "METADATA" in filename:
                    metadatafile = filename
                if "DATA_NATIONAL" in filename:
                    datafile = filename
            if datafile is None:
                raise (OSError("No data file in zip!"))
            if indfile is None:
                raise (OSError("No indicator file in zip!"))
            if cntfile is None:
                raise (OSError("No country file in zip!"))
            indpath = get_filepath(zipfile, indfile, folder, indicatorsetcode)
            indheaders, iterator = downloader.get_tabular_rows(
                indpath,
                headers=1,
                dict_form=True,
                format="csv",
                encoding="WINDOWS-1252",
            )
            indicatorsetindicators = indicatorsetsindicators.get(
                indicatorsetcode, dict()
            )
            for row in iterator:
                dict_of_lists_add(indicatorsetindicators, "rows", row)
                indicator_name = row["indicator_label_en"]
                ind0 = re.sub(r"\s+", " ", indicator_name)
                ind1, _, _ = ind0.partition(",")
                ind2, _, _ = ind1.partition("(")
                indicator_name, _, _ = ind2.partition(":")
                dict_of_sets_add(
                    indicatorsetindicators, "shortnames", indicator_name.strip()
                )
            indicatorsetsindicators[indicatorsetcode] = indicatorsetindicators

            cntpath = get_filepath(zipfile, cntfile, folder, indicatorsetcode)
            _, iterator = downloader.get_tabular_rows(
                cntpath, headers=1, dict_form=True, format="csv"
            )
            for row in iterator:
                countriesset.add(row["country_id"])

            if metadatafile:
                metadatapath = get_filepath(
                    zipfile, metadatafile, folder, indicatorsetcode
                )
            else:
                metadatapath = None
            datapath = get_filepath(zipfile, datafile, folder, indicatorsetcode)
            datafiles[indicatorsetcode] = (metadatapath, datapath)
    countries = list()
    for countryiso in sorted(list(countriesset)):
        iso2 = Country.get_iso2_from_iso3(countryiso)
        countryname = Country.get_country_name_from_iso3(countryiso)
        if iso2 is None or countryname is None:
            continue
        countries.append({"iso3": countryiso, "iso2": iso2, "countryname": countryname})
    return countries, indheaders, indicatorsetsindicators, indicatorsetsdates, datafiles


def generate_dataset_and_showcase(
    indicatorsetcodes,
    indheaders,
    indicatorsetsindicators,
    indicatorsetsdates,
    country,
    datafiles,
    downloader,
    folder,
):
    countryiso = country["iso3"]
    countryname = country["countryname"]
    title = f"{countryname} - Education Indicators"
    slugified_name = slugify(f"UNESCO data for {countryname}").lower()
    logger.info(f"Creating dataset: {title}")
    dataset = Dataset({"name": slugified_name, "title": title})

    dataset.set_maintainer("a5c5296a-3206-4e51-b2de-bfe34857185f")
    dataset.set_organization("18f2d467-dcf8-4b7e-bffa-b3c338ba3a7c")
    dataset.set_expected_update_frequency("Never")
    dataset.set_subnational(False)
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception(f"{countryname} has a problem! {e}")
        return None, None, None
    tags = [
        "sustainable development",
        "demographics",
        "socioeconomics",
        "education",
        "indicators",
        "sustainable development goals-sdg",
        "hxl",
    ]
    dataset.add_tags(tags)

    earliest_start_date = default_enddate
    latest_end_date = default_date

    def process_row(headers, row):
        nonlocal earliest_start_date, latest_end_date
        if row["country_id"] != countryiso:
            return None
        year = row["year"]
        if year:
            startdate, enddate = parse_date_range(
                year,
                zero_time=True,
                max_endtime=True,
            )
            if startdate < earliest_start_date:
                earliest_start_date = startdate
            if enddate > latest_end_date:
                latest_end_date = enddate
        return row

    def process_metadata_row(headers, row):
        if row["country_id"] == countryiso:
            return row
        else:
            return None

    categories = list()
    bites_disabled = None
    qc_indicators = None

    for indicatorsetcode in indicatorsetcodes:
        indicatorsetname = indicatorsetcodes[indicatorsetcode]["title"]
        metadatafile, datafile = datafiles[indicatorsetcode]
        indicatorsetindicators = indicatorsetsindicators[indicatorsetcode]
        indicator_names = indicatorsetindicators["shortnames"]
        filename = f"{indicatorsetcode}_data_{countryiso}.csv"
        resourcename = f"{indicatorsetname} data"
        resourcedata = {
            "name": resourcename,
            "description": f"{indicatorsetname} data with HXL tags.\n\nIndicators: {', '.join(sorted(indicator_names))}",
        }
        indicators_for_qc = indicatorsetcodes[indicatorsetcode].get("quickcharts")
        if indicators_for_qc:
            values = [x["code"] for x in indicators_for_qc]
            quickcharts = {
                "hashtag": "#indicator+code",
                "values": values,
                "numeric_hashtag": "#indicator+value+num",
                "cutdown": 2,
                "cutdownhashtags": ["#indicator+code", "#country+code", "#date+year"],
            }
            qc_indicators = indicators_for_qc
        else:
            quickcharts = None
        outputfolder = join(folder, indicatorsetcode)
        success, results = dataset.download_and_generate_resource(
            downloader,
            datafile,
            hxltags,
            outputfolder,
            filename,
            resourcedata,
            row_function=process_row,
            quickcharts=quickcharts,
        )
        if success is False:
            logger.warning(f"{resourcename} for {countryname} has no data!")
            continue
        disabled_bites = results.get("bites_disabled")
        if disabled_bites:
            bites_disabled = disabled_bites
        filename = f"{indicatorsetcode}_indicatorlist_{countryiso}.csv"
        resourcename = f"{indicatorsetname} indicator list"
        resourcedata = {
            "name": resourcename,
            "description": f"{indicatorsetname} indicator list with HXL tags",
        }
        indicators = indicatorsetindicators["rows"]
        success, _ = dataset.generate_resource_from_iterable(
            indheaders, indicators, hxltags, outputfolder, filename, resourcedata
        )
        if success is False:
            logger.warning(f"{resourcename} for {countryname} has no data!")
            continue
        categories.append(
            f"{indicatorsetname} (made {indicatorsetsdates[indicatorsetcode]})"
        )
        if metadatafile:
            filename = f"{indicatorsetcode}_metadata_{countryiso}.csv"
            resourcename = f"{indicatorsetname} metadata"
            resourcedata = {
                "name": resourcename,
                "description": f"{indicatorsetname} metadata with HXL tags",
            }
            success, results = dataset.download_and_generate_resource(
                downloader,
                metadatafile,
                hxltags,
                outputfolder,
                filename,
                resourcedata,
                row_function=process_metadata_row,
            )
            if success is False:
                logger.warning(f"{resourcename} for {countryname} has no data!")
                continue
    if dataset.number_of_resources() == 0:
        logger.warning(f"{countryname} has no data!")
        return None, None, None, None
    dataset.set_time_period(earliest_start_date, latest_end_date)
    dataset.quickcharts_resource_last()
    notes = [
        f"Education indicators for {countryname}.\n\n",
        "Contains data from the UNESCO Institute for Statistics [bulk data service](http://data.uis.unesco.org) ",
        f"covering the following categories: {', '.join(categories)}",
    ]
    dataset["notes"] = "".join(notes)

    showcase = Showcase(
        {
            "name": f"{slugified_name}-showcase",
            "title": title,
            "notes": f"Education indicators for {countryname}",
            "url": f"https://uis.unesco.org/en/country/{country['iso2']}",
            "image_url": "https://tcg.uis.unesco.org/wp-content/uploads/sites/4/2021/09/combined_uis_colors_eng-002-300x240.png",
        }
    )
    showcase.add_tags(tags)

    return dataset, showcase, bites_disabled, qc_indicators
