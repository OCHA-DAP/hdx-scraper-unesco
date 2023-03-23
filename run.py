#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import argparse
import logging
import sys
from os import getenv
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from unesco import (
    download_indicatorsets,
    generate_dataset_and_showcase,
    get_countriesdata,
)

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-unesco"


def main(base_url=None, test=False, **ignore):
    """Generate dataset and create it in HDX"""

    if base_url is None:
        raise ValueError("Must supply base_url ")
    with Download() as downloader:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            batch = info["batch"]
            indicatorsetcodes = Configuration.read()["indicatorsetcodes"]
            if test:
                newindicatorsetcodes = dict()
                for key in indicatorsetcodes:
                    if key == "NATMON":
                        newindicatorsetcodes[key] = indicatorsetcodes[key]
                        break
                indicatorsetcodes = newindicatorsetcodes
            indicatorsets = download_indicatorsets(base_url, folder, indicatorsetcodes)
            logger.info(f"Number of indicator types to upload: {len(indicatorsets)}")
            (
                countries,
                indheaders,
                indicatorsetsindicators,
                indicatorsetsdates,
                datafiles,
            ) = get_countriesdata(indicatorsets, downloader, folder)
            if test:
                countries = [
                    {"iso3": "AFG", "iso2": "AF", "countryname": "Afghanistan"}
                ]
            logger.info(f"Number of countries to upload: {len(countries)}")
            for info, country in progress_storing_folder(info, countries, "iso3"):
                (
                    dataset,
                    showcase,
                    bites_disabled,
                    qc_indicators,
                ) = generate_dataset_and_showcase(
                    indicatorsetcodes,
                    indheaders,
                    indicatorsetsindicators,
                    indicatorsetsdates,
                    country,
                    datafiles,
                    downloader,
                    info["folder"],
                )
                if dataset:
                    dataset.update_from_yaml()
                    dataset.generate_resource_view(
                        -1, bites_disabled=bites_disabled, indicators=qc_indicators
                    )
                    dataset.create_in_hdx(
                        match_resources_by_metadata=False,
                        remove_additional_resources=True,
                        match_resource_order=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: UNESCO",
                        batch=batch,
                    )
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)
                    if test:
                        sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="unesco")
    parser.add_argument("-bu", "--base_url", default=None, help="Base url to use")
    parser.add_argument(
        "-t", "--test", default=False, action="store_true", help="Generate test data"
    )
    args = parser.parse_args()
    base_url = args.base_url
    if base_url is None:
        base_url = getenv("BASE_URL")
        if base_url is None:
            base_url = "https://apimgmtstzgjpfeq2u763lag.blob.core.windows.net/content/MediaLibrary/bdds/"
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        base_url=base_url,
        test=args.test,
    )
