#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import argparse
import logging
import sys
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import get_temp_dir, progress_storing_tempdir, wheretostart_tempdir_batch, \
    progress_storing_folder

from unesco import download_indicatorsets, get_countriesdata, generate_dataset_and_showcase

from hdx.facades.keyword_arguments import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-unesco'


def main(test=False, **ignore):
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with Download() as downloader:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info['folder']
            batch = info['batch']
            indicatorsetcodes = Configuration.read()['indicatorsetcodes']
            if test:
                newindicatorsetcodes = dict()
                for key in indicatorsetcodes:
                    if key == 'NATMON':
                        newindicatorsetcodes[key] = indicatorsetcodes[key]
                        break
                indicatorsetcodes = newindicatorsetcodes
            indicatorsets = download_indicatorsets(base_url, folder, indicatorsetcodes)
            logger.info('Number of indicator types to upload: %d' % len(indicatorsets))
            countries, indheaders, indicatorsetsindicators, indicatorsetsdates, datafiles = \
                get_countriesdata(indicatorsets, downloader, folder)
            if test:
                countries = [{'iso3': 'AFG', 'iso2': 'AF', 'countryname': 'Afghanistan'}]
            logger.info('Number of countries to upload: %d' % len(countries))
            for info, country in progress_storing_folder(info, countries, 'iso3'):
                dataset, showcase, bites_disabled, qc_indicators = generate_dataset_and_showcase(
                    indicatorsetcodes, indheaders, indicatorsetsindicators, indicatorsetsdates, country, datafiles,
                    downloader, info['folder'])
                if dataset:
                    dataset.update_from_yaml()
                    dataset.generate_resource_view(-1, bites_disabled=bites_disabled, indicators=qc_indicators)
                    ordered_resource_names = [x['name'] for x in dataset.get_resources()]
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: UNESCO', batch=batch)
                    sorted_resources = sorted(dataset.get_resources(), key=lambda x: ordered_resource_names.index(x['name']))
                    dataset.reorder_resources([x['id'] for x in sorted_resources], hxl_update=True)
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)
                    if test:
                        sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='unesco')
    parser.add_argument('-t', '--test', default=False, action='store_true', help='Generate test data')
    args = parser.parse_args()
    facade(main, hdx_site='stage', user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'), test=args.test)

