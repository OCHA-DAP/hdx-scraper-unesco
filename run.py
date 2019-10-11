#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os import getenv, makedirs
from os.path import join, expanduser, exists
from shutil import rmtree
from timeit import default_timer

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import get_temp_dir

from unesco import generate_dataset_and_showcase, get_countries, get_endpoints_metadata

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-unesco'


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    tmp = get_temp_dir()
    folder = join(tmp, 'UNESCO')
    if not exists(folder):
        makedirs(folder)
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as downloader:
        endpoints = Configuration.read()['endpoints']
        endpoints_metadata = get_endpoints_metadata(base_url, downloader, endpoints)
        countries = get_countries(base_url, downloader)
        progress_file = join(folder, 'progress.txt')

        logger.info('Number of datasets to upload: %d' % len(countries))

        startiso3 = getenv('STARTISO3')
        if startiso3:
            startiso3 = startiso3.upper()
            if startiso3 == 'RESET':
                rmtree(folder)
                makedirs(folder)
                startiso3 = None
                logger.info('Removing progress file. Scraper will start from beginning!')
            else:
                logger.info('Environment variable STARTISO3 = %s' % startiso3)
        else:
            if exists(progress_file):
                with open(progress_file, 'r') as f:
                    startiso3 = f.read(3)
                    logger.info('File STARTISO3 = %s' % startiso3)
        found = False
        for countryiso3, countryiso2, countryname in countries:
            if startiso3 and not found:
                if countryiso3 == startiso3:
                    found = True
                    logger.info('Starting run from STARTISO3 %s (%s)' % (countryiso3, countryname))
                else:
                    logger.info('Run not started. Ignoring %s. STARTISO3 not matched: %s!=%s' % (countryname, countryiso3, startiso3))
                    continue
            logger.info('Adding datasets for %s (%s)' % (countryname, countryiso3))
            with open(progress_file, 'w') as f:
                f.write(countryiso3)
            for dataset, showcase in generate_dataset_and_showcase(downloader, countryiso3, countryiso2, countryname, endpoints_metadata, folder=folder, merge_resources=True, single_dataset=False):
                if dataset:
                    dataset.update_from_yaml()
                    start = default_timer()
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False)
                    logger.info('total time = %d' % (default_timer() - start))
                    resources = dataset.get_resources()
                    resource_ids = [x['id'] for x in sorted(resources, key=lambda x: x['name'], reverse=False)]
                    dataset.reorder_resources(resource_ids, hxl_update=False)
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)
    logger.info('UNESCO scraper completed!')
    rmtree(folder)


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

