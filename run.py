#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser
from timeit import default_timer

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_tempdir

from unesco import generate_dataset_and_showcase, get_countries, get_endpoints_metadata

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-unesco'


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as downloader:
        endpoints = Configuration.read()['endpoints']
        endpoints_metadata = get_endpoints_metadata(base_url, downloader, endpoints)
        countries = get_countries(base_url, downloader)
        logger.info('Number of datasets to upload: %d' % len(countries))

        for folder, country in progress_storing_tempdir('UNESCO', countries, 'iso3'):
            logger.info('Adding datasets for %s' % country['name'])
            for dataset, showcase in generate_dataset_and_showcase(downloader, country, endpoints_metadata, folder=folder, merge_resources=True, single_dataset=False):
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


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

