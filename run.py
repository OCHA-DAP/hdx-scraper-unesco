#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import get_temp_dir, progress_storing_tempdir

from unesco import download_indicatorsets, get_countriesdata, generate_dataset_and_showcase

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-unesco'


def main():
    """Generate dataset and create it in HDX"""

    base_url = Configuration.read()['base_url']
    with Download() as downloader:
        folder = get_temp_dir('UNESCO')
        indicatorsetcodes = Configuration.read()['indicatorsetcodes']
        indicatorsets = download_indicatorsets(base_url, folder, indicatorsetcodes)
        logger.info('Number of indicator types to upload: %d' % len(indicatorsets))
        countries, indheaders, indicatorsetsindicators, indicatorsetsdates, datafiles = \
            get_countriesdata(indicatorsets, downloader, folder)
        logger.info('Number of countries to upload: %d' % len(countries))
        for info, country in progress_storing_tempdir('UNESCO', countries, 'iso3'):
            dataset, showcase, bites_disabled, qc_indicators = generate_dataset_and_showcase(
                indicatorsetcodes, indheaders, indicatorsetsindicators, indicatorsetsdates, country, datafiles,
                downloader, info['folder'])
            if dataset:
                dataset.update_from_yaml()
                dataset.generate_resource_view(-1, bites_disabled=bites_disabled, indicators=qc_indicators)
                dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: UNESCO', batch=info['batch'])
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)


if __name__ == '__main__':
    facade(main, hdx_site='stage', user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))

