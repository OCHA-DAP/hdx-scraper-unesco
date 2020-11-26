#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
UNESCO:
------

Reads UNESCO bulk files and creates datasets.

"""
import logging
import re
from os import remove
from os.path import join, exists
from urllib.request import urlretrieve
from zipfile import ZipFile

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date_range
from hdx.utilities.dictandlist import dict_of_lists_add, dict_of_sets_add
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {'INDICATOR_ID': '#indicator+code', 'INDICATOR_LABEL_EN': '#indicator+name', 'COUNTRY_ID': '#country+code', 'YEAR': '#date+year', 'VALUE': '#indicator+value+num'}


def download_indicatorsets(base_url, folder, indicatorsetcodes, urlretrieve=urlretrieve):
    indicatorsets = dict()
    for indicatorsetcode in indicatorsetcodes:
        filename = '%s.zip' % indicatorsetcode
        path = join(folder, filename)
        statusfile = join(folder, '%s.txt' % indicatorsetcode)
        if exists(path):
            if exists(statusfile):
                with open(statusfile) as f:
                    status = f.read()
                    if status == 'OK':
                        indicatorsets[indicatorsetcode] = path
                        continue
                remove(statusfile)
            remove(path)
        url = '%s/%s' % (base_url, filename)
        path, headers = urlretrieve(url, path)
        if headers.get_content_type() != 'application/zip':
            raise IOError('Problem with %s!' % path)
        with open(statusfile, 'w') as f:
            f.write('OK')
            indicatorsets[indicatorsetcode] = path
    return indicatorsets


def get_countriesdata(indicatorsets, downloader, folder):
    countriesset = set()
    datafiles = dict()
    indicatorsetsdates = dict()
    indicatorsetsindicators = dict()
    for indicatorsetcode in indicatorsets:
        # if indicatorsetcode != 'NATMON':   FOR GENERATING TEST DATA
        #     continue
        path = indicatorsets[indicatorsetcode]
        indfile = None
        cntfile = None
        datafile = None
        with ZipFile(path, 'r') as zip:
            for filename in zip.namelist():
                if 'README' in filename:
                    fuzzy = dict()
                    parse_date_range(filename.replace('_', ' '), fuzzy=fuzzy)
                    indicatorsetsdates[indicatorsetcode] = ''.join(fuzzy['date'])
                if 'LABEL' in filename:
                    indfile = filename
                if 'COUNTRY' in filename:
                    cntfile = filename
                if 'DATA_NATIONAL' in filename:
                    datafile = filename
            if datafile is None:
                raise(IOError('No data file in zip!'))
            if indfile is None:
                raise(IOError('No indicator file in zip!'))
            if cntfile is None:
                raise(IOError('No country file in zip!'))
            indheaders, iterator = downloader.get_tabular_rows(zip.open(indfile), headers=1, dict_form=True,
                                                               format='csv', encoding='WINDOWS-1252')
            indicatorsetindicators = indicatorsetsindicators.get(indicatorsetcode, dict())
            for row in iterator:
                dict_of_lists_add(indicatorsetindicators, 'rows', row)
                indicator_name = row['INDICATOR_LABEL_EN']
                ind0 = re.sub(r'\s+', ' ', indicator_name)
                ind1, _, _ = ind0.partition(',')
                ind2, _, _ = ind1.partition('(')
                indicator_name, _, _ = ind2.partition(':')
                dict_of_sets_add(indicatorsetindicators, 'shortnames', indicator_name.strip())
            indicatorsetsindicators[indicatorsetcode] = indicatorsetindicators

            _, iterator = downloader.get_tabular_rows(zip.open(cntfile), headers=1, dict_form=True,
                                                               format='csv')
            for row in iterator:
                countriesset.add(row['COUNTRY_ID'])

            path = zip.extract(datafile, path=folder)
            datafiles[indicatorsetcode] = path
        countries = list()
        for countryiso in sorted(list(countriesset)):
            iso2 = Country.get_iso2_from_iso3(countryiso)
            countryname = Country.get_country_name_from_iso3(countryiso)
            if iso2 is None or countryname is None:
                continue
            countries.append({'iso3': countryiso, 'iso2': iso2, 'countryname': countryname})
    return countries, indheaders, indicatorsetsindicators, indicatorsetsdates, datafiles


def generate_dataset_and_showcase(indicatorsetcodes, indheaders, indicatorsetsindicators,
                                  indicatorsetsdates, country, datafiles, downloader, folder):
    countryiso = country['iso3']
    countryname = country['countryname']
    title = '%s - Education Indicators' % countryname
    slugified_name = slugify('UNESCO data for %s' % countryname).lower()
    logger.info('Creating dataset: %s' % title)
    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })

    dataset.set_maintainer('a5c5296a-3206-4e51-b2de-bfe34857185f')
    dataset.set_organization('18f2d467-dcf8-4b7e-bffa-b3c338ba3a7c')
    dataset.set_expected_update_frequency('Every three months')
    dataset.set_subnational(False)
    try:
        dataset.add_country_location(countryiso)
    except HDXError as e:
        logger.exception('%s has a problem! %s' % (countryname, e))
        return None, None, None
    tags = ['sustainable development', 'demographics', 'socioeconomics', 'education', 'indicators', 'hxl']
    dataset.add_tags(tags)

    def process_row(headers, row):
        #                if row['INDICATOR_ID'] not in ['20082', '20122', '26375']:  FOR GENERATING TEST DATA
        #                    continue
        if row['COUNTRY_ID'] == countryiso:
            return row
        else:
            return None

    categories = list()
    bites_disabled = None
    qc_indicators = None
    for indicatorsetcode in indicatorsetcodes:
        indicatorsetname = indicatorsetcodes[indicatorsetcode]['title']
        datafile = datafiles[indicatorsetcode]
        indicatorsetindicators = indicatorsetsindicators[indicatorsetcode]
        indicator_names = indicatorsetindicators['shortnames']
        filename = '%s_%s.csv' % (indicatorsetcode, countryiso)
        resourcedata = {
            'name': '%s data' % indicatorsetname,
            'description': '%s data with HXL tags.\n\nIndicators: %s' % (indicatorsetname, ', '.join(sorted(indicator_names)))
        }
        indicators_for_qc = indicatorsetcodes[indicatorsetcode].get('quickcharts')
        if indicators_for_qc:
            values = [x['code'] for x in indicators_for_qc]
            quickcharts = {'hashtag': '#indicator+code', 'values': values, 'numeric_hashtag': '#indicator+value+num',
                           'cutdown': 2, 'cutdownhashtags': ['#indicator+code', '#country+code', '#date+year']}
            qc_indicators = indicators_for_qc
        else:
            quickcharts = None
        success, results = dataset.download_and_generate_resource(
            downloader, datafile, hxltags, folder, filename, resourcedata, row_function=process_row, yearcol='YEAR',
            quickcharts=quickcharts, encoding='WINDOWS-1252')
        if success is False:
            logger.warning('%s for %s has no data!' % (indicatorsetname, countryname))
            continue
        disabled_bites = results.get('bites_disabled')
        if disabled_bites:
            bites_disabled = disabled_bites
        filename = '%s_indicatorlist.csv' % indicatorsetcode
        resourcedata = {
            'name': '%s indicator list' % indicatorsetname,
            'description': '%s indicator list with HXL tags' % indicatorsetname
        }
        indicators = indicatorsetindicators['rows']
        success, _ = dataset.generate_resource_from_iterator(
            indheaders, indicators, hxltags, folder, filename, resourcedata)
        if success is False:
            logger.warning('%s for %s has no data!' % (indicatorsetname, countryname))
            continue
        categories.append('%s (made %s)' % (indicatorsetname, indicatorsetsdates[indicatorsetcode]))
    if dataset.number_of_resources() == 0:
        logger.warning('%s has no data!' % countryname)
        return None, None, None, None
    dataset.quickcharts_resource_last()
    notes = ['Education indicators for %s.\n\n' % countryname,
             'Contains data from the UNESCO Institute for Statistics [bulk data service](http://data.uis.unesco.org) ',
             'covering the following categories: %s' % ', '.join(categories)]
    dataset['notes'] = ''.join(notes)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title,
        'notes': 'Education indicators for %s' % countryname,
        'url': 'http://uis.unesco.org/en/country/%s' % country['iso2'],
        'image_url': 'http://www.tellmaps.com/uis/internal/assets/uisheader-en.png'
    })
    showcase.add_tags(tags)

    return dataset, showcase, bites_disabled, qc_indicators

