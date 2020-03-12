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


def get_countriesdata(indicatorsets, downloader):
    countries = list()
    countriesdata = dict()
    indicatorsetsdates = dict()
    indicatorsetsindicators = dict()
    for indicatorsetcode in indicatorsets:
        # if indicatorsetcode != 'EDUN':   FOR GENERATING TEST DATA
        #     continue
        path = indicatorsets[indicatorsetcode]
        indfile = None
        datafile = None
        with ZipFile(path, 'r') as zip:
            for filename in zip.namelist():
                if 'README' in filename:
                    fuzzy = dict()
                    parse_date_range(filename.replace('_', ' '), fuzzy=fuzzy)
                    indicatorsetsdates[indicatorsetcode] = ''.join(fuzzy['date'])
                if 'LABEL' in filename:
                    indfile = filename
                if 'DATA_NATIONAL' in filename:
                    datafile = filename
            if datafile is None:
                raise(IOError('No data file in zip!'))
            if indfile is None:
                raise(IOError('No indicator file in zip!'))
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

            headers, iterator = downloader.get_tabular_rows(zip.open(datafile), headers=1, dict_form=True, format='csv', encoding='WINDOWS-1252')
            for row in iterator:
                countryiso = row['COUNTRY_ID']
                countrydata = countriesdata.get(countryiso)
                if countrydata is None:
                    countrydata = dict()
                    countriesdata[countryiso] = countrydata
                    iso2 = Country.get_iso2_from_iso3(countryiso)
                    countries.append({'iso3': countryiso, 'iso2': iso2, 'countryname': Country.get_country_name_from_iso3(countryiso)})
#                if row['INDICATOR_ID'] not in ['20082', '20122', '26375']:  FOR GENERATING TEST DATA
#                    continue
                dict_of_lists_add(countrydata, indicatorsetcode, row)
    return countries, headers, countriesdata, indheaders, indicatorsetsindicators, indicatorsetsdates


def generate_dataset_and_showcase(folder, indicatorsetcodes, indheaders, indicatorsetsindicators,
                                  indicatorsetsdates, country, headers, countrydata):
    countryiso = country['iso3']
    countryname = country['countryname']
    title = '%s - Education Indicators' % countryname
    slugified_name = slugify('UNESCO data for %s' % countryname).lower()
    logger.info('Creating dataset: %s' % title)
    dataset = Dataset({
        'name': slugified_name,
        'title': title
    })
    dataset.set_maintainer('196196be-6037-4488-8b71-d786adf4c081')
    dataset.set_organization('18f2d467-dcf8-4b7e-bffa-b3c338ba3a7c')
    dataset.set_expected_update_frequency('Every three months')
    dataset.set_subnational(False)
    dataset.add_country_location(country['iso3'])
    tags = ['sustainable development', 'demographics', 'socioeconomics', 'education', 'indicators', 'hxl']
    dataset.add_tags(tags)

    bites_disabled = None
    categories = list()
    for indicatorsetcode in indicatorsetcodes:
        indicatorsetname = indicatorsetcodes[indicatorsetcode]['title']
        if indicatorsetcode not in countrydata:
            continue
        indicatorsetindicators = indicatorsetsindicators[indicatorsetcode]
        indicator_names = indicatorsetindicators['shortnames']
        filename = '%s_%s.csv' % (indicatorsetcode, countryiso)
        resourcedata = {
            'name': '%s data' % indicatorsetname,
            'description': '%s data with HXL tags.\n\nIndicators: %s' % (indicatorsetname, ', '.join(sorted(indicator_names)))
        }
        indicators_for_qc = indicatorsetcodes[indicatorsetcode].get('quickcharts')
        if indicators_for_qc:
            quickcharts = {'hashtag': '#indicator+code', 'values': indicators_for_qc, 'cutdown': 2,
                           'cutdownhashtags': ['#indicator+code', '#country+code', '#date+year', '#indicator+value+num']}
        else:
            quickcharts = None
        success, results = dataset.generate_resource_from_download(
            headers, countrydata[indicatorsetcode], hxltags, folder, filename, resourcedata, yearcol='YEAR', quickcharts=quickcharts)
        if success is False:
            logger.warning('%s for %s has no data!' % (indicatorsetname, countryname))
            continue
        bites_disabled = results['bites_disabled']
        filename = '%s_indicatorlist.csv' % indicatorsetcode
        resourcedata = {
            'name': '%s indicator list' % indicatorsetname,
            'description': '%s indicator list with HXL tags' % indicatorsetname
        }
        indicators = indicatorsetindicators['rows']
        success, _ = dataset.generate_resource_from_download(
            indheaders, indicators, hxltags, folder, filename, resourcedata)
        if success is False:
            logger.warning('%s for %s has no data!' % (indicatorsetname, countryname))
            continue
        categories.append('%s (made on %s)' % (indicatorsetname, indicatorsetsdates[indicatorsetcode]))
    resources = dataset.get_resources()
    if len(resources) == 0:
        logger.warning('%s has no data!' % countryname)
        return None, None, None
    for i, resource in enumerate(resources):
        if resource['name'][:12] == 'QuickCharts-':
            resources.append(resources.pop(i))
    notes = ['Education indicators for %s.\n\n' % countryname,
             "Contains data from bulk download zips from UNESCO's [data portal](http://uis.unesco.org/) covering ",
             'the following categories: %s' % ', '.join(categories)]
    dataset['notes'] = ''.join(notes)

    showcase = Showcase({
        'name': '%s-showcase' % slugified_name,
        'title': title,
        'notes': 'Education indicators for %s' % countryname,
        'url': 'http://uis.unesco.org/en/country/%s' % country['iso2'],
        'image_url': 'http://www.tellmaps.com/uis/internal/assets/uisheader-en.png'
    })
    showcase.add_tags(tags)

    return dataset, showcase, bites_disabled



