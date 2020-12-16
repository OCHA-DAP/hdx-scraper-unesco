#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for UNESCO.

'''
import os
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir

from unesco import download_indicatorsets, get_countriesdata, generate_dataset_and_showcase


class TestUNESCO:
    headers = ['INDICATOR_ID', 'COUNTRY_ID', 'YEAR', 'VALUE', 'MAGNITUDE', 'QUALIFIER']
    indheaders = ['INDICATOR_ID', 'INDICATOR_LABEL_EN']

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = {'sustainable development goals': {'Action to Take': 'merge', 'New Tag(s)': 'sustainable development goals - sdg'}}
        Vocabulary._approved_vocabulary = {'tags': [{'name': 'hxl'}, {'name': 'indicators'}, {'name': 'health'}, {'name': 'demographics'}, {'name': 'sustainable development goals - sdg'}], 'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='function')
    def mock_urlretrieve(self):
        def myurlretrieve(url, path):
            class Headers:
                @staticmethod
                def get_content_type():
                    return 'application/zip'

            return path, Headers()
        return myurlretrieve

    def test_download_indicatorsets(self, mock_urlretrieve, configuration):
        with temp_dir('TestUNESCO') as folder:
            configuration = Configuration.read()
            result = download_indicatorsets(configuration['base_url'], folder, configuration['indicatorsetcodes'], urlretrieve=mock_urlretrieve)
            assert result == {'DEM': join(folder, 'DEM.zip'), 'NATMON': join(folder, 'NATMON.zip'), 'SDG': join(folder, 'SDG.zip')}

    def test_get_countriesdata(self):
        indicatorsets = {'NATMON': join('tests', 'fixtures', 'NATMON.zip')}
        with temp_dir('TestUNESCO') as folder:
            with Download(user_agent='test') as downloader:
                result = get_countriesdata(indicatorsets, downloader, folder)
                countries, indheaders, indicatorsetsindicators, indicatorsetsdates, datafiles = result
                assert len(countries) == 238
                assert countries[9] == {'countryname': 'Armenia', 'iso2': 'AM', 'iso3': 'ARM'}
                assert indheaders == TestUNESCO.indheaders
                assert indicatorsets == {'NATMON': 'tests/fixtures/NATMON.zip'}
                assert len(indicatorsetsindicators['NATMON']['rows']) == 1055
                assert indicatorsetsindicators['NATMON']['rows'][80] == {'INDICATOR_ID': '26442', 'INDICATOR_LABEL_EN': 'Africa: Students from Ghana, both sexes (number)'}
                assert len(indicatorsetsindicators['NATMON']['shortnames']) == 246
                assert sorted(indicatorsetsindicators['NATMON']['shortnames'])[40] == 'Enrolment in early childhood education'
                assert indicatorsetsdates == {'NATMON': '2020 September'}
                assert datafiles == {'NATMON': (join(os.sep, 'tmp', 'TestUNESCO',  'NATMON_METADATA.csv'), join(os.sep, 'tmp', 'TestUNESCO',  'NATMON_DATA_NATIONAL.csv'))}

    def test_generate_dataset_and_showcase(self, configuration):
        configuration = Configuration.read()
        indicatorsetcodes = {'NATMON': configuration['indicatorsetcodes']['NATMON']}
        with temp_dir('TestUNESCO', delete_on_failure=False) as folder:
            with Download(user_agent='test') as downloader:
                country = {'iso3': 'AFG', 'iso2': 'AF', 'countryname': 'Afghanistan'}
                indicators = [{'INDICATOR_ID': 'GER.1t3', 'INDICATOR_LABEL_EN': 'Gross enrolment ratio, primary and secondary, both sexes (number)'},
                              {'INDICATOR_ID': 'XGDP.1.FSgov', 'INDICATOR_LABEL_EN': 'Government expenditure on primary education, both sexes (number)'},
                              {'INDICATOR_ID': 'XGDP.2.FSgov', 'INDICATOR_LABEL_EN': 'Government expenditure on lower secondary education, both sexes (number)'}]
                shortnames = {'Gross enrolment ratio, primary and secondary', 'Government expenditure on primary education',
                              'Government expenditure on lower secondary education'}
                indicatorsetsindicators = {'NATMON': {'rows': indicators, 'shortnames': shortnames}}
                datafiles = {'NATMON': (join('tests', 'fixtures',  'NATMON_METADATA.csv'), join('tests', 'fixtures',  'NATMON_DATA_NATIONAL.csv'))}
                dataset, showcase, bites_disabled, qc_indicators = generate_dataset_and_showcase(
                    indicatorsetcodes, TestUNESCO.indheaders, indicatorsetsindicators,
                    {'NATMON': '2020 September'}, country, datafiles, downloader, folder)
                assert dataset == {'name': 'unesco-data-for-afghanistan', 'title': 'Afghanistan - Education Indicators',
                                   'maintainer': 'a5c5296a-3206-4e51-b2de-bfe34857185f', 'owner_org': '18f2d467-dcf8-4b7e-bffa-b3c338ba3a7c',
                                   'data_update_frequency': '90', 'subnational': '0', 'groups': [{'name': 'afg'}],
                                   'tags': [{'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                                   'dataset_date': '01/01/1970-12/31/2020',
                                   'notes': "Education indicators for Afghanistan.\n\nContains data from the UNESCO Institute for Statistics [bulk data service](http://data.uis.unesco.org) covering the following categories: National Monitoring (made 2020 September)"}

                resources = dataset.get_resources()
                assert resources == [{'name': 'National Monitoring data', 'description': 'National Monitoring data with HXL tags.\n\nIndicators: Government expenditure on lower secondary education, Government expenditure on primary education, Gross enrolment ratio, primary and secondary', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                     {'name': 'National Monitoring indicator list', 'description': 'National Monitoring indicator list with HXL tags', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                     {'name': 'National Monitoring metadata', 'description': 'National Monitoring metadata with HXL tags',
                                      'format': 'csv',

                                      'resource_type': 'file.upload',
                                      'url_type': 'upload'},

                                     {'name': 'QuickCharts-National Monitoring data', 'description': 'Cut down data for QuickCharts', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]

                assert showcase == {'name': 'unesco-data-for-afghanistan-showcase', 'title': 'Afghanistan - Education Indicators',
                                    'notes': 'Education indicators for Afghanistan', 'url': 'http://uis.unesco.org/en/country/AF', 'image_url': 'http://www.tellmaps.com/uis/internal/assets/uisheader-en.png',
                                    'tags': [{'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

                assert bites_disabled == [False, False, False]
                assert qc_indicators == [{'code': 'GER.1t3', 'title': 'Gross enrolment ratio, primary and secondary', 'unit': 'Percentage (%)'},
                                         {'code': 'XGDP.1.FSgov', 'title': 'Government expenditure on primary education', 'unit': 'Percentage of GDP (%)'},
                                         {'code': 'XGDP.2.FSgov', 'title': 'Government expenditure on lower secondary education', 'unit': 'Percentage of GDP (%)'}]
                file = 'NATMON_data_AFG.csv'
                assert_files_same(join('tests', 'fixtures', file), join(folder, file))
                file = 'qc_NATMON_data_AFG.csv'
                assert_files_same(join('tests', 'fixtures', file), join(folder, file))
                file = 'NATMON_indicatorlist_AFG.csv'
                assert_files_same(join('tests', 'fixtures', file), join(folder, file))
                file = 'NATMON_metadata_AFG.csv'
                assert_files_same(join('tests', 'fixtures', file), join(folder, file))
