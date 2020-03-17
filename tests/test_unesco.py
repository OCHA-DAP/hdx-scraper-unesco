#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for UNESCO.

'''
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
        Locations.set_validlocations([{'name': 'cpv', 'title': 'Cape Verde'}])
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
            assert result == {'DEM': join(folder, 'DEM.zip'), 'EDUF': join(folder, 'EDUF.zip'),
                              'EDUN': join(folder, 'EDUN.zip'), 'EDUR': join(folder, 'EDUR.zip'),
                              'SDG': join(folder, 'SDG.zip')}

    def test_get_countriesdata(self):
        indicatorsets = {'EDUR': join('tests', 'fixtures', 'EDUR.zip')}
        with Download(user_agent='test') as downloader:
            result = get_countriesdata(indicatorsets, downloader)
            countries, headers, countriesdata, indheaders, indicatorsetsindicators, indicatorsetsdates = result
            assert len(countries) == 125
            assert countries[10] == {'iso3': 'TGO', 'iso2': 'TG', 'countryname': 'Togo'}
            assert headers == TestUNESCO.headers
            assert indheaders == TestUNESCO.indheaders
            assert indicatorsets == {'EDUR': 'tests/fixtures/EDUR.zip'}
            assert len(indicatorsetsindicators['EDUR']['rows']) == 328
            assert indicatorsetsindicators['EDUR']['rows'][80] == {'INDICATOR_ID': 'DIndT.2.GPV.Ag50p', 'INDICATOR_LABEL_EN': 'Dissimilarity index for teachers aged 50 and above, lower secondary general education'}
            assert len(indicatorsetsindicators['EDUR']['shortnames']) == 120
            assert sorted(indicatorsetsindicators['EDUR']['shortnames'])[40] == 'Dissimilarity index for teachers with ISCED level less than 3'
            assert indicatorsetsdates == {'EDUR': '2020 February'}

    def test_generate_dataset_and_showcase(self, configuration):
        configuration = Configuration.read()
        indicatorsetcodes = {'EDUN': configuration['indicatorsetcodes']['EDUN']}
        with temp_dir('TestUNESCO') as folder:
            country = {'iso3': 'CPV', 'iso2': 'AF', 'countryname': 'Cape Verde'}
            indicators = [{'INDICATOR_ID': '20082', 'INDICATOR_LABEL_EN': 'Enrolment in secondary education, both sexes (number)'},
                          {'INDICATOR_ID': '20122', 'INDICATOR_LABEL_EN': 'Teachers in secondary education, public institutions, both sexes (number)'},
                          {'INDICATOR_ID': '26375', 'INDICATOR_LABEL_EN': 'Graduates from tertiary education, both sexes (number)'}]
            shortnames = {'Enrolment in secondary education', 'Teachers in secondary education',
                          'Graduates from tertiary education'}
            indicatorsetsindicators = {'EDUN': {'rows': indicators, 'shortnames': shortnames}}
            countrydata = {'EDUN': [{'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1980', 'VALUE': '3129', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1981', 'VALUE': '3341', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2016', 'VALUE': '3141', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1984', 'VALUE': '4465', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2014', 'VALUE': '3150', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2008', 'VALUE': '61906', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2009', 'VALUE': '2646', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2007', 'VALUE': '2530', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1982', 'VALUE': '3691', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2004', 'VALUE': '49790', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2005', 'VALUE': '2241', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1988', 'VALUE': '6413', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1990', 'VALUE': '7866', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2002', 'VALUE': '1962', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2000', 'VALUE': '44715', 'MAGNITUDE': '', 'QUALIFIER': 'UIS_EST'}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2014', 'VALUE': '59823', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2005', 'VALUE': '52671', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2017', 'VALUE': '3200', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1994', 'VALUE': '14097', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1987', 'VALUE': '5675', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '26375', 'COUNTRY_ID': 'CPV', 'YEAR': '2018', 'VALUE': '1420', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2004', 'VALUE': '2193', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2001', 'VALUE': '44748', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2010', 'VALUE': '2675', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2008', 'VALUE': '2587', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2017', 'VALUE': '56469', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2018', 'VALUE': '53355', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2011', 'VALUE': '2787', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2015', 'VALUE': '58657', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2012', 'VALUE': '61956', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2010', 'VALUE': '61677', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2007', 'VALUE': '60783', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2003', 'VALUE': '2091', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2012', 'VALUE': '2910', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2001', 'VALUE': '1886', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '26375', 'COUNTRY_ID': 'CPV', 'YEAR': '2016', 'VALUE': '1383', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2016', 'VALUE': '57730', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2006', 'VALUE': '61465', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2015', 'VALUE': '3073', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2018', 'VALUE': '3099', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2013', 'VALUE': '62297', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2003', 'VALUE': '49522', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1989', 'VALUE': '6891', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2009', 'VALUE': '60769', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2011', 'VALUE': '62222', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '1972', 'VALUE': '4096', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20082', 'COUNTRY_ID': 'CPV', 'YEAR': '2002', 'VALUE': '48055', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '26375', 'COUNTRY_ID': 'CPV', 'YEAR': '2017', 'VALUE': '1133', 'MAGNITUDE': '', 'QUALIFIER': ''}, {'INDICATOR_ID': '20122', 'COUNTRY_ID': 'CPV', 'YEAR': '2013', 'VALUE': '2932', 'MAGNITUDE': '', 'QUALIFIER': ''}]}
            dataset, showcase, bites_disabled = generate_dataset_and_showcase(
                folder, indicatorsetcodes, TestUNESCO.indheaders, indicatorsetsindicators,
                {'EDUN': '2020 February'}, country, TestUNESCO.headers, countrydata)
            assert dataset == {'name': 'unesco-data-for-cape-verde', 'title': 'Cape Verde - Education Indicators',
                               'maintainer': '9d90f882-341d-4934-a55a-7a0ee7cc2f73', 'owner_org': '18f2d467-dcf8-4b7e-bffa-b3c338ba3a7c',
                               'data_update_frequency': '90', 'subnational': '0', 'groups': [{'name': 'cpv'}],
                               'tags': [{'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}],
                               'dataset_date': '01/01/1972-12/31/2018',
                               'notes': "Education indicators for Cape Verde.\n\nContains data from the UNESCO Institute for Statistics [bulk data service](http://data.uis.unesco.org) covering the following categories: Students and Teachers (made 2020 February)"}

            resources = dataset.get_resources()
            assert resources == [{'name': 'Students and Teachers data', 'description': 'Students and Teachers data with HXL tags.\n\nIndicators: Enrolment in secondary education, Graduates from tertiary education, Teachers in secondary education', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                 {'name': 'Students and Teachers indicator list', 'description': 'Students and Teachers indicator list with HXL tags', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'},
                                 {'name': 'QuickCharts-Students and Teachers data', 'description': 'Cut down data for QuickCharts', 'format': 'csv', 'resource_type': 'file.upload', 'url_type': 'upload'}]

            assert showcase == {'name': 'unesco-data-for-cape-verde-showcase', 'title': 'Cape Verde - Education Indicators',
                                'notes': 'Education indicators for Cape Verde', 'url': 'http://uis.unesco.org/en/country/AF', 'image_url': 'http://www.tellmaps.com/uis/internal/assets/uisheader-en.png',
                                'tags': [{'name': 'demographics', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'indicators', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}, {'name': 'hxl', 'vocabulary_id': '4e61d464-4943-4e97-973a-84673c1aaa87'}]}

            assert bites_disabled == [False, False, False]
            file = 'EDUN_CPV.csv'
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))
            file = 'qc_%s' % file
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))
            file = 'EDUN_indicatorlist.csv'
            assert_files_same(join('tests', 'fixtures', file), join(folder, file))
