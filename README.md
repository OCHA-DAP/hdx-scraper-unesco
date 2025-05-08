 ### Collector for UNESCO's Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-unesco/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-unesco/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-unesco/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-unesco?branch=main)

This script connects to the [UNESCO bulk downloads](https://apiportal.uis.unesco.org/bdds) and extracts data for 5 endpoints (DEM_ECO, EDU_FINANCE, EDU_NON_FINANCE, EDU_REGIONAL_MODULE, SDG4) country by country creating one dataset (with 5 endpoints as resources) per country in HDX. It makes in the order of 10 reads from UNESCO and 1000 read/writes (API calls) to HDX in total. It creates around 120 temporary files, the largest, a zip, being under 100Mb. It is run when UNESCO make changes (not in their data but for example in their endpoints or API), in practice this is quarterly.


### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-unesco** as specified in the parameter *user_agent_lookup*.

 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, EXTRA_PARAMS, TEMP_DIR, LOG_FILE_ONLY
