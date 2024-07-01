# ddb_harvester
Two simple python scripts to download records from the [OAI/PMH-API](https://oai.deutsche-digitale-bibliothek.de/) of the [Deutsche Digitale Bibliothek (ddb)](https://www.deutsche-digitale-bibliothek.de/).  
Collects records from all found sets.  
[Ometha](https://github.com/Deutsche-Digitale-Bibliothek/ddblabs-ometha) can be used to download data from specific sets. 

## API Docs:
https://pro.deutsche-digitale-bibliothek.de/daten-nutzen/schnittstellen  
https://wiki.deutsche-digitale-bibliothek.de/pages/viewpage.action?pageId=27627109

## Protocol Docs:
https://www.openarchives.org/OAI/openarchivesprotocol.html


## Requirements:
[requests](https://pypi.org/project/requests/)

> python -m pip install requests


### harvest_records.py

Set global vars for configuration

>SAVE_DIR = ""                                  # Location to store metadata, use complete path e.g. /path/to/save/data  
>MAX_RETRIES = 10                               # Number of retries  
>THREADS = 10                                   # Number of threads use (os.cpu_count() or 1) * 5 for default  
  
1. Gets list of sets via ListSets
2. Collects record-identifiers for each set
3. Gets record-data with GetRecord using multiple threads
  
Works more reliable, creates xml for each record
  
- Pros:
    - Record fetching in multiple threads  
    - No need to process response xml

- Cons:
    - Getting identifiers for each set takes a while  
    - One API-Call for each record necessary


### harvest_records_in_batches.py
1. Gets list of sets via ListSets
2. Collects records via ListRecords
3. Loops over paginated response

- Pros:
    - Less calls to API

- Cons:
    - Response needs further processing, api returns paginated xml with selection of records
    - Seems to miss records sometimes, needs further inspection
