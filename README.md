# ddb_harverster
Two python scripts to download records from the [ddb](https://www.deutsche-digitale-bibliothek.de/) OAI/PMH-API. Collects records from all found sets.
[Ometha](https://github.com/Deutsche-Digitale-Bibliothek/ddblabs-ometha) can be used to download data from specific sets. 

## API Docs:
https://pro.deutsche-digitale-bibliothek.de/daten-nutzen/schnittstellen  
https://wiki.deutsche-digitale-bibliothek.de/pages/viewpage.action?pageId=27627109

## Protocol Docs:
https://www.openarchives.org/OAI/openarchivesprotocol.html


## Requirements:
[requests](https://pypi.org/project/requests/)

> python -m pip install requests

### harvest_parallel.py

1. Gets list of sets via ListSets
2. Collects record-identifiers for each set
3. Gets record-data with GetRecord with multiple threads
  
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
    - Response needs further processing, api returns paginated xml
    - Seems to not get all records, sometimes misses some records, needs further inspection
