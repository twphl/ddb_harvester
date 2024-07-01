# ddb_harverster
Two python scripts to download metadata from the ddb OAI/PMH-API.

## API Docs:
https://pro.deutsche-digitale-bibliothek.de/daten-nutzen/schnittstellen
https://wiki.deutsche-digitale-bibliothek.de/pages/viewpage.action?pageId=27627109

## Protocol Docs:
https://www.openarchives.org/OAI/openarchivesprotocol.html


## Requirements:
requests

### harvest_parallel.py
1. Gets list of sets via ListSets
2. Collects record-identifiers for each set
3. Gets record-data with GetRecord with multiple threads
  
-> Works more reliable, creates xml for each record
  
-> Pros:
    - Record fetching in multiple threads
    - No need to process response xml


-> Cons:
    - Getting identifiers for each set takes a while
    - One API-Call for each record necessary

### harvest_records_in_batches.py
1. Gets list of sets via ListSets
2. Collects records via ListRecords
3. Loops over paginated response

-> Pros:
    - Less calls to API

-> Cons:
    - Response needs further processing, api returns paginated xml
