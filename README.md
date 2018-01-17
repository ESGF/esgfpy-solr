# esgfpy-solr
Python client library to interact with the ESGF Solr catalogs.
Using this module requires no authentication or authorization, since the client sends HTTP request to the Solr update URL, which is
assumed to be freely available (typically, on a restricted port such as 8984 to clients on localhost).
This module has no dependencies except for those already contained in a standard Python installation.

## Quick Start

Check out this module from GitHub:
```shell
git clone https://github.com/ESGF/esgfpy-solr.git
cd esgfpy-solr
```
Run the example script, which updates the Solr master catalog at http://localhost:8984/solr (which must be running and open to the client for updates):
```shell
export PYTHONPATH=.
python esgfpy/solr/example.py
```

## Usage details
To use this client, you must encode the update instructions in a Python dictionary of this form:
```python
update_dict = { '<query expression>': { 'field1':['value1a','value1b',...], 'field2':['value2a','value2b',...], ... } }
```
and pass it to the following method:
```python
def updateSolr(update_dict, update='set', solr_url='http://localhost:8984/solr', solr_core='datasets'):
```
Semantics:
* Use *update='set'* to add new fields and values, overriding previous fields if existing already
* Use *update='add'* to add new values to existing fields
* Use *update='set'* with None or [] values to remove a field and all its existing values
