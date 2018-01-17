# esgfpy-solr
Python client library to interact with the ESGF Solr catalogs.
Using this module requires no authentication or authorization, since the client sends HTTP request to the Solr update URL, which is
assumed to be freely available (typically, on a restricted port such as 8984 to clients on localhost).
This module has no dependencies except for those already contained in a standard Python installation.

Behind the scenes, the module parses the metadata update instructions provided by the user, and encodes them in an XML document that
follows the Solr specificiation for atomic updates, then sends it to the Solr server for processing.

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
def update_solr(update_dict, update='set', solr_url='http://localhost:8984/solr', solr_core='datasets'):
```
Semantics:
* Use **update='set'** to add new fields and values, overriding previous fields if existing already
* Use **update='add'** to add new values to existing fields
* Use **update='set'** with None or [] value to remove a field and all its existing values

Multiple query constraints can be combines with '&', for example: 'id:obs4MIPs.NASA-JPL.AIRS.mon.v1|esgf-node.jpl.nasa.gov&variable:hus*'

Examples:

* To add two new fields to all obs4MIPs datasets:
```python
from esgfpy.update.utils import update_solr

solr_url = 'http://localhost:8984/solr'
update_dict = { 'project:obs4MIPs': {'location':['Pasadena'], 'realm':['atmosphere'] } }          
update_solr(update_dict, update='set', solr_url=solr_url, solr_core='datasets')
```

* To add additional values to existing or new fields:
```python
update_dict = { 'project:obs4MIPs&source_id:MLS': {'location':['Boulder'], 'stratus':['cumulus'] } }          
update_solr(update_dict, update='add', solr_url=solr_url, solr_core='datasets')
```
