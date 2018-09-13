import logging
from esgfpy.update.utils import update_solr

logging.basicConfig(level=logging.DEBUG)

SOLR_URL = 'http://localhost:8984/solr'

# associate supplementary data to datasets
# http://esgf-data.jpl.nasa.gov/thredds/fileServer/obs4MIPs/technotes/cltTechNote_MODIS_L3_C5_200003-201109.pdf
myDict = {'id:obs4mips.NASA-JPL.TES.tro3.mon.v20110608|esgf-data.jpl.nasa.gov':
          {'xlink': ['http://esgf-data.jpl.nasa.gov/thredds/fileServer/'
                     'obs4MIPs/technotes/tro3TechNote_TES_L3_tbd_200507-200912'
                     '.pdf|TES Ozone Technical Note|technote',
                     'https://esgf-data.jpl.nasa.gov/thredds/fileServer/'
                     'obs4MIPs/supplementary_data/TES-SUPPLEMENTARY.zip|'
                     'TES Supplementary Data|supdata']},
          }

update_solr(myDict, update='set', solr_url=SOLR_URL, solr_core='datasets')
