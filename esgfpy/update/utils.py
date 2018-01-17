import json
import logging
import urllib
import urllib2
from xml.etree.ElementTree import Element, SubElement, tostring

logging.basicConfig(level=logging.INFO)

MAX_ROWS = 1000 # maximum number of records returned by a Solr query

def updateSolr(updateDict, update='set', solr_url='http://localhost:8984/solr', solr_core='datasets'):
    '''
    Method to bulk-update all matching records in a Solr index.
    
    updateDict: dictionary of Solr queries to map of field name and values to be updated for all matching results
    update='set' to override the previous values of that field, update'add' to add new values to that field

    Example of updateDict:
    setDict = { 'id:test.test.v1.testData.nc|esgf-dev.jpl.nasa.gov': 
                {'xlink':['http://esg-datanode.jpl.nasa.gov/.../zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|summary']}}
                
    Note: multiple query constraints can be combined with '&', for example: 'id:obs4MIPs.NASA-JPL.AIRS.mon.v1|esgf-node.jpl.nasa.gov&variable:hus*'
    
    Note: to remove a field, set its value to None or to an empty list, for example: 'xlink':None or 'xlink':[]
    
    Note: to transfer the value of field1 to field2, use the special '$' notation: { query: { 'field2':[$field1], ... } }
    
    Note: to rename a field, you must first transfer the value to the new field, then delete the old field.
          Example: {'project:CORDEX': {'rcm_name':['$model'], 'model':None } }

    Example of returned document:
    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
    <add>
        <doc>
            <field name="id">test.test.v1.testData.nc|esgf-dev.jpl.nasa.gov</field>
            <field name="xlink" update="add">http://esg-datanode.jpl.nasa.gov/.../zosTechNote_AVISO_L4_199210-201012.pdf|AVISO Sea Surface Height Technical Note|summary</field>
        </doc>
    </add>
    '''
    
    solr_core_url = solr_url+"/"+solr_core
    
    # process each query separately
    for query, fieldDict in updateDict.items():
        logging.info("SOLR QUERY: %s" % query)
        queries = query.split('&')
    
        # VERY IMPORTANT: FIRST QUERY FOR ALL RESULTS
        # THEN UPDATE ALL RESULTS
        # BECAUSE PAGINATION DOES NOT WORK IN BETWEEN COMMITS
        start = 0
        numFound = start+1
        xmlDocs = []
        
        # 1) query for all matching records
        while start < numFound:
    
            # query Solr, construct update document
            (xmlDoc, numFound, numRecords) = _buildSolrXml(solr_core_url, queries, fieldDict, update=update, start=start)
            xmlDocs.append( xmlDoc )
                                    
            # increase starting record locator
            start += numRecords
            
        # 2) update all matching records
        for xmlDoc in xmlDocs:
            _sendSolrXml(solr_core_url, xmlDoc)

        # 3) commit after each separate query
        _commit(solr_core_url)
    
def _buildSolrXml(solr_core_url, queries, fieldDict, update='set', start=0):
    
    # /select URL
    # https://esgf-node.jpl.nasa.gov:8984/solr/datasets/select?q=*%3A*&wt=json&indent=true
    url = solr_core_url + "/select"
    params = [ ('q','*:*'), ('fl', 'id'), ('wt','json'), ('indent','true'),
              ('start', start), ('rows', MAX_ROWS) ]
    for query in queries:
        params.append( ('fq',query) )
    # retrieve optional fields for replacement: {'rcm_name':['$experiment'] }
    for fkey, fvals in fieldDict.items():
        if fvals is not None:
            for fval in fvals:
                if fval[0]=='$':
                    params.append( ('fl',fval[1:])) # '$experiment' --> 'experiment'
        
    # execute query to Solr
    url = url + "?"+urllib.urlencode(params)
    logging.info('Executing Solr search URL=%s' % url)
    fh = urllib2.urlopen( url )
    response = fh.read().decode("UTF-8")
    jobj = json.loads(response)
    
    numFound = jobj['response']['numFound']
    numRecords = len( jobj['response']['docs'] )
    logging.info("Total number of records found: %s number of records returned: %s" % (numFound, numRecords))
                
    # update all records matching the query
    # <add>
    # root of global update document
    rootEl = Element("add")
   
    for result in jobj['response']['docs']:
        logging.debug("Updating record id=%s" % result['id'])
                
        # <doc>
        docEl = SubElement(rootEl, "doc")
        # <field name="id">obs4MIPs.NASA-JPL.AIRS.mon.v1.taStderr_AIRS_L3_RetStd-v5_200209-201105.nc|esgf-node.jpl.nasa.gov</field>
        el = SubElement(docEl, "field", attrib={ "name": 'id' })
        el.text = str(result['id'])

        # loop over fields to be updated
        for fieldName, fieldValues in fieldDict.items():
            
            if fieldValues is not None and len(fieldValues)>0:
                for fieldValue in fieldValues:
                    
                    # special case: override 'fieldValue' with value(s) from another field
                    if fieldValue[0]=='$':
                        _fieldName = fieldValue[1:] # field to copy from
                        _fieldValues = result.get(_fieldName, None)
                        if _fieldValues is not None and len(_fieldValues)>0:  
                            for _fieldValue in _fieldValues:
                                # <field name="xlink" update="set">https://earthsystemcog.org/.../taTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Air Temperature Technical Note|technote</field>
                                el = SubElement(docEl, "field", attrib={ "name": fieldName, 'update': update })
                                el.text = _fieldValue

                    # otherwise use the specified value
                    else:
                        # <field name="xlink" update="set">https://earthsystemcog.org/.../taTechNote_AIRS_L3_RetStd-v5_200209-201105.pdf|AIRS Air Temperature Technical Note|technote</field>
                        el = SubElement(docEl, "field", attrib={ "name": fieldName, 'update': update })
                        el.text = fieldValue
                        
            else:
                # <field name="xlink" update="set" null="true"/>
                el = SubElement(docEl, "field", attrib={ "name": fieldName, 'update': update, 'null':'true' })
                
    # serialize document from all queries            
    xmlstr = tostring(rootEl)
    #logging.debug(xmlstr)
    return (xmlstr, numFound, numRecords)
    

def _sendSolrXml(solr_core_url, xmlDoc):
    '''Method to send a Solr/XML update document to a specific Solr server and core.'''
    
    logging.debug( xmlDoc )
    
    # update URL (no commit)
    url = solr_core_url + '/update'
    
    # send XML document
    r = urllib2.Request(url, data=xmlDoc, headers={'Content-Type': 'application/xml'})
    u = urllib2.urlopen(r)
    response = u.read()
    logging.info(response)


def _commit(solr_core_url):
    '''Method to commit the changes.'''
    
    # commit URL
    url = solr_core_url + '/update?commit=true'
    
    # send XML document
    r = urllib2.Request(url)
    u = urllib2.urlopen(r)
    response = u.read()
    logging.debug(response)
