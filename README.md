My repository for [Visidata](https://github.com/saulpw/visidata) plugins. 

## vd_elasticsearch
An interface towards the [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html) python [client](https://elasticsearch-py.readthedocs.io/en/stable/) for reading from, writing to and managing a cluster. Also includes a subset of the Elasticsearch [analyzers, tokenizers and filters](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis.html
), for transforming text columns within Visidata. 


### Connection
You open the Elasticsearch url as if it were a filetype. 
```
# Connect to a local cluster, no security
vd http://localhost:9200 --filetype=elasticsearch
```
```
# security enabled with username, password and certificate
vd https://superuser:changeme@localhost:9200 --filetype=elasticsearch --http-req-verify=/home/erik/elastic/elasticsearch-ca.pem
```




### Commands
Commands are listed below, by the type of sheet you can call them from. Please note that there are differences in terminology. In Visidata an IndexSheet is an overview, for example the SheetsSheet that lists the sheets you currently have open. In Elasticsearch terminology however, an index is an individual table. For this plugin I have tried to go with the Visidata nomenclature: an ElasticsearchSheet is what contains an individual index/table, whereas an ElasticsearchIndexSheet lists all the available indices of the cluster. This is arbitrary, as the complete opposite would have made sense as well. 

#### BaseSheet
_Scope: Any Visidata sheet_

|  command | meaning |
| --- | --- |
| es-create | Create an empty index |
| es-read | Read from an existing index |


#### IndexSheet
_Scope: A Visidata index sheet_

|  command | meaning |
| --- | --- |
| es-read | Read indices, by a common prefix |
| es-write | Write selected sheets in bulk. Needs at least one key column to be set for each sheet. |
| setkey | Set a common key column in multiple sheets |
| addkey | Add a common numeric key column to selected sheets, starting from 0 |
| addkey-offset | Add a common numeric key column to selected sheets, starting from an offset of your choice |


#### ElasticsearchIndexSheet
_Scope: The sheet where your ES indices are listed (not the same as the Visidata index sheet type above)_

|  command | meaning |
| --- | --- |
| es-read | Read indices, by either selected rows or a common prefix |
| es-open | Open indices to read/write operations, by selected rows or common prefix |
| es-close | Close indices to read/write operations, by selected rows or common prefix |
| es-delete | Delete indices, by selected rows or common prefix |
| es-toggle-closed | Toggle open/closed for individual index |
| es-mappings | Get index mappings, by selected rows or drop-down menu. Writes to a CompressedMappingSheet |
| es-settings | Get index settings, by selected rows or drop-down menu. Writes to a CompressedMappingSheet |
| es-stats | Get cluster stats |
| es-template | Get an existing index template from the cluster |


#### TableSheet
_Scope: Any tabular sheet, whether derived from Elasticsearch or otherwise_

|  command | meaning |
| --- | --- |
| es-write | Write table to an Elasticsearch index |
| es-infer-mappings | Try to infer Elasticsearch mappings using Visidata column types |
| es-tokenize | Apply an Elasticsearch tokenizer on a Visidata column, save result to a new column |
| es-analyze | Apply an Elasticsearch analyzer on a Visidata column, save result to a new column |
| es-html-strip | Shorthand for removing html tags using the html_strip character filter on a Visidata column, save result to a new column |


#### CompressedMappingSheet
_Scope: When you have fetched the mappings of an existing index, or inferred them from a TableSheet_

|  command | meaning |
| --- | --- |
| nest-mappings | Write the mappings to a new Python Object sheet |
| dump-mappings | Dump the mappings as json to a TextSheet |


### Considerations

### When setting up the client
You need the Elasticsearch python client: __python -m pip install elasticsearch__. When connecting to a secure cluster, this plugin assumes that the user has sufficient privileges to read/write and monitor. 

### In Visidata
Please be aware that your login credentials will be fully visible inside Visidata, as they are part of the connection url. The url is shown in the _Statuses_ window at startup, and as part of the _Options_ ("es\_url"). This might be a problem if you are working in some kind of a shared environment. 

#### At index time
If you index a sheet called _dogs_ into Elasticsearch, the plugin will look for type mappings in a sheet named _dogs\_mappings_. If this mapping sheet does not exist, Elasticsearch will guess the types and give you default mappings. You can produce a template of type mappings by using _es-infer-mappings_ on a Visidata TableSheet. The mapping sheet is provided as key-value pairs, so you can edit the mappings in Visidata and not have to keep track of matching parentheses of the nested dict that Elasticsearch expects at index time.
A lot of the ES and Kibana functionality depends on typing, so try and be specific by providing the mappings at index time. The same goes for index settings, optionally contained in a sheet named _dogs\_settings_. 


### Overview shortcut
If you get good use out of this plugin, you may want to add a keyboard shortcut in your .visidatarc, for accessing the overview sheet. 
```
# capital E conveniently lists available indices 
BaseSheet.add Command('E', 'es-indexsheet', 'sheet.es_indexsheet()') 
```
I have gone with a single capital letter for convenience, but you might opt for a different [keybinding](https://www.visidata.org/docs/customize/) that is less likely to cause conflict with future Visidata updates. 



## ~~vd_colorbrewer~~ (deprecated)
Adds [Colorbrewer](https://colorbrewer2.org/) scales to Visidata plotting. Improved upon by @saulpw and added to core Visidata at [features/colorbrewer.py](https://github.com/saulpw/visidata/blob/develop/visidata/features/colorbrewer.py). 

