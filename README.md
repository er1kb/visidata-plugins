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
# with environment variable and security certificate
vd $ES_URL --filetype=elasticsearch --http-req-verify=/home/erik/elastic/elasticsearch-ca.pem
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
| es-write | Write selected sheets in bulk |
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


### Overview shortcut
If you get good use out of this plugin, you may want to add a keyboard shortcut in your .visidatarc, for accessing the overview sheet. 
```
# capital E conveniently lists available indices 
BaseSheet.add Command('E', 'es-indexsheet', 'sheet.es_indexsheet()') 
```
I have gone with a single capital letter for convenience, but you might opt for a different [keybinding](https://www.visidata.org/docs/customize/) that is less likely to cause conflict with future Visidata updates. 



## ~~vd_colorbrewer~~ (deprecated)
Adds [Colorbrewer](https://colorbrewer2.org/) scales to Visidata plotting. Improved upon by @saulpw and added to core Visidata at [features/colorbrewer.py](https://github.com/saulpw/visidata/blob/develop/features/colorbrewer.py). 

