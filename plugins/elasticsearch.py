'''
TODO: 
    run es_bulk_open in separate thread
    es_geo_point needs to account for row type (dict or list/tuple)
    maybe add menu item for geo_point column type
    ...
...'''

__author__='Erik Broman <mikroberna@gmail.com>'
__version__='0.1'

import os
from collections import abc

from visidata import VisiData, vd, visidata,  BaseSheet, TableSheet, PyobjSheet, TextSheet, IndexSheet, InferColumnsSheet, asyncthread, Column, SettableColumn,options, Progress, getGlobals, addGlobals, anytype, RowColorizer


# vd.option('certfile', None, 'Location of security certificate (.pem)')
# vd.option('http_auth', None, 'Tuple for basic authentication towards Elasticsearch, eg ("user", "pass"), if not given as part of the url.')
vd.option('es_hide_closed_indices', False, 'Hide closed Elasticsearch indices by default')
vd.option('es_index', None, 'Individual index to open')



@VisiData.api
def es_init(esConnectionString):
    from elasticsearch import Elasticsearch

    es = Elasticsearch([str(esConnectionString)], 
                           ca_certs = vd.options.get('http_req_verify') if str(esConnectionString).startswith('https') else None, 
                           request_timeout = 60, 
                           max_retries = 3, 
                           retry_on_timeout = True)
    return es



@VisiData.api
def open_elasticsearch(vd, p):
    index = vd.options.es_index
    if index:
        # vd.status(p.name)
        vd.status(p)
        vd.option('es_url', p, 'URL of the Elasticsearch instance')
        return ElasticsearchSheet(index,
                                  source = {'host': str(p), 'index': index})
    else:
        p.name = str(p).split('@')[-1]  # hide login credentials from sheet name
        return ElasticsearchIndexSheet(p.name, source=p)

class ElasticsearchIndexSheet(InferColumnsSheet):
    """ Index sheet of Elasticsearch indices """
    rowtype = 'indices' # rowdef: [str]

    def iterload(self):
        vd.option('es_url', self.source, 'URL of the Elasticsearch instance')
        from elasticsearch import Elasticsearch, helpers
        # es = es_init(str(self.source))
        connectionString = vd.options.get('es_url')
        es = es_init(connectionString)
        closed_indices = list(es.indices.get_alias(index = "*", expand_wildcards = 'closed').keys())
        # vd.status(closed_indices)
        wildcards = 'open' if vd.options.es_hide_closed_indices else 'all'
        all_indices = [i for i in es.indices.get_alias(index = "*", expand_wildcards = wildcards).keys() if not i.startswith('.')]
        all_indices = [{'index':i, 
                        'doc_count': int(es.cat.count(index = i).split()[2]) if i not in closed_indices else None,
                        'status': 'closed' if i in closed_indices else 'open'
        } for i in all_indices]

        self.addColorizer(RowColorizer(5, 'color_inactive_status',
                                       lambda s,c,r,v: r['status'] == 'closed' if r else None))

        for i in all_indices:
            yield i
        self.orderBy('index')

    def openRow(self, row):
        if row['status'] == 'open':
            vd.push(ElasticsearchSheet(row['index'], 
                                       source = {'host': str(self.source), 'index':row['index']}))
        else:
            vd.fail(f'Index {row["index"]} is closed. Use command es_open on it to enable read/write.')


class ElasticsearchSheet(InferColumnsSheet):
    rowtype = 'docs' # rowdef: [str]

    def iterload(self):
        from elasticsearch import helpers
        # es = es_init(str(self.source['host']))
        vd.status(self.source)
        es = es_init(str(self.source['host']))
        index = self.source['index']
        ndocs = es.count(index = index)['count']
        vd.status(str(ndocs))
        body={'query':{'match_all':{}}}
        all_docs = helpers.scan(client = es,
                                    scroll = '2m',
                                    query = body,
                                    index = index)
        for doc in Progress(all_docs, total = ndocs, gerund = f'Reading from {index}'):
            hit = doc['_source']
            yield hit


@ElasticsearchIndexSheet.api
def es_toggle_closed(self):
    """Show/hide closed indices"""
    vd.options.es_hide_closed_indices = not vd.options.es_hide_closed_indices
    self.reload()


@BaseSheet.api
@ElasticsearchIndexSheet.api
def es_get_indices(self, wildcards = 'all', key = True):
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    indices = [i for i in es.indices.get_alias(index = "*", expand_wildcards = wildcards).keys() if not i.startswith('.')]
    if key:
        indices = [{'key':p} for p in indices if not p.startswith('.')]
    return indices


## No saver available at the moment - use es-write!
## Elasticsearch indices are read-only.
# @VisiData.api
# def save_elasticsearch(vd, p, *vsheets):
#     connectionString = vd.options.get('es_url')
#     es = es_init(connectionString)
#     has_keyCols = all([s.keyCols() for s in vsheets])
#     if not has_keyCols:
#         vd.fail('Not all sheets have a key column.')
#     available_indices = es.indices.get_alias(index = "*").keys()
#     for s in vsheets:
#         if s.name not in available_indices:
#             mappings = _get_mappings_from_sheet()
#             settings = _get_settings_from_sheet()
#             es.indices.create(index = s.name, mappings = mappings, settings = settings)
#         keycol = s.keyCols[0]
#         _es_write_index(s, es, index_name = s.name, keycol = keycol)
#     es.transport.close()


@BaseSheet.api
def es_create(self):
    """Create an empty Elasticsearch index from any sheet"""
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = es.indices.get_alias(index = "*").keys()
    index_name = vd.input('Name of index to create?')
    if index_name in available_indices:
        vd.fail('Index already exists, use command es_delete to remove it.')
    mappings = _get_mappings_from_sheet()
    settings = _get_settings_from_sheet()
    es.indices.create(index = index_name, mappings = mappings, settings = settings)

@ElasticsearchIndexSheet.api
def es_delete(self):
    """Delete an index"""
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices()
    index_name = vd.choose(available_indices, n = 1)
    prompt = vd.confirm(f'Really delete {index_name}? (Y/n)')
    if prompt:
        # _es_delete(es, index_name)
        es.indices.delete(index = index_name)
    self.reload()



@ElasticsearchIndexSheet.api
def es_close(self):
    """ Close an index to read/write operations. """
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices(wildcards = 'open')
    index_name = vd.choose(available_indices, n = 1)
    es.indices.close(index = index_name, expand_wildcards = 'open')
    self.reload()

@ElasticsearchIndexSheet.api
def es_bulk_close(self):
    """ Close one or several indices to read/write operations.
    Uses selected rows if any, otherwise a common string (name startswith)."""
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices(key = False)
    if self.selectedRows:
        indices = [i['index'] for i in self.selectedRows]
    else:
        pattern = vd.input('String to match:')
        if pattern == '':
            vd.fail('No string given. Will not close all indices. Exiting.')
        if pattern.startswith('.'):
            vd.fail('Cannot close system indices (names starting with .)')
        indices = [i for i in available_indices if i.startswith(pattern) and not i.startswith('.')]
        if len(indices) == 0:
            vd.fail('Aborted. No matching indices.')
    for i in indices:
        es.indices.close(index = i)
    vd.status(f'Closed index {", ".join(indices)}')

    vd.sync()
    self.reload()


@ElasticsearchIndexSheet.api
def es_open(self):
    """ Open an index to read/write operations. """
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices(wildcards = 'closed')
    index_name = vd.choose(available_indices, n = 1)
    es.indices.open(index = index_name, expand_wildcards = 'closed')
    self.reload()


@ElasticsearchIndexSheet.api
def es_bulk_open(self):
    """ Open one or several indices to read/write operations. 
    Uses selected rows if any, otherwise a common string (name startswith). """
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices(key = False)

    if self.selectedRows:
        indices = [i['index'] for i in self.selectedRows]
    else:
        pattern = vd.input('String to match:')
        if pattern == '':
            vd.fail('No string given. Will not close all indices. Exiting.')
        if pattern.startswith('.'):
            vd.fail('Cannot close system indices (names starting with .)')
        indices = [i for i in available_indices if i.startswith(pattern) and not i.startswith('.')]
        if len(indices) == 0:
            vd.fail('Aborted. No matching indices.')

    @asyncthread
    def load_indices():
        for i in indices:
            es.indices.open(index = i)
        vd.status(f'Opened index {(", ").join(indices)}')

    vd.sync(load_indices())
    self.reload()


@ElasticsearchIndexSheet.api
def es_bulk_delete(self):
    """ Delete one or several indices. 
    Uses selected rows if any, otherwise a common string (name startswith). """
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices(key = False)

    if self.selectedRows:
        indices = [i['index'] for i in self.selectedRows]
    else:
        prompt = vd.confirm('Warning! This command will delete ALL indices matching an initial string. Proceed?')
        if not prompt:
            vd.fail('Aborted. To remove a single index, use command es_delete.')
        pattern = vd.input('String to match:')
        if pattern == '':
            vd.fail('No string given. Cannot delete all indices. Exiting.')
        if pattern.startswith('.'):
            vd.fail('Cannot close system indices (names starting with .)')
        indices = [i for i in available_indices if i.startswith(pattern)]
        # The line below is to prevent you and me from deleting system indices
        indices = [i for i in indices if not i.startswith('.')]
    if len(indices) == 0:
        vd.fail('Aborted. No matching indices.')
    if any(i.startswith('.') for i in indices):
        vd.fail('Cannot delete system indices (names starting with .)')
    prompt = vd.confirm(f'Really delete these indices? {indices}')
    if prompt:
        for i in indices:
            es.indices.delete(index = i)
        vd.status(f'Deleted index {(", ").join(indices)}')

    vd.sync()
    self.reload()

@BaseSheet.api
def _get_mappings_from_sheet(name = None, suffix = '_mappings', choose = True):
    mapping_sheets = [{'key': s.name} for s in vd.sheets if s.name.endswith(suffix)]
    vd.status('choices --> ' + str(mapping_sheets))
    vd.status('name: ' + str(name))
    if mapping_sheets and len(mapping_sheets) > 0:
        if name and vd.getSheet(name): 
            mapping_sheet = name
        else:
            mapping_sheet = vd.choose(mapping_sheets, n = 1) if choose else None
            if not mapping_sheet:
                return None
        mappings = vd.getSheet(mapping_sheet).dot_expand()
        return mappings
    else:
        return None


@BaseSheet.api
def _get_settings_from_sheet(name = None, suffix = '_settings', choose = True):
    _get_mappings_from_sheet(name = name, suffix = suffix, choose = choose)


@ElasticsearchSheet.api
@BaseSheet.api
def es_read(self):
    """ Read from Elasticsearch index """
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices()
    index_name = vd.chooseOne(available_indices)
    vd.push(ElasticsearchSheet(index_name, source = connectionString))


@IndexSheet.api
@ElasticsearchIndexSheet.api
def es_bulk_read(self):
    """ Read from Elasticsearch indices. 
    Uses selected rows if any, otherwise a common string (name startswith). """
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = self.es_get_indices(key = False)

    if self.selectedRows and not isinstance(self, IndexSheet):
        indices = [i['index'] for i in self.selectedRows]
    else:
        pattern = vd.input('String to match:')
        if pattern == '':
            vd.fail('No string given. Will not close all indices. Exiting.')
        indices = [i for i in available_indices if i.startswith(pattern) and not i.startswith('.')]
        if len(indices) == 0:
            vd.fail('Aborted. No matching indices.')

    @asyncthread
    def bulk_read(indices):
        for i in indices:
            vd.push(ElasticsearchSheet(i, source = {'host': connectionString, 'index': i}))
    bulk_read(indices)

    vd.sync()
    self.reload()

# @ElasticsearchSheet.api
# def es_save(self):
#     es = es_init(self.source)
#     if not self.keyCols:
#         vd.fail('This sheet has no key columns.')
#     keycol = self.keyCols[0]
#     available_indices = self.es_get_indices()
#     reset = vd.confirm(f'This will reset index {self.name}. Continue?')
#     # remove docs from Elasticsearch which are not present in Visidata
#     if reset: 
#         if self.name not in available_indices: 
#             mappings = _get_mappings_from_sheet(self.name + '_mappings', choose = False)
#             settings = _get_settings_from_sheet(self.name + '_settings', choose = False)
#             es.indices.create(index = self.name, mappings = mappings, settings = settings)
#         _es_write_index(self, es, index_name = self.name, keycol = keycol)


@TableSheet.api
def es_write(self, *keycols):
    """ Uses first (left-most) key column for document id, if more than one column is selected """
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    if not keycols or keycols[0] is None:
        vd.fail('No key columns selected. Select one or more key columns using !.')
    keycol = keycols[0]
    available_indices = [{'key': 'Create new index'}] + self.es_get_indices()
    index_name = vd.choose(available_indices, n = 1)
    if index_name == 'Create new index':
        index_name = vd.input('Name of index to create?')
        if index_name in available_indices:
            # prompt = vd.confirm(f'Update existing index {index_name}? (Y/n)')
            vd.fail('Index already exists, use command es_delete to remove it.')
        mappings = _get_mappings_from_sheet()
        vd.status('auto_mappings --> ' + str(mappings))
        settings = _get_settings_from_sheet()
        es.indices.create(index = index_name, mappings = mappings, settings = settings)
    _es_write_index(self, es, index_name = index_name, keycol = keycol)


@asyncthread
def _es_write_index(self, es, keycol, index_name = None, mappings = None, settings = None):
    from elasticsearch import helpers
    index_name = self.name if not index_name else index_name

    def gendata(self):
        firstrow = self.rows[0]
        vd.status('firstrow: ' + str(firstrow))
        # if isinstance(firstrow, dict):
        if isinstance(firstrow, abc.Mapping):
            # vd.status({ '_index': f'{index_name}',
            #             '_id': firstrow[keycol.name],
            #             '_op_type': 'update',
            #             'doc': {**firstrow} })
            for row in Progress(self.rows, total = len(self.rows), gerund = f'Indexing into {index_name}'):
                    yield { '_index': f'{index_name}',
                            '_id': row[keycol.name],
                            **row }
        # elif isinstance(firstrow, list) or isinstance(firstrow, tuple):
        elif isinstance(firstrow, abc.Sequence):
            keycolPosition = [i for i,c in enumerate(self.columns) if c == keycol][0]
            keys = [c.name for c in self.columns]
            # vd.status({ '_index': f'{index_name}',
            #             '_id': firstrow[keycolPosition],
            #             '_op_type': 'update',
            #             'doc': {**dict(zip(keys, firstrow))}})

            for row in Progress(self.rows, total = len(self.rows), gerund = f'Indexing into {index_name}'):
                    yield { '_index': f'{index_name}',
                            '_id': row[keycolPosition],
                            **dict(zip(keys, row))}
                            # 'doc': {**dict(zip(keys, row))}}
        else:
            vd.fail(f'Cannot parse row type {type(firstrow)}')

    helpers.bulk(es, gendata(self))


def dot_compress(d):
    """ Flattens a nested dict. https://codereview.stackexchange.com/a/21035 """
    def expand(key, value):
        # if isinstance(value, dict):
        if isinstance(value, abc.Mapping):
            return [ (key + '.' + k, v) for k, v in dot_compress(value).items() ]
        else:
            return [ (key, value) ]
    items = [ item for k, v in d.items() for item in expand(k, v) ]
    # vd.status(str(dict(items)))
    return dict(items)


def nested_set(dic, keys, value):
    """ Setter for a nested dict. https://stackoverflow.com/a/13688108/1781221 """
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value



class CompressedMappingSheet(InferColumnsSheet):
    """ A sheet where every row is a key-value pair of a mapping. """
    rowtype = 'mappings' #rowdef: [str]
    def iterload(self):
        rows = self.source
        # rows = self.rows
        # if isinstance(rows, dict):
        if isinstance(rows, abc.Mapping):
            for key in rows.keys():
                    yield {'key': key, 
                           'value': rows[key]}
        # elif isinstance(rows, list) or isinstance(rows, tuple):
        elif isinstance(rows, abc.Sequence):
            pass
        else:
            vd.fail(f'Cannot parse row source type {type(rows)}')



# Default mapping templates for when you infer mappings
# anytype
def es_map_anytype(col):
    return [{f'key': f'{col.name}.type', 'value': 'text'},
            {f'key': f'{col.name}.fields.keyword.type', 'value': 'keyword'},
            {f'key': f'{col.name}.fields.keyword.ignore_above', 'value': 256}]
# str
def es_map_str(col):
    return [{f'key': f'{col.name}.type', 'value': 'text'},
            {f'key': f'{col.name}.index', 'value': True},
            {f'key': f'{col.name}.store', 'value': True},
            {f'key': f'{col.name}.norms', 'value': True},
            {f'key': f'{col.name}.fielddata', 'value': True},
            {f'key': f'{col.name}.index_phrases', 'value': True},
            {f'key': f'{col.name}.eager_global_ordinals', 'value': False}]
# int
def es_map_int(col):
    return [{'key': f'{col.name}.type', 'value': 'integer'}]
# float
def es_map_float(col):
    return [{'key': f'{col.name}.type', 'value': 'float'}]
# date
def es_map_date(col):
    return [{'key': f'{col.name}.type', 'value': 'date'}]
# currency
# vlen
# geo_point
def es_map_geo_point(col):
    return [{'key': f'{col.name}.type', 'value': 'geo_point'}]



@TableSheet.api
def es_infer_mappings(sourcesheet):
    """ Guess ES mappings (with multi-fields) based on column types.
    https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-types.html """
    mapping_sheet = CompressedMappingSheet(sourcesheet.name + '_mappings', source = sourcesheet)
    vd.push(mapping_sheet, load = True)
    for c in sourcesheet.columns:
        vd.status((c.name, c.type))
        # vd.status(vars(c))
        # c.type = str
        if c.type == anytype:
            vd.status('anytype')
            for r in es_map_anytype(c):
                mapping_sheet.addRow(r)
        elif(c.type == str):
            for r in es_map_str(c):
                mapping_sheet.addRow(r)
        elif(c.type == int):
            for r in es_map_int(c):
                mapping_sheet.addRow(r)
        elif(c.type == float):
            for r in es_map_float(c):
                mapping_sheet.addRow(r)
        # elif(c.type == date):
        #     for r in es_date(c):
        #         mapping_sheet.addRow(r)
        elif(c.type == geo_point):
            for r in es_map_geo_point(c):
                mapping_sheet.addRow(r)
        # mapping_sheet.addRow({'key': c.name, 'value': 'meh'})
        else:
            for r in es_map_str(c):
                mapping_sheet.addRow(r)
            # vd.status(f'Unknown type, no mapping added for column {c}.')

    mapping_sheet.orderBy('key')



@CompressedMappingSheet.api
def dot_expand(self):
    """ Nest flattened mappings. """
    mappings = {}
    vd.status(str(self))
    for v in self.rows:
        nested_set(mappings, keys = v['key'].split('.'), value = v['value'])
    mappings = {'properties': mappings}
    # vd.status(mappings)
    return mappings

@CompressedMappingSheet.api
def nest_mappings(self):
    vd.push(PyobjSheet(name = self.name + '_nested', source = dot_expand(self)))


@CompressedMappingSheet.api
def dump_mappings(self):
    orig_value = vd.options.get('textwrap_cells')
    orig_doc = 'wordwrap text for multiline rows'
    vd.option('textwrap_cells', False, '')
    import json
    s = json.dumps(dot_expand(self), indent = 2)
    s = s.split('\n')
    vd.push(TextSheet(name = self.name + '_txt', source = s))
    vd.option('textwrap_cells', orig_value, orig_doc)


@ElasticsearchIndexSheet.api
def es_get_mappings(self, suffix = '_mappings'):
    """ Get actual mappings of the selected indices, each into a separate sheet. """
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    if self.selectedRows:
        indices = [i['index'] for i in self.selectedRows]
    else:
        indices = [vd.chooseOne(self.es_get_indices())]

    @asyncthread
    def bulk_get_mappings(indices):
        for i in indices:
            if suffix == '_mappings':
                mappings = es.indices.get_mapping(index = i)[i]['mappings']['properties']
            elif suffix == '_settings':
                mappings = es.indices.get_settings(index = i)[i]['settings']#['properties']
            elif suffix == '_stats':
                mappings = es.indices.field_usage_stats(index = i)[i]['shards'][0]
            else: 
                mappings = []
            mappings = dot_compress(mappings)
            vd.status(suffix + ': ' + str(mappings))
            mapping_sheet = CompressedMappingSheet(i + suffix, source = mappings)
            vd.push(mapping_sheet)

    bulk_get_mappings(indices)

    vd.sync()
    self.reload()


@ElasticsearchIndexSheet.api
def es_get_settings(self, suffix = '_settings'):
    es_get_mappings(self, suffix)

@ElasticsearchIndexSheet.api
def es_get_stats(self, suffix = '_stats'):
    es_get_mappings(self, suffix)
#
@ElasticsearchIndexSheet.api
def es_get_template(self, suffix = '_template'):
    """ Fetch an index template from the cluster. """
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_templates = [{'key':p} for p in es.indices.get_template().keys() if not p.startswith('.')]
    if len(available_templates) == 0:
        vd.fail('No non-system index templates available.')
    template_name = vd.choose(available_templates, n = 1)
    template = es.indices.get_template(name = template_name)
    vd.status('template: ' + str(template))
    # TODO: Push to new sheet
    pass


@ElasticsearchIndexSheet.api
def es_write_template(self):
    pass
    # TODO
    # es = es_init(str(self.source))
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    es.indices.put_index_template(name = '', index_patterns = [])


@IndexSheet.api
def es_bulk_write(self):
    """ Write several sheets to individual Elasticsearch indices (using the sheet names to name these indices). Mappings and settings sheets with matching names will be used automatically. For example, if sheet 'data' is indexed, sheets 'data_mappings' and 'data_settings' will contribute. Key column needs to be set for all the sheets being indexed. """
    if self.selectedRows:
        sheets = self.selectedRows
        vd.status(self.selectedRows)
    else:
        pattern = vd.input('String to match:')
        if pattern == '':
            vd.fail('No string given. Cannot match all sheets. Exiting.')
        if pattern.startswith('.'):
            vd.fail('Cannot write to system indices (names starting with .)')
        sheets = [s for s in vd.sheets if s.name.startswith(pattern) and not s.name.startswith('.')]

    mappings = None
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    available_indices = es.indices.get_alias(index = "*").keys()

    _merge = False
    merge = vd.input(f'Merge sheets into a single index? (Y/n)')
    if merge.lower().startswith('y'):
        _merge = True
    index_name = None

    if _merge:
        index_name = vd.input('Name of index?')
        if index_name in available_indices:
            vd.fail(f'Index {index_name} already exists.')
        mappings = _get_mappings_from_sheet(index_name + '_mappings', choose = False)
        settings = _get_settings_from_sheet(index_name + '_mappings', choose = False)

        es.indices.create(index = index_name, mappings = mappings, settings = settings)

    hasNoKeycols = [s for s in sheets if len(s.keyCols) == 0]
    if len(hasNoKeycols) > 0:
        vd.fail('Missing key columns for sheet(s) ' + 
                ', '.join([s.name for s in hasNoKeycols]))

    for s in sheets:
        vd.status(s.rows)
        keycol = s.keyCols[0]
        if not _merge:
            index_name = s.name
            mappings = _get_mappings_from_sheet(s.name + '_mappings', choose = False)
            settings = _get_settings_from_sheet(s.name + '_settings', choose = False)
            if index_name not in available_indices: 
                es.indices.create(index = index_name, mappings = mappings, settings = settings)
        _es_write_index(s, es, keycol, index_name)



@IndexSheet.api
def set_key(self):
    """ Helper function for setting a common key column in multiple sheets. """
    sheets = self.selectedRows
    l = []
    vd.status(vd.sheets)
    for s in sheets:
        l.append({c.name for c in s.columns})
    common_keys = l.pop().intersection(*l)
    if not common_keys:
        vd.fail('No common key in selected sheets.')

    key_column = vd.chooseOne([{'key': v} for v in common_keys])

    @asyncthread
    def _set_key(sheets, key_column):
        for s in sheets:
            s.unsetKeys(s.columns)
            for c in s.columns:
                if c.name == key_column:
                    c.keycol = 1

    vd.sync(_set_key(sheets, key_column))
    return key_column


@IndexSheet.api
def add_key(self, offset = 0):
    """ Helper function for adding a common key column in multiple sheets. """
    sheets = self.selectedRows
    colname = vd.input('Name of index variable?')
    vd.status(vd.sheets)
    vd.status(sheets)

    offset = offset

    @asyncthread
    def _add_key(sheets, colname, offset):
        for s in sheets:
            vd.push(s, pane = -1, load = True)
            vd.sync()

            firstrow = s.rows[0]
            vd.status(firstrow)
            vd.status(str({colname: offset}))
            # if isinstance(firstrow, dict):
            if isinstance(firstrow, abc.Mapping):
                s.rows = [dict(r, **{colname: i + offset}) for i,r in enumerate(s.rows)]
                c = Column(colname, type = str, sheet = s, 
                           setter=lambda col,row, val: val, 
                           getter=lambda col,row: row[colname])
            # elif isinstance(firstrow, list) or isinstance(firstrow, tuple):
            elif isinstance(firstrow, abc.Sequence):
                s.rows = [r + [i + offset] for i,r in enumerate(s.rows)]
                c = Column(colname, type = str, sheet = s, 
                           setter=lambda col,row, val: val, 
                           getter=lambda col,row: row[-1])
            else:
                vd.fail('Unrecognized row type.')

            s.addColumn(c)

            offset += s.nRows

    vd.sync(_add_key(sheets, colname, offset))
    return colname



@IndexSheet.api
def add_key_offset(self):
    offset = vd.input('Start key column at number:')
    if not offset.isdigit():
        vd.fail(f'{offset} is not a valid number')
    add_key(self, offset = int(offset))


language_analyzers = "arabic armenian basque bengali brazilian bulgarian catalan cjk czech danish dutch english estonian finnish french galician german greek hindi hungarian indonesian irish italian latvian lithuanian norwegian persian portuguese romanian russian sorani spanish swedish turkish thai".split()
language_normalization_token_filters = "arabic_normalization german_normalization hindi_normalization indic_normalization sorani_normalization persian_normalization scandinavian_normalization scandinavian_folding serbian_normalization".split()

es_builtins = {
    'analyzers': "standard simple whitespace stop pattern fingerprint".split() + language_analyzers, 
    # keyword 
    'tokenizers': "standard letter lowercase whitespace uax_url_email classic thai ngram edge_ngram".split(),
    # keyword pattern simple_pattern char_group simple_pattern_split path_hierarchy
    'token_filters': "asciifolding cjk_bigram cjk_width classic decimal_digit delimited_payload edge_ngram elision fingerprint kstem limit lowercase ngram porter_stem reverse shingle stemmer stop trim truncate unique uppercase word_delimiter ".split() + language_normalization_token_filters,
    # apostrophe common_grams condition dictionary_decompounder flatten_graph hunspell hyphenation_decompounder keep_types keep keyword_repeat keyword_marker length min_hash multiplexer pattern_capture pattern_replace predicate_token_filter remove_duplicates snowball stemmer_override synonym graph_synonyms word_delimiter_graph
    'char_filters': "html_strip".split(),
    # mapping pattern_replace
    'normalizers': "".split()
}

# stemmer token filter has language parameter
# stop token filter has stopwords parameter (language)
# all filters used with default parameters at the moment


@TableSheet.api
def _es_analyze(self, es, analyzer, tokenizer, token_filter, char_filter, text):
    analyzed_text = es.indices.analyze(analyzer = analyzer, 
                                       char_filter = char_filter, 
                                       filter = token_filter, 
                                       tokenizer = tokenizer, 
                                       text = text)
    tokens = ' '.join([t['token'] for t in analyzed_text['tokens']])
    return tokens


@TableSheet.api
def es_analyze(self, source_col, target_col):
    """ helpful blueprint: https://github.com/saulpw/visidata/discussions/1564 """
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    analyzer = vd.chooseOne([{'key': 'custom'}] + [{'key': k} for k in es_builtins['analyzers']])
    vd.status('analyzer: ' + str(analyzer))
    if analyzer == 'custom':
        analyzer = vd.input('What is the name of your custom analyzer?')

    @asyncthread
    def _analyze():
        for row in Progress(self.rows, total = self.nRows, gerund = 'Tokenizing...'):
           target_col.setValue(row, self._es_analyze(es, 
                                                     token_filter = None, 
                                                     analyzer = analyzer, 
                                                     char_filter = None, 
                                                     tokenizer = None, 
                                                     text = row[source_col.name])) 
    _analyze()


@TableSheet.api
def es_tokenize(self, source_col, target_col):
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    tokenizer = vd.chooseOne([{'key': 'None'}] + [{'key': k} for k in es_builtins['tokenizers']])
    # char_filter = vd.chooseMany([{'key': 'None'}] + [{'key': k} for k in es_builtins['char_filters']])
    token_filter = vd.chooseMany([{'key': 'None'}] + [{'key': k} for k in es_builtins['token_filters']])
    tokenizer = None if 'None' in tokenizer else tokenizer
    # char_filter = None if 'None' in char_filter else char_filter
    token_filter = None if 'None' in token_filter else token_filter

    vd.status(token_filter)

    @asyncthread
    def _tokenize():
        for row in Progress(self.rows, total = self.nRows, gerund = 'Tokenizing...'):
           target_col.setValue(row, self._es_analyze(es,
                                                     token_filter = token_filter, 
                                                     analyzer = None, 
                                                     # char_filter = char_filter, 
                                                     char_filter = None, 
                                                     tokenizer = tokenizer, 
                                                     text = row[source_col.name])) 
    _tokenize()



@TableSheet.api
def es_html_strip(self, source_col, target_col):
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)

    @asyncthread
    def _html_strip():
        for row in Progress(self.rows, total = self.nRows, gerund = 'Tokenizing...'):
           target_col.setValue(row, self._es_analyze(es,
                                                     token_filter = None, 
                                                     analyzer = None, 
                                                     char_filter = ['html_strip'], 
                                                     tokenizer = None, 
                                                     text = row[source_col.name])) 
    _html_strip()



class geo_point(str):
    """ Right now this is just a string alias. To be modified to allow for different formats. 
        https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html 
        The icon (?) is supposed to resemble a pin. 
    """
    pass


vd.addType(geo_point, icon = '?', formatter = lambda fmt, point: str(point))
# TODO: Could use a new column type in Visidata for converting to geo_shape in ES
# TableSheet.addCommand('', 'type-geo', 'cursorCol.type = geo_point', 'set type of current column to geo something')
# vd.addMenuItem('Column', 'Type as', 'geo point', 'type-geo')


@TableSheet.api
def es_geo_point(self):
    """ Convert lat/lon to Elasticsearch compatible geo_point.
    Assumes the existence of two columns called lat and lon. """
    # @asyncthread
    c = Column('geo_point', type = geo_point, sheet = self, 
               setter=lambda col,row, val: val, 
               getter=lambda col,row: str(row['lat']) + ',' + str(row['lon'] or row['long']))
    self.addColumn(c, index = 0)



# @ElasticsearchSheet.api
@BaseSheet.api
def es_indexsheet(self):
    """ Go to the index sheet of all the Elasticsearch indices. """
    connectionString = vd.options.get('es_url')
    es = es_init(connectionString)
    name = str(connectionString).split('@')[-1]  # hide login credentials from sheet name
    s = vd.getSheet(name) or ElasticsearchIndexSheet(name, source = vd.options.es_url)
    vd.push(s)
    vd.sync()
    s.reload()


BaseSheet.addCommand(None, 'es-create', 'sheet.es_create()')
BaseSheet.addCommand(None, 'es-read', 'sheet.es_read()')
TableSheet.addCommand(None, 'es-write', 'sheet.es_write(*keyCols)')

IndexSheet.addCommand(None, 'es-read', 'sheet.es_bulk_read()')
IndexSheet.addCommand(None, 'es-write', 'sheet.es_bulk_write()')
IndexSheet.addCommand(None, 'setkey', 'sheet.set_key()')
IndexSheet.addCommand(None, 'addkey', 'sheet.add_key()')
IndexSheet.addCommand(None, 'addkey-offset', 'sheet.add_key_offset()')

ElasticsearchIndexSheet.addCommand(None, 'es-read', 'sheet.es_bulk_read()')
ElasticsearchIndexSheet.addCommand(None, 'es-open', 'sheet.es_bulk_open()')
ElasticsearchIndexSheet.addCommand(None, 'es-close', 'sheet.es_bulk_close()')
# ElasticsearchIndexSheet.addCommand(None, 'es-delete', 'sheet.es_delete()')
ElasticsearchIndexSheet.addCommand(None, 'es-delete', 'sheet.es_bulk_delete()')
ElasticsearchIndexSheet.addCommand('1', 'es-toggle-closed', 'sheet.es_toggle_closed()')
ElasticsearchIndexSheet.addCommand(None, 'es-mappings', 'sheet.es_get_mappings()')
ElasticsearchIndexSheet.addCommand(None, 'es-settings', 'sheet.es_get_settings()')
ElasticsearchIndexSheet.addCommand(None, 'es-stats', 'sheet.es_get_stats()')
ElasticsearchIndexSheet.addCommand(None, 'es-template', 'sheet.es_get_template()')

TableSheet.addCommand(None, 'es-infer-mappings', 'sheet.es_infer_mappings()')
TableSheet.addCommand('', 'es-tokenize', 's=cursorCol;c=SettableColumn(s.name+"_tokenized", type=str);sheet.addColumnAtCursor(c);sheet.es_tokenize(s,c)')
TableSheet.addCommand('', 'es-analyze', 's=cursorCol;c=SettableColumn(s.name+"_analyzed", type=str);sheet.addColumnAtCursor(c);sheet.es_analyze(s,c)')
TableSheet.addCommand('', 'es-html-strip', 's=cursorCol;c=SettableColumn(s.name+"_stripped", type=str);sheet.addColumnAtCursor(c);sheet.es_html_strip(s,c)')
TableSheet.addCommand(None, 'es-geo-point', 'sheet.es_geo_point()')

BaseSheet.addCommand(None, 'print-source', 'vd.status(sheet.rows)')

CompressedMappingSheet.addCommand(None, 'nest-mappings', 'sheet.nest_mappings()')
CompressedMappingSheet.addCommand(None, 'dump-mappings', 'sheet.dump_mappings()')


vd.addGlobals({
    'ElasticsearchSheet': ElasticsearchSheet,
    'ElasticsearchIndexSheet': ElasticsearchIndexSheet,
    'CompressedMappingSheet': CompressedMappingSheet,
    'geo_point': geo_point
})




BaseSheet.addCommand('', 'es-init', 'es_init()')
