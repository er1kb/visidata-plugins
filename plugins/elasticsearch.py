'''
TODO: 
    self.es.indices.analyze(index, body) - möjlighet att använda olika tokenizers och analyzers i Visidata
    Indexera flera blad på en gång, dvs bulk write
        - Från IndexSheet: indexera markerade rader
        - (Från övriga blad: låt användaren välja blad (vd.ChooseMany) från en lista)
    Göm inloggningsuppgifterna i bladnamnet (i sheets sheet)
    MAKE es_bulk_read async!

    ...
...'''

__author__='Erik Broman <mikroberna@gmail.com>'
__version__='0.1'

import os
import json

from visidata import VisiData, vd, BaseSheet, TableSheet, InferColumnsSheet, asyncthread, Progress, Column, options, visidata, SequenceSheet, PythonSheet, copy, Sheet, IndexSheet, getGlobals, SettableColumn, addGlobals, getGlobals


# vd.option('default_colname', '', '')
# vd.option('es_index', 'kt-komplett-20221124', 'Which index to read from')
vd.option('certfile', None, 'Location of security certificate (.pem)')
vd.option('http_auth', None, 'Tuple for basic authentication towards Elasticsearch, eg ("user", "pass"), if not given as part of the url.')
vd.option('es_chunk_size', 500, 'Chunk size when indexing documents in Elasticsearch.')
vd.option('es_hide_system_indices', True, 'Hide Elasticsearch system indices by default')


@VisiData.api
def es_init(esConnectionString):
    from elasticsearch import Elasticsearch

    es = Elasticsearch([esConnectionString], 
                           # ca_certs = certfile, 
                           ca_certs = vd.options.certfile, 
                           http_auth = vd.options.http_auth,
                           request_timeout = 60, 
                           max_retries = 3, 
                           retry_on_timeout = True)
    return es


@BaseSheet.api
def print_es():
    vd.status(vd.es)

# visidata.addCommand('', 'print_es', 'vd.print_es()')
BaseSheet.addCommand('', 'es_init', 'es_init()')
BaseSheet.addCommand('', 'print_es', 'sheet.print_es()')



@VisiData.api
def open_elasticsearch(vd, p):
    p.name = p.name.split('@')[-1]  # hide login credentials from sheet name
    return ElasticsearchIndexSheet(p.name, source=p)
    # return ElasticsearchSheet(p.name, source=p)

class ElasticsearchIndexSheet(InferColumnsSheet):
    """ Index sheet of Elasticsearch indices """
    rowtype = 'indices' # rowdef: [str]

    def iterload(self):
        vd.option('es_url', self.source, 'URL of the Elasticsearch instance')
        from elasticsearch import Elasticsearch, helpers
        es = es_init(str(self.source))
        indices = es.indices.get_alias(index = "*").keys()
        indices = [{'index':i} for i in indices]
        if vd.options.es_hide_system_indices:
            indices = [i for i in indices if not i['index'].startswith('.')]

        for i in indices:
            yield i
        self.orderBy('index')

    def openRow(self, row):
        vd.push(ElasticsearchSheet(row['index'], source = self.source))

class ElasticsearchSheet(InferColumnsSheet):
    rowtype = 'docs' # rowdef: [str]

    def iterload(self):
        vd.option('es_url', self.source, 'URL of the Elasticsearch instance')
        from elasticsearch import helpers
        es = es_init(str(self.source))
        index = self.name
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
def es_toggle_hidden(self):
    vd.options.es_hide_system_indices = not vd.options.es_hide_system_indices
    self.reload()

@BaseSheet.api
def es_get_indices(self, key = True):
    es = es_init(str(self.source))
    indices = es.indices.get_alias(index = "*").keys()
    if vd.options.es_hide_system_indices:
        indices = [i for i in indices if not i.startswith('.')]
    if key:
        indices = [{'key':p} for p in indices if not p.startswith('.')]
    # vd.status(str(indices))
    vd.status(indices)
    return indices


# @VisiData.api
# def save_elasticsearch(vd, p, *vsheets):
#
#     # index_name = vd.input('Name of index to write to?')
#
#     from elasticsearch import Elasticsearch
#     import eland as ed
#     es_py_pw = os.getenv('ES_PY_PW')
#     es_py_un = os.getenv('ES_PY_UN')
#
#     # certfile = '/home/erik/elastic/elasticsearch-chain.pem'
#     # esConnectionString = f'https://{es_py_un}:{es_py_pw}@161.52.11.63:9200'
#     esConnectionString = f'https://{es_py_un}:{es_py_pw}@localhost:9200'
#
#     es = Elasticsearch([esConnectionString], 
#                            ca_certs = vd.options.certfile, 
#                            request_timeout = 60, 
#                            max_retries = 3, 
#                            retry_on_timeout = True)
#
#     es.indices.create(index = 'meh')
#
#     es.transport.close()


@ElasticsearchSheet.api
def es_create(self):
    es = es_init(str(self.source))
    available_indices = es.indices.get_alias(index = "*").keys()
    index_name = vd.input('Name of index to create?')
    if index_name in available_indices:
        vd.fail('Index already exists, use command es_delete to remove it.')
    mappings = _get_mappings_from_sheet()
    settings = _get_settings_from_sheet()
    es.indices.create(index = index_name, mappings = mappings, settings = settings)

@ElasticsearchIndexSheet.api
def es_delete(self):
    es = es_init(str(self.source))
    available_indices = self.es_get_indices()
    index_name = vd.choose(available_indices, n = 1)
    prompt = vd.confirm(f'Really delete {index_name}? (Y/n)')
    if prompt:
        _es_delete(es, index_name)
        # es.indices.delete(index = index_name)
    self.reload()

def _es_delete(es, index):
    es.indices.delete(index = index)



@ElasticsearchSheet.api
def es_bulk_delete(self):
    es = es_init(str(self.source))
    available_indices = self.es_get_indices(key = False)
    prompt = vd.confirm('Warning! This command will delete ALL indices matching an initial string. Proceed?')
    if not prompt:
        vd.fail('Aborted. To remove a single index, use command es_delete.')
    pattern = vd.input('String to match:')
    if pattern == '':
        vd.fail('No string given. Cannot delete all indices. Exiting.')
    matching_indices = [i for i in available_indices if i.startswith(pattern)]
    # The line below is to prevent you and me from deleting system indices
    matching_indices = [i for i in matching_indices if not i.startswith('.')]
    if len(matching_indices) == 0:
        vd.fail('Aborted. No matching indices.')
    if any(i.startswith('.') for i in matching_indices):
        vd.fail('Cannot delete system indices (names starting with .)')
    prompt = vd.confirm(f'Really delete these indices? {matching_indices}')
    if prompt:
        for i in matching_indices:
            vd.status(f'Deleted index {i}')
            es.indices.delete(index = i)

@Sheet.api
def _get_mappings_from_sheet():
    mapping_sheets = [{'key': s.name} for s in vd.sheets if s.name.endswith('_mappings')]
    vd.status('choices --> ' + str(mapping_sheets))
    if mapping_sheets and len(mapping_sheets) > 0:
        mapping_sheet = vd.choose(mapping_sheets, n = 1)
        mappings = vd.getSheet(mapping_sheet).nest_mappings()
        return mappings
    else:
        return None


@Sheet.api
def _get_settings_from_sheet():
    settings_sheets = [{'key': s.name} for s in vd.sheets if s.name.endswith('_settings')]
    if settings_sheets and len(settings_sheets) > 0:
        settings_sheet = vd.choose(settings_sheets, n = 1)
        settings = vd.getSheet(settings_sheet).nest_mappings()
        return settings
    else:
        return None


@ElasticsearchSheet.api
def es_read(self):
    """ Read from Elasticsearch index """
    es = es_init(str(self.source))
    available_indices = self.es_get_indices()
    index_name = vd.chooseOne(available_indices)
    vd.push(ElasticsearchSheet(index_name, source = self.source))


@ElasticsearchSheet.api
def es_bulk_read(self):
    """ Read from Elasticsearch indices """
    es = es_init(str(self.source))
    pattern = vd.input('Index name pattern? (starts with, no wildcard character)')
    indices = es.indices.get_alias(index = f"{pattern}*").keys()
    if vd.options.es_hide_system_indices:
        indices = [i for i in indices if not i.startswith('.')]
    # vd.status(list(indices))
    for i in indices:
        vd.push(ElasticsearchSheet(i, source = self.source))



@ElasticsearchSheet.api
def es_write(self, *cols):
    """ Uses first (left-most) key column for document id, if more than one column is selected """
    # from elasticsearch import Elasticsearch, helpers
    es = es_init(str(self.source))
    if not cols or cols[0] is None:
        vd.fail('No key columns selected. Select one or more key columns using !.')
    # keycol = cols[0].name
    keycol = cols[0]
    available_indices = [{'key': 'Create new index'}] + self.es_get_indices()
    index_name = vd.choose(available_indices, n = 1)
    if index_name == 'Create new index':
        index_name = vd.input('Name of index to create?')
        if index_name in available_indices:
            vd.fail('Index already exists, use command es_delete to remove it.')
    mappings = _get_mappings_from_sheet()
    vd.status('auto_mappings --> ' + str(mappings))
    settings = _get_settings_from_sheet()
    es.indices.create(index = index_name, mappings = mappings, settings = settings)
    _es_write_index(self, es, keycol = keycol)


@asyncthread
def _es_write_index(self, es, keycol):
    from elasticsearch import helpers
    def gendata(self):
        firstrow = self.rows[0]
        vd.status('firstrow: ' + str(firstrow))
        if isinstance(firstrow, dict):
            for row in Progress(self.rows, total = len(self.rows), gerund = f'Indexing into {self.name}'):
                    yield { '_index': f'{self.name}',
                            '_id': row[keycol.name],
                            **row }
        elif isinstance(firstrow, list) or isinstance(firstrow, tuple):
            keycolPosition = [i for i,c in enumerate(self.columns) if c == keycol][0]
            keys = [c.name for c in self.columns]
            for row in Progress(self.rows, total = len(self.rows), gerund = f'Indexing into {self.name}'):
                    yield { '_index': f'{self.name}',
                            '_id': row[keycolPosition],
                            **dict(zip(keys, row))}
        else:
            vd.fail(f'Cannot parse row type {type(firstrow)}')
    helpers.bulk(es, gendata(self))


def unnest(d):
    """ https://codereview.stackexchange.com/a/21035 """
    def expand(key, value):
        if isinstance(value, dict):
            return [ (key + '.' + k, v) for k, v in unnest(value).items() ]
        else:
            return [ (key, value) ]
    items = [ item for k, v in d.items() for item in expand(k, v) ]
    return dict(items)


def nested_set(dic, keys, value):
    """ https://stackoverflow.com/a/13688108/1781221 """
    for key in keys[:-1]:
        dic = dic.setdefault(key, {})
    dic[keys[-1]] = value



class NestedMappingSheet(InferColumnsSheet):
    rowtype = 'mappings' #rowdef: [str]
    # endpoint = '_mapping'
    def iterload(self):
        rows = self.source
        for key in rows.keys():
            yield {'key': key, 
                   'value': rows[key]}


@BaseSheet.api
def print_source(self):
    vd.status(self.source)

@BaseSheet.api
def print_rows(self):
    vd.status(self.rows)

@Sheet.api
def es_infer_mappings(sheet):
    """..."""
    # TODO: create ES mappings (with multi-fields) based on column types
    mapping_sheet = NestedMappingSheet(name = sheet.name + '_mappings', source = sheet)
    vd.push(mapping_sheet)



@NestedMappingSheet.api
def nest_mappings(self):
    mappings = {}
    for v in self.rows:
        nested_set(mappings, keys = v['key'].split('.'), value = v['value'])
    mappings = {'properties': mappings}
    vd.status(mappings)
    return mappings

@Sheet.api
def es_get_mappings(self):
    # import json
    es = es_init(str(self.source))
    available_indices = self.es_get_indices()
    # available_indices = self.es.indices.get_alias(index = "*").keys()
    # available_indices = [{'key':p} for p in available_indices if not p.startswith('.')]
    index_name = vd.choose(available_indices, n = 1)
    mappings = es.indices.get_mapping(index = index_name)[index_name]['mappings']['properties']
    mappings = unnest(mappings)
    mapping_sheet = NestedMappingSheet(index_name + '_mappings', source = mappings)
    vd.push(mapping_sheet)

@Sheet.api
def es_get_settings(self):
    # import json
    es = es_init(str(self.source))
    available_indices = self.es_get_indices()
    # available_indices = self.es.indices.get_alias(index = "*").keys()
    # available_indices = [{'key':p} for p in available_indices if not p.startswith('.')]
    index_name = vd.choose(available_indices, n = 1)
    settings = es.indices.get_settings(index = index_name)[index_name]['settings']
    settings = unnest(settings)
    settings_sheet = NestedMappingSheet(index_name + '_settings', source = settings)
    vd.push(settings_sheet)


@ElasticsearchSheet.api
def es_stats(self):
    # import json
    es = es_init(str(self.source))
    available_indices = self.es_get_indices()
    # available_indices = self.es.indices.get_alias(index = "*").keys()
    # available_indices = [{'key':p} for p in available_indices if not p.startswith('.')]
    index_name = vd.choose(available_indices, n = 1)
    stats = es.indices.field_usage_stats(index = index_name)[index_name]['shards'][0]
    stats = unnest(stats)
    stats_sheet = NestedMappingSheet(index_name + '_settings', source = stats)
    vd.push(stats_sheet)

@Sheet.api
def print_sheetstack(self):
    vd.status([s.name for s in vd.sheets])

@Sheet.api
def print_sheets(self):
    vd.status(str(vd.sheets))

@Sheet.api
def print_globals(self):
    vd.status(str(visidata.getGlobals()))

@BaseSheet.api
def print_b(self):
    col = [c for c in self.columns if c.name == 'b'][0]
    vd.status(*col.getValues(self.rows))

@IndexSheet.api
def es_bulk_write(self):
    """ Write several sheets to individual Elasticsearch indices (using the sheet names to name these indices). Mappings and settings sheets with matching names will be used automatically. For example, if sheet 'data' is indexed, sheets 'data_mappings' and 'data_settings' will contribute. Key column needs to be set for all the sheets being indexed. """
    # sheets = [s.name for s in vd.sheets]
    # sheets = vd.sheets
    sheets = self.selectedRows
    connectionString = vd.options.get('es_url')
    # vd.status(connectionString)
    es = es_init(connectionString)

    hasNoKeycols = [s for s in sheets if len(s.keyCols) == 0]
    if len(hasNoKeycols) > 0:
        vd.fail('Missing key columns for sheet(s)' + 
                ', '.join([s.name for s in hasNoKeycols]))

    for s in sheets:
        vd.status(s.rows)
        # keycol = s.keyCols[0].name
        keycol = s.keyCols[0]
        # for row in s.rows:
        #     vd.status(row[keycol])
        # vd.status(s.name + ' ,' + str(s.keyCols))
        _es_write_index(s, es, keycol)
    # vd.fail('TODO')

@IndexSheet.api
def es_merge_write():
    """ Write several sheets to a common Elasticsearch index. Mappings and settings sheets with matching names will be used automatically. For example, if sheet 'data' is indexed, sheets 'data_mappings' and 'data_settings' will contribute. Key column needs to be set for all the sheets being indexed (use helper functions add_key and set_key beforehand). """
    # if there are any duplicate keys in the data of the different sheets, concatenate the key column value with the sheet name
    vd.fail('TODO')


@IndexSheet.api
def set_key(self):
    """ Helper function to set common key columns for multiple sheets. """
    sheets = self.selectedRows
    l = []
    vd.status(vd.sheets)
    for s in sheets:
        # vd.push(s, pane = -1, load = True)
        # s.reload()
        l.append({c.name for c in s.columns})
        # vd.status([c.name for c in s.columns])
    # vd.status(l)
    common_keys = l.pop().intersection(*l)
    vd.status(common_keys)

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
def add_key(self):
    """ Helper function to add a common key columns for multiple sheets. """
    sheets = self.selectedRows
    colname = vd.input('Name of index variable?')
    vd.status(vd.sheets)
    vd.status(sheets)

    offset = 0

    @asyncthread
    def _add_key(sheets, colname, offset):
        for s in sheets:
            vd.push(s, pane = -1, load = True)
            # s.reload()
            vd.sync()

            s.rows = [r + [i + offset] for i,r in enumerate(s.rows)]
            c = Column(colname, type = str, sheet = s, 
                       setter=lambda col,row, val: val, 
                       getter=lambda col,row: row[-1])
            # c.setValues(s.rows, offset)
            s.addColumn(c)
            # freeze column?

            offset += s.nRows

    vd.sync(_add_key(sheets, colname, offset))
    return colname

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

meh = "this is the <b>pärt</b> where I <i>say</i> I dön't <p name='paragraph'>want you</p> I'm stronger than I've been before &amp; this is the pårt where I break free cause I can't resist it no more arianagrande.com ariana@grande.com"
# , char_filter = ['html_strip'], filter = ['asciifolding', 'unique'], 

# stemmer token filter has language parameter
# stop token filter has stopwords parameter (language)

@BaseSheet.api
# @asyncthread
def _es_analyze(self, es, analyzer, tokenizer, token_filter, char_filter, text):
    analyzed_text = es.indices.analyze(analyzer = analyzer, 
                                       char_filter = char_filter, 
                                       filter = token_filter, 
                                       tokenizer = tokenizer, 
                                       text = text)
    tokens = ' '.join([t['token'] for t in analyzed_text['tokens']])
    return tokens


@Sheet.api
def es_analyze(self, source_col, target_col):
    """ helpful blueprint: https://github.com/saulpw/visidata/discussions/1564 """
    es = es_init(str(self.source))
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


@Sheet.api
def es_tokenize(self, source_col, target_col):
    es = es_init(str(self.source))
    tokenizer = vd.chooseOne([{'key': 'None'}] + [{'key': k} for k in es_builtins['tokenizers']])
    char_filter = vd.chooseMany([{'key': 'None'}] + [{'key': k} for k in es_builtins['char_filters']])
    token_filter = vd.chooseMany([{'key': 'None'}] + [{'key': k} for k in es_builtins['token_filters']])
    tokenizer = None if 'None' in tokenizer else tokenizer
    char_filter = None if 'None' in char_filter else char_filter
    token_filter = None if 'None' in token_filter else token_filter

    vd.status(token_filter)

    @asyncthread
    def _tokenize():
        for row in Progress(self.rows, total = self.nRows, gerund = 'Tokenizing...'):
           target_col.setValue(row, self._es_analyze(es,
                                                     token_filter = token_filter, 
                                                     analyzer = None, 
                                                     char_filter = char_filter, 
                                                     tokenizer = tokenizer, 
                                                     text = row[source_col.name])) 
    _tokenize()


Sheet.addCommand('', 'es_tokenize', 's=cursorCol;c=SettableColumn(s.name+"_tokenized", type=str);sheet.addColumnAtCursor(c);sheet.es_tokenize(s,c)')
Sheet.addCommand('', 'es_analyze', 's=cursorCol;c=SettableColumn(s.name+"_analyzed", type=str);sheet.addColumnAtCursor(c);sheet.es_analyze(s,c)')



# @BaseSheet.api
# def es_analyze(self, col, key = True):
#     values = col.getValues(self.rows)
#     self.es = es_init(str(self.source))
#     analyzer = vd.chooseOne([{'key': k} for k in es_builtins['analyzers']])
#     colname = col.name + '_analyzed'
#
#
#     # @asyncthread
#     def analyze(values):
#         for v in Progress(values, total = len(self.nRows), gerund = 'Analyzing...'):
#             yield _es_analyze(self, analyzer = analyzer,
#                                     char_filter = None,
#                                     token_filter = None,
#                                     tokenizer = None,
#                                     text = v)
#
#     # self.rows = [{**r, **{colname: analyzed}} for i,r in enumerate(self.rows)]
#     self.rows = [{**self.rows[i], **{colname: v}} for i,v in enumerate(analyze(values))]
#
#     c = Column(colname, sheet = self,
#                         setter=lambda col,row, val: val, 
#                         getter=lambda col,row: row[colname])
#     # c.setValues(self.rows, *analyze(values))
#     self.addColumn(c)

# @BaseSheet.api
# def es_tokenize(self, col, key = True):
#     values = col.getValues(self.rows)
#     self.es = es_init(str(self.source))
#     colname = col.name + '_tokenized'
#     tokenizer = vd.chooseOne([{'key': 'None'}] + [{'key': k} for k in es_builtins['tokenizers']])
#     char_filter = vd.chooseMany([{'key': 'None'}] + [{'key': k} for k in es_builtins['char_filters']])
#     token_filter = vd.chooseMany([{'key': 'None'}] + [{'key': k} for k in es_builtins['token_filters']])
#
#     tokenizer = None if tokenizer == 'None' else tokenizer
#     char_filter = None if char_filter == 'None' else char_filter
#     token_filter = None if token_filter == 'None' else token_filter
#     
#     vd.status(token_filter)
#
#     def analyze(values):
#         for v in Progress(values, total = 240000, gerund = 'Analyzing...'):
#             yield _es_analyze(self, analyzer = None,
#                                     char_filter = char_filter,
#                                     token_filter = token_filter,
#                                     tokenizer = tokenizer,
#                                     text = v)
#
#     vd.sync()
#
#     vd.status('self.defer: ' + str(self.defer))
#
#     self.rows = [{**self.rows[i], **{colname: v}} for i,v in enumerate(analyze(values))]
#
#     # c = Column(colname, sheet = self)
#     c = Column(colname, sheet = self,
#                         setter=lambda col,row, val: val, 
#                         getter=lambda col,row: row[colname])
#     # # c.setValues(self.rows, *analyze(values))
#     self.addColumn(c)


ElasticsearchSheet.addCommand(None, 'es_read', 'sheet.es_read()')
ElasticsearchSheet.addCommand(None, 'es_bulk_read', 'sheet.es_bulk_read()')
ElasticsearchSheet.addCommand(None, 'es_create', 'sheet.es_create()')
ElasticsearchSheet.addCommand(None, 'es_write', 'sheet.es_write(*keyCols)')
ElasticsearchSheet.addCommand(None, 'es_infer_mappings', 'sheet.es_infer_mappings()')
ElasticsearchSheet.addCommand(None, 'es_get_mappings', 'sheet.es_get_mappings()')
ElasticsearchSheet.addCommand(None, 'es_get_settings', 'sheet.es_get_settings()')
ElasticsearchSheet.addCommand(None, 'es_stats', 'sheet.es_stats()')
ElasticsearchIndexSheet.addCommand(None, 'es_delete', 'sheet.es_delete()')
ElasticsearchIndexSheet.addCommand('1', 'es_toggle_hidden', 'sheet.es_toggle_hidden()')
ElasticsearchSheet.addCommand(None, 'es_get_indices', 'sheet.es_get_indices()')
IndexSheet.addCommand(None, 'set_key', 'sheet.set_key()')
IndexSheet.addCommand(None, 'add_key', 'sheet.add_key()')
IndexSheet.addCommand(None, 'es_bulk_write', 'sheet.es_bulk_write()')
IndexSheet.addCommand(None, 'es_merge_write', 'sheet.es_merge_write()')
IndexSheet.addCommand(None, 'es_bulk_delete', 'sheet.es_bulk_delete()')

BaseSheet.addCommand(None, 'print_source', 'sheet.print_source()')
BaseSheet.addCommand(None, 'print_rows', 'sheet.print_rows()')
BaseSheet.addCommand(None, 'print_col', 'vd.status(vars(cursorCol))')
BaseSheet.addCommand(None, 'nest_mappings', 'sheet.nest_mappings()')
BaseSheet.addCommand(None, 'print_sheetstack', 'sheet.print_sheetstack()')
BaseSheet.addCommand(None, 'print_sheets', 'sheet.print_sheets()')
BaseSheet.addCommand(None, 'print_globals', 'sheet.print_globals()')
BaseSheet.addCommand(None, 'print_b', 'sheet.print_b()')

vd.addGlobals({
    'ElasticsearchSheet': ElasticsearchSheet,
    'NestedMappingSheet': NestedMappingSheet,
})
