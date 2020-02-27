import abc
import collections.abc
from tqdm import tqdm
import mongoquery
import pandas
import intake.catalog
import itertools
class HackingCatalog(intake.catalog.Catalog):
    def __init__(self, json_dict, query=None, records=None, columns=None):
        self._json_dict = json_dict
        self._query = query or {}
        if records is not None and columns is not None:
            # This path is taken when we are explicitly given records and do
            # not need to parse them from CSV. This happens when filter is
            # called.
            self._columns = columns
        else:
            # Here we build a list of "records" (dicts that we are going to treat
            # like MongoDB documents) in memory. You could instead connect to a
            # MongoDB here and avoiding doing all this work / holding all the
            # records in memory. See https://github.com/bluesky/databroker/blob/master/databroker/_drivers/mongo_normalized.py
            # for an example of this.
            table = pandas.read_csv('pangeo-cmip6.csv')
            # table = pandas.read_csv(json_dict['catalog_file'])
            N = 100000
            it = tqdm(table.head(N).iterrows(), total=len(table.head(N)),
                      desc="Parsing...")
            records = {series['zstore']: series.to_dict() for _, series in it}
            self._columns = list(table.columns)

        # The library mongoquery (pure Python, pip-installable) supports most
        # of the MongoDB query API on dicts in memory.
        q = mongoquery.Query(self._query)
        # Loop through our CSV, make a dict ("record") out of each row, check
        # if it matches the query.
        self._records = {}
        for key, record in tqdm(records.items(), total=len(records),
                           desc='Filtering...'):
            if q.match(record):
                self._records[key] = record
        # TODO Validate that zstore column is unique because we treat it as the key
        # of our dict-like object.

    def __repr__(self):
        return f"<{self.__class__.__name__} ({len(self)} entries)>"

    def _repr_pretty_(self, pp, cycle):
        N = 10
        pp.text(f"<{self.__class__.__name__} ({len(self)} entries)>\n")
        pp.text(f"Summary (first {N} shown):\n")
        pp.text(repr(pandas.DataFrame(itertools.islice(self._records.values(), 10))))
     
    def filter(self, query):
        # Ensure the query is a literal dict. (We accept dict-like objects
        # too.)
        query = dict(query)
        # If *this* instance (self) is itself a set of search results, we want
        # to logically AND its query with the new query.
        if self._query:
            query = {'$and': [self._query, query]}
        # Return a new Catalog instance passing in the query.
        return self.__class__(self._json_dict, query,
                              records=self._records, columns=self._columns)

    search = filter

    def __len__(self):
        return len(self._records)

    def __getitem__(self, key):
        # The canonical unique key is 'zstore'.
        # We can also accept other aliases here if we want.
        # BUT this must always return exactly one Datasource (or raise if there
        # are multiple or no matches). Otherwise this would have type
        # instability (*sometimes* returns a DataSource; *other times* returns
        # some collection of DataSources) which would force the calling code to
        # check the return value every time...and in general breaks the
        # contract of what Python mapping-like objects are supposed to do.
        try: 
            record = self._records[key]
            return HackingDatasource(record)
        except KeyError:
            # No matches for zstore. Maybe the user tried some alias.
            path_column_name = self._json_dict['assets']['column_name']
            columns = list(self._columns)
            columns.remove(path_column_name)
            results = self.filter(dict(zip(columns, key.split('.'))))
            if len(results) > 1:
                raise KeyError("Not a unique enough key")
            elif len(results) == 0:
                raise KeyError(key)
            else:
                (_, datasource), = results.items()
                return datasource

    def items(self):
        for key, record in self._records.items():
            yield key, HackingDatasource(record)

    def __iter__(self):
        for key, _ in self.items():
            yield key

    def __contains__(self, key):
        # Python falls back to iterating over the entire catalog if this is not
        # defined. We want to avoid that! So we implement it differently.
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True


class HackingDatasource:
    "placeholder"
    def __init__(self, record):
        self._record = record

    def __repr__(self):
        return f"<{self.__class__.__name__} {self._record['zstore']}>"


class Query(collections.abc.Mapping):
    """
    This represents a MongoDB query.
    
    MongoDB queries are typically encoded as simple dicts. This object supports
    the dict interface in a read-only fashion. Subclassses add a nice __repr__
    and mutable attributes from which the contents of the dict are derived.
    """
    @abc.abstractproperty
    def query(self):
        ...

    @abc.abstractproperty
    def kwargs(self):
        ...

    def __iter__(self):
        return iter(self.query)

    def __getitem__(self, key):
        return self.query[key]
    
    def __len__(self):
        return len(self.query)

    def replace(self, **kwargs):
        """
        Make a copy with parameters changed.
        """
        return type(self)(**{**self.kwargs, **kwargs})

    def __repr__(self):
        return (f"{type(self).__name__}("
                f"{', '.join(f'{k}={v}' for k, v in self.kwargs.items())})")


class Or(Query):
    def __init__(self, *args):
        self._args = args

    @property
    def query(self):
        return {'$or': list(self._args)}


    @property
    def kwargs(self):
        ...


def main():
    import requests
    resposne = requests.get('https://raw.github.com/NCAR/intake-esm-datastore/master/catalogs/pangeo-cmip6.json')
    json_dict = resposne.json()

catalog = HackingCatalog(json_dict)


