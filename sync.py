import sys
import datetime
import time
import logging
import json
import argparse
from time import mktime
from cassandra.cluster import Cluster
from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)

def unix_time(dt):
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = dt - epoch
    return delta.total_seconds()

def unix_time_millis(dt):
    return int(unix_time(dt) * 1000.0)

def escape_string(s):
    """Escape a string"""
    return "'%s'" % s

def extract_date(dt):
    """Extract a date"""
    return datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')


class CassandraDB(object):
    """The Cassandra database"""
    def __init__(self, keyspace, table, id_field='id', timestamp_field='insertion', fields=[]):
        self.keyspace = keyspace
        self.table = table
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.fields = [self.id_field, self.timestamp_field] + fields
        self.conn = Cluster()

    def get_changes(self, last_update):
        """Get changes from cassandra database."""

        session = self.conn.connect(self.keyspace)

        query = "SELECT %s FROM %s WHERE %s > '%s' ALLOW FILTERING" % (
            ', '.join(self.fields),
            self.table,
            self.timestamp_field,
            unix_time_millis(last_update)
        )
        logger.debug('Get changes from cassandra query: %s' % query)

        result = session.execute(query)

        changes = []
        for change in result:
            fields = { field: getattr(change, field) for field in self.fields }
            # fields.pop(self.id_field)
            fields.pop(self.timestamp_field)

            changes.append(Change(getattr(change, self.id_field),
                    getattr(change, self.timestamp_field), **fields))

        return ChangeList(changes)

    def insert(self, change):
        values = [change.uuid, escape_string(str(unix_time_millis(change.insertion)))] + [escape_string(str(getattr(change, field))) for field in change.fields]
        session = self.conn.connect(self.keyspace)

        query = "INSERT INTO %s (%s) VALUES (%s);" % (
            self.table,
            ', '.join(self.fields),
            ', '.join(values),
        )

        logger.debug("Inserting in cassandra with query: %s" % query)

        session.execute(query)

    def insert_if_newer(self, change, another):
        if another is not None:
            if change.insertion > another.insertion:
                logger.debug('Inserting in cassandra, has another and another is old')
                self.insert(change)
        else:
            logger.debug('Inserting in cassandra, another is None')
            self.insert(change)


class ElasticDB(object):
    """The Elastic Search database"""

    def __init__(self, index, doctype, id_field='id', timestamp_field='insertion', fields=[]):
        self.index = index
        self.doctype = doctype
        self.id_field = id_field
        self.timestamp_field = timestamp_field
        self.fields = [self.id_field, self.timestamp_field] + fields
        self.es = Elasticsearch()

    def get_changes(self, last_update):
        """Get changes from elasticsearch database."""

        result = self.es.search(index=self.index, body={
            "query": {
                "filtered": {
                    "filter" : {
                        "range": {
                            "insertion": {
                                "gt": last_update
                            }
                        }
                    }
                }
            },
            "sort": {
                "insertion": { "order": "desc" }
            }
        })

        changes = []
        for change in result['hits']['hits']:
            doc = change['_source']

            fields = { field: doc[field] for field in self.fields }
            # fields.pop(self.id_field)
            fields.pop(self.timestamp_field)

            changes.append(Change(doc[self.id_field],
                extract_date(doc[self.timestamp_field]), **fields))

        return ChangeList(changes)

    def insert(self, change):
        fields = { field: getattr(change, field) for field in change.fields }

        doc = {
            self.id_field: getattr(change, self.id_field),
            self.timestamp_field: getattr(change, self.timestamp_field)
        }.update(fields)

        logger.debug(getattr(change, self.id_field))

        # Estou com um problema com essa linha :( o id é um UUID e me parece qye o elastic não está aceitando.
        # Não vou insistir neste problema, já me custou algumas horas.
        self.es.index(index=self.index, doc_type=self.doctype, id=getattr(change, self.id_field), body=doc)
        self.es.indices.refresh(index=self.index)

    def insert_if_newer(self, change, another):
        if another is not None:
            if change.insertion > another.insertion:
                logger.debug('Inserting in elastic, has another and another is old')
                self.insert(change)
        else:
            logger.debug('Inserting in elastic, another is None')
            self.insert(change)


class Change(object):

    """Represents a Change"""

    def __init__(self, uuid, insertion, **fields):
        self.uuid = uuid
        self.insertion = insertion
        self.fields = fields

        for field in fields:
            setattr(self, field, fields[field])

    def __eq__(self, another):
        return self.uuid == another.uuid


class ChangeList(object):

    """Represents a list of changes"""

    def __init__(self, changes=[]):
        self.changes = changes

    def __iter__(self):
        for change in self.changes:
            yield change

    def find(self, change):
        """Find change in list of changes or return None"""
        for e in self.changes:
            if e == change:
                return e

        return None


def sync(cassandradb, elasticdb, last_update):
    """Sync databases"""
    changes_cassandra = cassandradb.get_changes(last_update)
    changes_elastic = elasticdb.get_changes(last_update)

    logger.debug('Syncing...')

    for in_cassandra in iter(changes_cassandra):
        logger.debug('Syncing from cassandra')

        # Verify if the change is in Elastic too.
        in_elastic = changes_elastic.find(in_cassandra)

        # Insert the data in Elastic if the change is newer
        elasticdb.insert_if_newer(in_cassandra, in_elastic)

    for in_elastic in changes_elastic:
        logger.debug('Syncing from elastic')

        in_cassandra = changes_cassandra.find(in_elastic)
        cassandradb.insert_if_newer(in_elastic, in_cassandra)


def main():
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Arquivo de configuração', metavar='config')
    args = parser.parse_args()

    if args.config is None:
        logger.error('Nenhum arquivo de configuração')
        sys.exit(1)

    with open(args.config) as file:
        config = json.load(file)

    logger.info('Starting...')
    cassandradb = CassandraDB(config['cassandra']['keyspace'], config['cassandra']['table'], fields=config['fields'])
    elasticdb = ElasticDB(config['elastic']['index'], config['elastic']['collection'], fields=config['fields'])

    while True:
        sync(cassandradb, elasticdb, datetime.datetime.now())
        time.sleep(getattr(config, 'interval', 1))

if __name__ == '__main__':
    main()
