import unittest
import datetime
import sync

class DumbDatabase(object):
    def __init__(self, changes):
        self.db = []
        self.changes = changes

    def get_changes(self, last_update):
        return self.changes

    def total_elements(self):
        return len(self.db)

    def insert_if_newer(self, change, another):
        if another is not None:
            if change.insertion > another.insertion:
                self.db.append(change)
        else:
            self.db.append(change)

class DumbCassandra(DumbDatabase):
    pass


class DumbElastic(DumbDatabase):
    pass

class TestSync(unittest.TestCase):

    def test_cassandra_sync_not_newer(self):
        cassandra = DumbCassandra(sync.ChangeList([
            sync.Change('1', datetime.datetime.now())
        ]))
        elastic = DumbElastic(sync.ChangeList())

        elements_in_elastic = elastic.total_elements()

        sync.sync(cassandra, elastic, datetime.datetime.now())
        self.assertGreater(elastic.total_elements(), elements_in_elastic)

    def test_cassandra_sync_having_a_newer(self):
        cassandra = DumbCassandra(sync.ChangeList([
            sync.Change('1', datetime.datetime(2015, 5, 8, 23, 9))
        ]))
        elastic = DumbElastic(sync.ChangeList([
            sync.Change('1', datetime.datetime(2015, 5, 8, 23, 10))
        ]))

        elements_in_elastic = elastic.total_elements()

        sync.sync(cassandra, elastic, datetime.datetime.now())

        self.assertEqual(elastic.total_elements(), elements_in_elastic)

    def test_elastic_sync_not_newer(self):
        cassandra = DumbCassandra(sync.ChangeList([]))
        elastic = DumbElastic(sync.ChangeList([
            sync.Change('1', datetime.datetime.now())
        ]))

        elements_in_cassandra = cassandra.total_elements()

        sync.sync(elastic, cassandra, datetime.datetime.now())
        self.assertGreater(cassandra.total_elements(), elements_in_cassandra)

    def test_elastic_sync_having_a_newer(self):
        cassandra = DumbCassandra(sync.ChangeList([
            sync.Change('1', datetime.datetime(2015, 5, 8, 23, 9))
        ]))
        elastic = DumbElastic(sync.ChangeList([
            sync.Change('1', datetime.datetime(2015, 5, 8, 23, 10))
        ]))

        elements_in_cassandra = cassandra.total_elements()

        sync.sync(elastic, cassandra, datetime.datetime.now())
        self.assertGreater(cassandra.total_elements(), elements_in_cassandra)


class ChangeTest(unittest.TestCase):
    def test_access_attribute(self):
        change = sync.Change('1', datetime.datetime.now(), title='Hello World', body='Lorem')

        self.assertEqual('Hello World', change.title)
        self.assertEqual('Lorem', change.body)


if __name__ == '__main__':
    unittest.main()
