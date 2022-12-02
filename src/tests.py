import unittest
import pymongo
import main

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]

test_timestamp_one = "1999-11-18 13:21:26.368387"
test_timestamp_two = "2022-12-02 13:21:26.368387"


class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

    def test_ais_insertion(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.insert_ais("AISMessages.json")
        self.assertEqual("Number of Insertions: 3", result)

    def test_ais_deletion(self):
        tmb = main.TrafficMonitoringBackEnd
        tmb.insert_ais("AISMessages_2.json")
        result = tmb.delete_ais_by_timestamp(test_timestamp_one)
        self.assertEqual("Number of Deletions: 3", result)
