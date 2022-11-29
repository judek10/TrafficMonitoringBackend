import unittest
import pymongo
import main

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]

test_ais = {"Timestamp": "2020-11-18T00:00:00.000Z", "Class": "Class A", "MMSI": 210169000, "MsgType": "static_data",
            "IMO": 9584865, "CallSign": "5BNZ3", "Name": "KATHARINA SCHEPERS", "VesselType": "Cargo",
            "CargoTye": "Category X", "Length": 152, "Breadth": 24, "Draught": 7.8, "Destination": "NODRM",
            "ETA": "2020-11-18T09:00:00.000Z", "A": 143, "B": 9, "C": 13, "D": 11}


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

    def test_insert_ais(self):
        x = main.TrafficMonitoringBackEnd
        x.insert_ais(test_ais)
        self.assertTrue(myCollection.find({"IMO": 9584865}))
