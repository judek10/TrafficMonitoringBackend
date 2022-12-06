import unittest
import pymongo
import main

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]

test_timestamp_one = "1999-11-18 13:21:26.368387"
test_timestamp_two = "2022-12-02 13:21:26.368387"

test_recent_postions = [
    {"Timestamp": "2020-11-18T00:02:00.000Z", "Class": "Class A", "MMSI": 244265000, "MsgType": "position_report",
     "Position": {"type": "Point", "coordinates": [55.522592, 15.068637]}, "Status": "Under way using engine",
     "RoT": 2.2, "SoG": 14.8, "CoG": 62, "Heading": 61},
    {"Timestamp": "2020-11-18T00:01:00.000Z", "Class": "Class A", "MMSI": 220490000, "MsgType": "position_report",
     "Position": {"type": "Point", "coordinates": [56.60658, 8.088727]}, "Status": "Under way using engine", "RoT": 35,
     "SoG": 8.8, "CoG": 12.2, "Heading": 16},
    {"Timestamp": "2020-11-18T00:00:20.000Z", "Class": "Class A", "MMSI": 210169000, "MsgType": "static_data",
     "IMO": 9584865, "CallSign": "5BNZ3", "Name": "KATHARINA SCHEPERS", "VesselType": "Cargo", "CargoTye": "Category X",
     "Length": 152, "Breadth": 24, "Draught": 7.8, "Destination": "NODRM", "ETA": "2020-11-18T09:00:00.000Z", "A": 143,
     "B": 9, "C": 13, "D": 11},
    {"Timestamp": "2020-11-18T00:00:10.000Z", "Class": "Class A", "MMSI": 219018405, "MsgType": "position_report",
     "Position": {"type": "Point", "coordinates": [57.71615, 10.586342]}, "Status": "Engaged in fishing", "RoT": 0}]

test_ais = {"Timestamp": "2020-11-18T00:02:00.000Z", "Class": "Class A", "MMSI": 244265000,
            "MsgType": "position_report",
            "Position": {"type": "Point", "coordinates": [55.522592, 15.068637]}, "Status": "Under way using engine",
            "RoT": 2.2, "SoG": 14.8, "CoG": 62, "Heading": 61}


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

    def test_batch_of_ais_insertion(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.insert_batch_of_ais("AISMessages.json")
        self.assertEqual("Number of Insertions: 3", result)

    def test_single_ais_insertion(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.insert_single_ais(test_ais)
        self.assertEqual("Success: 1", result)

    def test_ais_deletion(self):
        tmb = main.TrafficMonitoringBackEnd
        tmb.insert_batch_of_ais("AISMessages_2.json")
        result = tmb.delete_ais_by_timestamp(test_timestamp_one)
        self.assertEqual("Number of Deletions: 3", result)

    def test_find_ports_with_name(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.find_all_ports("Falkenberg")
        self.assertEqual([{'country': 'Sweden', 'id': '446', 'latitude': '56.889444', 'longitude': '12.481944',
                           'mapview_1': 1, 'mapview_2': 5530, 'mapview_3': 55301, 'port_location': 'Falkenberg',
                           'un/locode': 'SEFAG', 'website': 'www.falkenbergs-terminal.se'}], result)

    def test_find_ports_with_name_and_country(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.find_all_ports("Struer", "Denmark")
        self.assertEqual([{'country': 'Denmark', 'id': '2977', 'latitude': '56.494167', 'longitude': '8.595556',
                           'mapview_1': 1, 'mapview_2': 5137, 'mapview_3': 51372, 'port_location': 'Struer',
                           'un/locode': 'DKSTR', 'website': 'www.struerhavn.dk'}], result)

    def test_get_recent_vessel_positions(self):
        x = main.TrafficMonitoringBackEnd()
        recent_vessel_positions = x.get_recent_vessel_positions()
        positions = []
        for position in recent_vessel_positions:
            positions.append(position)
        self.assertEqual([{'MMSI': 235090202, 'Position': {'coordinates': [54.646827, 11.355255]}},
                          {'MMSI': 266343000, 'Position': {'coordinates': [55.503335, 10.874448]}},
                          {'MMSI': 375203000, 'Position': {'coordinates': [54.520033, 12.182773]}}],
                         positions)

    def test_get_recent_vessel_position_mmsi(self):
        x = main.TrafficMonitoringBackEnd
        recent_vessel_position = x.get_recent_vessel_position_mmsi(235090202)
        positions = []
        for position in recent_vessel_position:
            positions.append(position)
        position = positions[0]
        self.assertEqual({'MMSI': 235090202, 'Position': {'coordinates': [54.646827, 11.355255]}}, position)
