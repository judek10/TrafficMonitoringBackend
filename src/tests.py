import unittest
import pymongo
import main

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]

test_timestamp_one = "1801-11-18 13:21:26.368387"
test_timestamp_two = "2022-12-02 13:21:26.368387"
test_timestamp_three = "1903-12-02 13:21:26.368387"

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

test_ais_two = {"Timestamp": "1902-11-18T00:02:00.000Z", "Class": "Class A", "MMSI": 244265000,
                "MsgType": "position_report",
                "Position": {"type": "Point", "coordinates": [55.522592, 15.068637]},
                "Status": "Under way using engine",
                "RoT": 2.2, "SoG": 14.8, "CoG": 62, "Heading": 61}

test_ais_three = {"Timestamp": "2040-11-18T00:02:00.000Z", "Class": "Class A", "MMSI": 244265000,
                  "MsgType": "position_report",
                  "Position": {"type": "Point", "coordinates": [55.522592, 15.068637]},
                  "Status": "Under way using engine",
                  "RoT": 2.2, "SoG": 14.8, "CoG": 62, "Heading": 61}

unique_ais = {"Timestamp": "1902-11-18T00:02:00.000Z", "Class": "Class A", "MMSI": 000000000,
              "MsgType": "position_report",
              "Position": {"type": "Point", "coordinates": [55.522592, 15.068637]},
              "Status": "Under way using engine",
              "RoT": 2.2, "SoG": 14.8, "CoG": 62, "Heading": 61}


class TestStringMethods(unittest.TestCase):

    def test_batch_of_ais_insertion(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.insert_batch_of_ais("AISMessages.json")
        self.assertEqual("Number of Insertions: 3", result)

    def test_single_ais_insertion(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.insert_single_ais(test_ais)
        try:
            self.assertEqual("Success: 1", result)
        except:
            self.assertEqual("Failure: 0", result)

    def test_single_ais_insertion_failure(self):
        tmb = main.TrafficMonitoringBackEnd
        result = tmb.insert_single_ais("")
        self.assertEqual("Failure: 0", result)

    def test_insert_batch_and_single(self):
        tmb = main.TrafficMonitoringBackEnd
        batch = tmb.insert_batch_of_ais("AISMessages.json")
        single = tmb.insert_single_ais(test_ais)
        self.assertEqual("Number of Insertions: 3", batch)
        self.assertEqual("Success: 1", single)

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
        self.assertEqual({'MMSI': 375203000, 'Position': {'coordinates': [54.520033, 12.182773]}},
                         positions[0])

    def test_get_recent_vessel_position_mmsi(self):
        x = main.TrafficMonitoringBackEnd
        x.insert_single_ais(unique_ais)
        recent_vessel_position = x.get_recent_vessel_position_mmsi(000000000)
        positions = []
        for position in recent_vessel_position:
            positions.append(position)
        position = positions[0]
        self.assertEqual({'MMSI': 0, 'Position': {'coordinates': [55.522592, 15.068637]}}, position)

    def test_get_recent_vessel_position_mmsi_invalid_data_type(self):
        x = main.TrafficMonitoringBackEnd
        x.insert_single_ais(unique_ais)
        with self.assertRaises(TypeError):
            x.get_permanent_vessel_information("000000000")

    def test_get_permanent_vessel_information_all_attributes(self):
        x = main.TrafficMonitoringBackEnd
        vessels = x.get_permanent_vessel_information(mmsi=235095435, imo=1000019, name="Lady K Ii")
        self.assertEqual({'IMO': 1000019, 'Name': 'Lady K Ii', 'MMSI': 235095435}, vessels[0])

    def test_get_permanent_vessel_information_all_attributes_invalid_mmsi_data_type(self):
        x = main.TrafficMonitoringBackEnd
        with self.assertRaises(TypeError):
            x.get_permanent_vessel_information(mmsi="235095435", imo=1000019, name="Lady K Ii")

    def test_get_permanent_vessel_information_all_attributes_invalid_imo_data_type(self):
        x = main.TrafficMonitoringBackEnd
        with self.assertRaises(TypeError):
            x.get_permanent_vessel_information(mmsi=235095435, imo="1000019", name="Lady K Ii")

    def test_get_permanent_vessel_information_all_attributes_invalid_name_data_type(self):
        x = main.TrafficMonitoringBackEnd
        with self.assertRaises(TypeError):
            x.get_permanent_vessel_information(mmsi=235095435, imo="1000019", name=["Lady K Ii"])

    def test_get_permanent_vessel_information_imo_mmsi(self):
        x = main.TrafficMonitoringBackEnd
        vessels = x.get_permanent_vessel_information(mmsi=235095435, imo=1000019)
        self.assertEqual({'IMO': 1000019, 'Name': 'Lady K Ii', 'MMSI': 235095435}, vessels[0])

    def test_get_permanent_vessel_information_name_mmsi(self):
        x = main.TrafficMonitoringBackEnd
        vessels = x.get_permanent_vessel_information(mmsi=235095435, name="Lady K Ii")
        self.assertEqual({'IMO': 1000019, 'Name': 'Lady K Ii', 'MMSI': 235095435}, vessels[0])

    def test_read_all_ship_positions_with_null_mapview(self):
        x = main.TrafficMonitoringBackEnd
        ship_positions = x.read_all_ship_positions("Hobro", "Denmark")
        message = "Test value is not true."
        self.assertTrue(ship_positions, message)

    def test_read_ship_positions_spelling_mistake(self):
        x = main.TrafficMonitoringBackEnd
        ship_positions = x.read_all_ship_positions("Strueer", "Demark")
        message = "Test value is not true"
        self.assertTrue(ship_positions, message)

    def test_read_all_ship_positions(self):
        tmb = main.TrafficMonitoringBackEnd
        ship_positions = tmb.read_all_ship_positions("Struer", "Denmark")
        self.assertEqual({'coordinates': [56.493048, 8.598582]}, ship_positions[0]['Position'])

    def test_read_all_ship_positions_two(self):
        tmb = main.TrafficMonitoringBackEnd
        ship_positions = tmb.read_all_ship_positions("Flensburg", "Germany")
        self.assertEqual({'coordinates': [54.814258, 9.454653]}, ship_positions[0]['Position'])

    def test_read_positions_with_port_id(self):
        tmb = main.TrafficMonitoringBackEnd
        port = tmb.read_positions_with_id('2977')
        self.assertEqual({'coordinates': [56.493048, 8.598582]}, port[0]['Position'])

    def test_get_png_to_binary(self):
        tmb = main.TrafficMonitoringBackEnd
        binary = tmb.get_tile_png(50371)
        self.assertEqual('110011 111001 1000110 110111 110001 101110 1110000 1101110 1100111'
                         , str(binary))

    def test_get_png_to_binary(self):
        tmb = main.TrafficMonitoringBackEnd
        binary = tmb.get_tile_png(53333)
        self.assertEqual('110100 110001 1000111 110000 110011 101110 1110000 1101110 1100111'
                         , str(binary))

    def test_get_recent_vessel_position_tile(self):
        x = main.TrafficMonitoringBackEnd
        vessel_positions = x.get_recent_vessel_position_tile(5237)
        self.assertEqual({'MMSI': 255805899, 'Position': {'coordinates': [57.478323, 9.329788]}}, vessel_positions[0])

    def test_get_recent_vessel_position_tile_invalid_data_type(self):
        x = main.TrafficMonitoringBackEnd
        with self.assertRaises(TypeError):
            x.get_recent_vessel_position_tile("5237")

    def test_get_last_five_positions_mmsi(self):
        x = main.TrafficMonitoringBackEnd
        x.insert_batch_of_ais("AISMessages3.json")
        recent_vessel_position = x.get_last_five_positions_mmsi(232323237)
        positions = []
        for position in recent_vessel_position:
            positions.append(position)
        self.assertEqual([{'MMSI': 232323237, 'Position': {'coordinates': [55.60658, 8.022727]}},
                          {'MMSI': 232323237, 'Position': {'coordinates': [54.60613, 7.088777]}},
                          {'MMSI': 232323237, 'Position': {'coordinates': [55.522592, 15.062222]}},
                          {'MMSI': 232323237, 'Position': {'coordinates': [55.522592, 15.068637]}},
                          {'MMSI': 232323237, 'Position': {'coordinates': [56.60658, 7.088727]}}], list(positions))

    def test_get_tiles_of_map_tile(self):
        x = main.TrafficMonitoringBackEnd
        mapview_docs = x.get_tiles_of_map_tile(5237)
        self.assertEqual([
            {'id': 52371, 'west': 9.0, 'south': 57.25, 'east': 9.5, 'north': 57.5, 'filename': '43F91.png'},
            {'id': 52372, 'west': 9.5, 'south': 57.25, 'east': 10.0, 'north': 57.5, 'filename': '43F92.png'},
            {'id': 52373, 'west': 9.0, 'south': 57.0, 'east': 9.5, 'north': 57.25, 'filename': '43F93.png'},
            {'id': 52374, 'west': 9.5, 'south': 57.0, 'east': 10.0, 'north': 57.25, 'filename': '43F94.png'}],
            list(mapview_docs))

    def test_get_tiles_of_map_tile_invalid_data_type(self):
        x = main.TrafficMonitoringBackEnd
        with self.assertRaises(TypeError):
            x.get_tiles_of_map_tile("5237")

    def test_read_positions_with_port_name(self):
        tmb = main.TrafficMonitoringBackEnd
        ship_positions = tmb.read_positions_with_port_name("Struer", "Denmark")
        self.assertEqual({'coordinates': [56.493048, 8.598582]}, ship_positions[0]['Position'])
