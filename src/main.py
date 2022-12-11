import base64
import pymongo
import json
from datetime import datetime, timedelta

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]
myPorts = myDataBase["ports"]
myMapViews = myDataBase["mapviews"]
vessels = myDataBase["vessels"]


class TrafficMonitoringBackEnd:

    def insert_batch_of_ais(ais_data):
        insertion_number = 0

        with open(ais_data) as file:
            file_data = json.load(file)

        myCollection.insert_many(file_data)
        insertion_number += len(list(file_data))

        return "Number of Insertions: " + str(insertion_number)

    def insert_single_ais(ais_data):
        try:
            myCollection.insert_one(ais_data)
            return "Success: 1"
        except:
            return "Failure: 0"

    def delete_ais_by_timestamp(current_time):
        date_format = "%Y-%m-%d %H:%M:%S.%f"
        given_time = datetime.strptime(current_time, date_format)

        subtracted_current_time = given_time - timedelta(minutes=5)
        subtracted_current_time = subtracted_current_time.isoformat()[:-3] + 'Z'

        return "Number of Deletions: " + str(
            myCollection.delete_many({"Timestamp": {"$lt": subtracted_current_time}}).deleted_count)

    def get_recent_vessel_positions(self):
        vessel_positions = myCollection.find({}, {"_id": 0, "MMSI": 1, "Position.coordinates": 1}) \
            .sort('Timestamp', pymongo.DESCENDING).limit(3)
        return vessel_positions

    def get_recent_vessel_position_mmsi(mmsi):
        return myCollection.find({"MMSI": {"$eq": mmsi}}, {"_id": 0, "MMSI": 1, "Position.coordinates": 1}) \
            .sort('Timestamp', pymongo.DESCENDING).limit(1)

    def find_all_ports(port_name, country=None):
        ports_list = []

        if country is not None:
            port = myPorts.find({"port_location": port_name, "country": country}, {"_id": 0})
            for doc in port:
                ports_list.append(doc)
        else:
            port = myPorts.find({"port_location": port_name}, {"_id": 0})
            for doc in port:
                ports_list.append(doc)
        return ports_list

    def get_permanent_vessel_information(mmsi, imo=None, name=None):
        if imo or name is not None:
            if imo is None:
                return vessels.find({"MMSI": {"$eq": mmsi}, "Name": {"$eq": name}},
                                    {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})
            elif name is None:
                return vessels.find({"MMSI": {"$eq": mmsi}, "IMO": {"$eq": imo}},
                                    {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})
            else:
                return vessels.find({"MMSI": {"$eq": mmsi}, "IMO": {"$eq": imo}, "Name": {"$eq": name}},
                                    {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})
        else:
            return vessels.find({"MMSI": {"$eq": mmsi}}, {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})

    def read_all_ship_positions(port_name, country):
        tile_id = myPorts.find({"port_location": port_name, "country": country}
                               , {"mapview_3": 1, "_id": 0})
        tile_id_list = []
        for doc in tile_id:
            tile_id_list.append(doc)

        if not tile_id_list or tile_id_list[0]["mapview_3"] is None:
            port_documents = myPorts.find({}, {"_id": 0, "un/locode": 0, "website": 0})
            ports_list = []
            for doc in port_documents:
                ports_list.append(doc)
            return ports_list
        else:
            mapview_tile = list(
                myMapViews.find({"id": tile_id_list[0]["mapview_3"]},
                                {"west": 1, "south": 1, "east": 1, "north": 1, "_id": 0}))

            cardinal_directions = []
            for doc in mapview_tile:
                cardinal_directions.append(doc)
            ship_positions = myCollection.find({"Position.coordinates.0": {"$gte": cardinal_directions[0]["south"],
                                                                           "$lte": cardinal_directions[0]["north"]},
                                                "Position.coordinates.1": {"$gte": cardinal_directions[0]["west"],
                                                                           "$lte": cardinal_directions[0]["east"]}},
                                               {"Position.coordinates": 1, "_id": 0})
            ship_positions_list = []
            for doc in ship_positions:
                ship_positions_list.append(doc)
            return ship_positions_list

    def get_tile_png(mapview_id):
        x = myMapViews.find({"id": mapview_id})
        y = [document["filename"] for document in x]
        with open(y.pop(), "rb") as f:
            png_encoded = base64.b64encode(f.read())
            encoded_b2 = "".join([format(n, '08b') for n in png_encoded])
        return encoded_b2


def main():
    x = TrafficMonitoringBackEnd
    vesselList = x.get_permanent_vessel_information(mmsi=235095435, imo=1000019, name="Lady K Ii")

    for i in vesselList:
        print(i)
    print(x.get_tile_png(50383))


if __name__ == '__main__':
    main()
