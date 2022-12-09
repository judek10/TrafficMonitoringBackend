import pprint
import pymongo
import json
from datetime import datetime, timedelta

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]
myPorts = myDataBase["ports"]
vessels = myDataBase["vessels"]


class TrafficMonitoringBackEnd:

    def insert_batch_of_ais(ais_data):
        i = 0

        with open(ais_data) as file:
            file_data = json.load(file)

        myCollection.insert_many(file_data)
        i += len(list(file_data))

        return "Number of Insertions: " + str(i)

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
        array_of_ports = []

        if country is not None:
            ports = myPorts.find({"port_location": port_name, "country": country}, {"_id": 0})
            for doc in ports:
                array_of_ports.append(doc)
        else:
            ports = myPorts.find({"port_location": port_name}, {"_id": 0})
            for doc in ports:
                array_of_ports.append(doc)
        return array_of_ports

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


def main():
    x = TrafficMonitoringBackEnd
    vesselList = x.get_permanent_vessel_information(mmsi=235095435, imo=1000019, name="Lady K Ii")
    for i in vesselList:
        print(i)


if __name__ == '__main__':
    main()
