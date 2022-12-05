import datetime

import pymongo
import json
from datetime import datetime, timedelta

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]


class TrafficMonitoringBackEnd:

    def insert_ais(ais_data):
        i = 0

        with open(ais_data) as file:
            file_data = json.load(file)

        if isinstance(file_data, list):
            myCollection.insert_many(file_data)
            i += len(list(file_data))
        else:
            myCollection.insert_one(file_data)
            i = 1

        return "Number of Insertions: " + str(i)

    def delete_ais_by_timestamp(current_time):
        date_format = "%Y-%m-%d %H:%M:%S.%f"
        given_time = datetime.strptime(current_time, date_format)

        subtracted_current_time = given_time - timedelta(minutes=5)
        subtracted_current_time = subtracted_current_time.isoformat()[:-3] + 'Z'

        return "Number of Deletions: " + str(
            myCollection.delete_many({"Timestamp": {"$lt": subtracted_current_time}}).deleted_count)

    def get_recent_vessel_positions(self):
        vessel_positions = myCollection.find({}, {"_id": 0, "MMSI": 1, "Position.coordinates": 1})\
            .sort('Timestamp', pymongo.DESCENDING).limit(3)
        return vessel_positions

    def get_recent_vessel_position_mmsi(mmsi):
        return myCollection.find({"MMSI": {"$eq": mmsi}}, {"_id":0, "MMSI": 1, "Position.coordinates": 1})\
            .sort('Timestamp', pymongo.DESCENDING).limit(1)


def main():
    print("hello world")


if __name__ == '__main__':
    main()
