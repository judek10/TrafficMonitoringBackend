import pymongo
import json

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

        return print("Number of Insertions: " + str(i))


def main():
    print("hello world")


if __name__ == '__main__':
    main()
