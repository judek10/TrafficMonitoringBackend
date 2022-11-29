import pymongo
import json

myClient = pymongo.MongoClient("mongodb://localhost:27017")
myDataBase = myClient["AISTestData"]
myCollection = myDataBase["aisdk_20201118"]


class TrafficMonitoringBackEnd:

    def insert_ais(ais_data):
        json_object = json.dumps(ais_data)
        with open("sample.json", "w") as outfile:
            outfile.write(json_object)

        with open("sample.json") as file:
            file_data = json.load(file)

        myCollection.insert_one(file_data)

        return print("Number of Insertions: ")


def main():
    print("hello world")


if __name__ == '__main__':
    main()
