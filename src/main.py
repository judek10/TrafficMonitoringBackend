"""A TMB (Traffic Monitoring Backend) that can store AIS data in the mongo database

   Can also perform vendor-specific queries that implement functions such as deletion,
   insertion, and query searches
   module authors:: Jude, Ashley, and Gerardo
"""

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
    """A class that stores the methods for the TMB"""

    def insert_batch_of_ais(ais_data):
        """takes a batch of AIS data (json file) and inserts this data into mongoDB

        param ais_data: file object that stores the to be inserted AIS data
        :type ais_data: json file
        :return: the number of documents inserted into the collection
        :rtype: str
        """

        insertion_number = 0

        with open(ais_data) as file:
            file_data = json.load(file)

        myCollection.insert_many(file_data)
        insertion_number += len(list(file_data))

        return "Number of Insertions: " + str(insertion_number)

    def insert_single_ais(ais_data):
        """inserts an AIS report (static data or position) into the collection.

        param ais_data: a json formatted string that is to be inserted
        :type ais_data: str
        :return: 'Success: 1' for successful insertion or 'Failure: 0' for failure
        :rtype: str
        """

        try:
            myCollection.insert_one(ais_data)
            return "Success: 1"
        except:
            return "Failure: 0"

    def delete_ais_by_timestamp(current_time):
        """deletes all AIS messages whose timestamp is 5 min older than current time

        param current_time: current time formatted in UTC ISO
        :type current_time: str
        :return: numbers of deletions
        """

        date_format = "%Y-%m-%d %H:%M:%S.%f"
        given_time = datetime.strptime(current_time, date_format)

        subtracted_current_time = given_time - timedelta(minutes=5)
        subtracted_current_time = subtracted_current_time.isoformat()[:-3] + 'Z'

        return "Number of Deletions: " + str(
            myCollection.delete_many({"Timestamp": {"$lt": subtracted_current_time}}).deleted_count)

    def get_recent_vessel_positions(self):
        """get recent vessel information
            searches for most recent vessel position reports and
            retrieves the corresponding vessel documents for the position reports
                                        :return: array of vessel documents
                                        :rtype: array
                                        """
        vessel_positions = myCollection.find({}, {"_id": 0, "MMSI": 1, "Position.coordinates": 1}) \
            .sort('Timestamp', pymongo.DESCENDING)
        return vessel_positions

    def get_recent_vessel_position_mmsi(mmsi):
        """given an MMSI and optional IMO and name values, get permanent vessel information
            searches for vessels with matching MMSI, imo, and name, then retrieves the corresponding vessel document
                                :param mmsi, optional imo and/or name
                                :type mmsi:int, imo:int, name:string
                                :return: vessel object
                                :rtype: vessel object
                                """
        if isinstance(mmsi, int):
            return myCollection.find({"MMSI": {"$eq": mmsi}}, {"_id": 0, "MMSI": 1, "Position.coordinates": 1}) \
                .sort('Timestamp', pymongo.DESCENDING).limit(1)
        else:
            raise TypeError('MMSI must be an integer')

    def find_all_ports(port_name, country=None):
        """finds all ports with the given param: port_name and country(optional).

        if no country is given, then a search with find all ports with the given country,
        then append the documents into a list that is returned.
        if a country is given in the parameter, the steps from above are done, only
        difference being is that the search is done with both country and port name.
        param port_name: port name to be searched
        :type port_name: str
        :param country: country of port to be searched
        :type country: str
        :return: an array that contains all documents that fit the criteria
        :rtype: array
        """

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
        """given an MMSI and optional IMO and name values, get permanent vessel information

                searches for vessels with matching MMSI, imo, and name, then retrieves the corresponding vessel document
                        :param mmsi, optional imo and/or name
                        :type mmsi:int, imo:int, name:string
                        :return: vessel object
                        :rtype: vessel object
                        """
        if isinstance(mmsi, int):
            if imo or name is not None:
                if imo is None:
                    if isinstance(name, str):
                        return vessels.find({"MMSI": {"$eq": mmsi}, "Name": {"$eq": name}},
                                            {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})
                    else:
                        raise TypeError('name must be a string')

                elif name is None:
                    if isinstance(imo, int):
                        return vessels.find({"MMSI": {"$eq": mmsi}, "IMO": {"$eq": imo}},
                                            {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})
                    else:
                        raise TypeError('imo must be an int')
                else:
                    if isinstance(imo, int) and isinstance(name, str):
                        return vessels.find({"MMSI": {"$eq": mmsi}, "IMO": {"$eq": imo}, "Name": {"$eq": name}},
                                            {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})
                    else:
                        raise TypeError('incorrect datatype(s)')
            else:
                return vessels.find({"MMSI": {"$eq": mmsi}}, {"_id": 0, "MMSI": 1, "Name": 1, "IMO": 1})
        else:
            raise TypeError('MMSI must be an integer')




    def read_all_ship_positions(port_name, country):
        """read all ship positions in the tile of scale 3 containing given port

        takes the port name and country to find port, takes the mapview_3 id
        to search for tile 3 size, and return all ship positions within
        that tile.
        param port_name: the port name of port to be searched
        :type port_name: str
        :param country: country of port to be searched
        :type country: str
        :return: array of position reports, otherwise, an array or port docs
        :rtype: array
        """

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

    def get_recent_vessel_position_tile(tileId):
        """given a tile id, get the recent vessel positions within the tile

                searches for mapview tile with id, then retrieves the recent vessel positions inside the tile.
                :param mapview_id:
                :type mapview_id:
                :return: array of ship documents
                :rtype: array
                """
        if isinstance(tileId, int):
            map_view_coordinates = myMapViews.find({"id": tileId},
                                                   {"_id": 0, "west": 1, "east": 1, "north": 1, "south": 1})
            ship_positions = myCollection.find({"Position.coordinates.0": {"$gte": map_view_coordinates[0]["south"],
                                                                           "$lte": map_view_coordinates[0]["north"]},
                                                "Position.coordinates.1": {"$gte": map_view_coordinates[0]["west"],
                                                                           "$lte": map_view_coordinates[0]["east"]}},
                                               {"_id": 0, "Position.coordinates": 1, "MMSI": 1, "Name": 1, "IMO": 1})
            return ship_positions
        else:
            raise TypeError('tileId must be an integer')

    def read_positions_with_id(port_id):
        """read most recent positions of ships headed to port with port id

        takes the parameter of port id to search for port, then takes the
        mapview id to do another search that retrieves the tile size.
        After retrieving the measurements a search is done to find all
        positions within that area, and returned in an array
        :param port_id:
        :type port_id:
        :return: array containing all the ship positions found within the searched port
        :rtype: array
        """
        tile_id = myPorts.find({"id": port_id}
                               , {"mapview_3": 1, "_id": 0})
        tile_id_list = []
        for doc in tile_id:
            tile_id_list.append(doc)

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
        """given a tile id, gets the actual tile data in binary

        searches for mapview tile with id, then converts the png
        into binary data to return to user
        :param mapview_id:
        :type mapview_id:
        :return: binary data of png file
        :rtype: str
        """
        get_filename = myMapViews.find({"id": mapview_id}, {"_id": 0, "filename": 1})\
            .sort('Timestamp', pymongo.DESCENDING)
        filename = []
        for doc in get_filename:
            filename.append(doc)
        to_binary = " ".join(format(ord(c), "b") for c in filename[0]["filename"])
        return to_binary

    def get_last_five_positions_mmsi(mmsi):
        """given an MMSI and or IMO/name values, it will get the permanent vessel information and
            searches for vessels with matching MMSI, imo/name, then gets the corresponding vessel document of
            the coordinates and the last five positions of the vessel
            :param mmsi, optional imo or name
            :type mmsi:int, imo:int, name:string
            :return: vessel object
            :rtype: vessel object
            """
        return myCollection.find({"MMSI": {"$eq": mmsi}}, {"_id": 0, "MMSI": 1, "Position.coordinates": 1}) \
            .sort('Timestamp', pymongo.DESCENDING).limit(5)

    def get_tiles_of_map_tile(mapview_id):
        """given a mapview id of zoom level 1, gets the 4 tiles contained in the mapview id's map tile
                :param mapview_id:
                :type mapview_id:
                :return: array of mapview documents
                :rtype: array
                """
        if isinstance(mapview_id, int):
            return myMapViews.find({"contained_by": mapview_id}, {"_id": 0, "id": 1, "west": 1, "south": 1,
                                                              "east": 1, "north": 1, "filename": 1})
        else:
            raise TypeError("mapview_id must be an integer")

    def read_positions_with_port_name(port_name, country):

        tile_id = myPorts.find({"port_location": port_name, "country": country}, {"mapview_3": 1, "_id": 0})

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

def main():
    x = TrafficMonitoringBackEnd
    vessel_positions = x.get_tiles_of_map_tile(5237)
    for vessel in vessel_positions:
        print(vessel)


if __name__ == '__main__':
    main()
