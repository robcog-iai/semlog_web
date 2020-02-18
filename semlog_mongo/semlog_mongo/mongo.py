import itertools
import sys
sys.path.append("../..")
from semlog_mongo.semlog_mongo.utils import *


def compile_query(data):
    data = data.split(" ")
    optional_data = data[1:]
    data = data[0].split("@")
    search_type = data[0]
    query = {}

    if search_type == "entity":
        query["search_type"] = "entity"
        query['database'] = data[1]
        query['collection'] = data[2].split("+")
        query['logic'] = data[3]
        query['class'] = data[4].split("+")
        query['type'] = data[5].split("+")

        if len(optional_data) != 0:
            if "label" in optional_data:
                query['label'] = True
            if "crop" in optional_data:
                query['crop'] = True
            if "detection" in optional_data:
                query['detection'] = True
            if "classifier" in optional_data:
                query['classifier'] = True

    elif search_type == "scan":
        query["search_type"] = "scan"
        query['database'] = data[1]
        query['collection'] = data[1]+".meta"
        query['class'] = data[2].split("+")
        query['type'] = data[3].split("+")

    elif search_type == "event":
        query["search_type"] = "event"
        query['database'] = data[1]
        query['collection'] = data[2]
        query['camera_view'] = data[3]
        query['timestamp'] = data[4]
        query['class'] = ['Event']
        query['type'] = data[5].split("+")
    return query


def search_mongo(query_dict, logger, config_path):
    """Search database with query dict.

    Args:
        query_dict (Dict): A compiled dict from website input data.
        logger (Logger): A Logger instance to record all progress.
        config_path (String): Path for db config file

    Returns:
        pandas.Dataframe: A df instance stores image data.
    """
    ip, username, password = load_mongo_account(config_path)
    if query_dict["search_type"] == "entity":
        db = query_dict["database"]
        coll_list = query_dict["collection"]
        class_list = query_dict["class"]
        image_type_list = query_dict['type']
        db_client = MongoClient(ip, username=username, password=password)[db]
        result = []

        logger.write("Enter entity search.")
        logger.write("Database: "+db)
        for coll in coll_list:
            logger.write("Collection: "+coll)
            client = db_client[coll]
            for _class in class_list:
                logger.write("Class: "+_class)
                result.extend(search_one(
                    client, _class, image_type_list=image_type_list))
                logger.write("Length of results: "+str(len(result)))
        if len(result) == 0:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(result)
            df['file_id'] = df['file_id'].astype(str)
            df[['x_min', 'x_max', 'y_min', 'y_max']] = df[[
                'x_min', 'x_max', 'y_min', 'y_max']].astype('int32')
    elif query_dict["search_type"] == "scan":
        db = query_dict["database"]
        coll = query_dict["collection"]
        class_list = query_dict["class"]
        image_type_list = query_dict['type']
        df = scan_search(db, coll, class_list, image_type_list, config_path)
    elif query_dict["search_type"] == "event":
        db = query_dict["database"]
        coll = query_dict["collection"]
        camera_view = query_dict['camera_view']
        timestamp = query_dict['timestamp']
        image_type_list = query_dict['type']
        df = event_search(db, coll, timestamp, camera_view, config_path)

    return df
