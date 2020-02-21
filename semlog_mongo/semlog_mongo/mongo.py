import itertools
import sys
sys.path.append("../..")
from semlog_mongo.semlog_mongo.utils import *




def compile_query(data):

    def compile_class_list(class_info):
        class_list=class_info.split("+")
        return_dict={}
        for each_class in class_list:
            info=each_class.split("(")
            class_name=info[0]
            return_dict[class_name]={}

            # If no optional input
            if "(" not in each_class:
                continue
                
            params=info[1].replace(")","")
            param_list=params.split(";") 
            for param in param_list:
                if "occlusion" in param:
                    if ">" in param:
                        return_dict[class_name]["occlusion_gt"]=float(param.split(">")[1])
                    elif "<" in param:
                        return_dict[class_name]["occlusion_lt"]=float(param.split("<")[1])
                elif "size" in param:
                    if ">" in param:
                        return_dict[class_name]["size_gt"]=float(param.split(">")[1])
                    elif "<" in param:
                        return_dict[class_name]["size_lt"]=float(param.split("<")[1])
                elif "clipped" in param:
                    return_dict[class_name]['clipped']=False if param.split("=")[1] == "false" else True

        return return_dict

    try:
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
            query['class'] = compile_class_list(data[4])
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
            # Change to scan collection
            query['collection'] = data[1]+".scans"
            query['class'] = data[2].split("+")
            query['type'] = data[3].split("+")

        elif search_type == "event":
            query["search_type"] = "event"
            query['database'] = data[1]
            # Change to .vis collection
            query['collection'] = data[2]+".vis"
            query['camera_view'] = data[3]
            query['timestamp'] = data[4]
            query['class'] = ['Event']
            query['type'] = data[5].split("+")
    except Exception as e:
        # All invalid input return false
        return False
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
        class_dict = query_dict["class"]
        image_type_list = query_dict['type']
        db_client = MongoClient(ip, username=username, password=password)[db]
        result = []

        logger.write("Enter entity search.")
        logger.write("Database: "+db)
        for coll in coll_list:
            coll=coll+".vis"
            logger.write("Collection: "+coll)
            # Change to .vis collection
            client = db_client[coll]
            for class_name,optional_dict in class_dict.items():
                logger.write("Search class: "+class_name)
                if optional_dict!={}:
                    logger.write("Parameter dict: "+str(optional_dict))
                result.extend(search_one(
                    client, class_name,optional_dict, image_type_list=image_type_list))
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
