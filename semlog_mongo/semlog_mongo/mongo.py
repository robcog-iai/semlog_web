import itertools
import sys
import time
sys.path.append("../..")
from semlog_mongo.semlog_mongo.utils import *




def compile_optional_data(data):
    optional_data=data.lower().replace(" ","").split(",")
    return_dict={}

    if len(optional_data) != 0:
        for param in optional_data:
            if "label" in param:
                return_dict['label'] = True
            if "crop" in param:
                return_dict['crop'] = True
            if "detection" in param:
                return_dict['detection'] = True
            if "classifier" in param:
                return_dict['classifier'] = True
            if "expand" in param:
                return_dict['expand'] = True
            if "limit" in param:
                return_dict['limit']=int(param.split("=")[1])
    return return_dict
    
def compile_type_data(data):
    image_type_list=[]
    
    data=data.lower()
    if 'color' in data:
        image_type_list.append("Color")
    if 'depth' in data:
        image_type_list.append("Depth")
    if 'mask' in data:
        image_type_list.append("Mask")
    if 'normal' in data:
        image_type_list.append("Normal")
    if 'unlit' in data:
        image_type_list.append("Unlit")
    return image_type_list



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
                if "occl_perc" in param:
                    if ">" in param:
                        return_dict[class_name]["occlusion_gt"]=float(param.split(">")[1])
                    elif "<" in param:
                        return_dict[class_name]["occlusion_lt"]=float(param.split("<")[1])
                elif "img_perc" in param:
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



        elif search_type == "scan":
            query["search_type"] = "scan"
            query['database'] = data[1]
            # Change to scan collection
            query['collection'] = data[1]+".scans"
            query['class'] = data[2].split("+")

        elif search_type == "event":
            query["search_type"] = "event"
            query['database'] = data[1]
            # Change to .vis collection
            query['collection'] = data[2]+".vis"
            query['camera_view'] = data[3].split("+")
            query['timestamp'] = data[4]
            query['class'] = ['Event']
    except Exception as e:
        # All invalid input return false
        return False
    return query


def search_mongo(query_dict,optional_dict,image_type_list, logger, config_path):
    """Search database with query dict.

    Args:
        query_dict (Dict): A compiled dict from website input data.
        optional_dict (Dict): A compiled dict from website optional input data.
        logger (Logger): A Logger instance to record all progress.
        config_path (String): Path for db config file

    Returns:
        pandas.Dataframe: A df instance stores image data.
    """
    ip, username, password = load_mongo_account(config_path)
    logger.write("Query dict: "+str(query_dict))
    logger.write("Optional dict: "+str(optional_dict))
    logger.write("Image types:"+",".join(image_type_list))
    if query_dict["search_type"] == "entity":
        t_start_entity_search=time.time()
        db = query_dict["database"]
        coll_list = query_dict["collection"]
        class_dict = query_dict["class"]
        if 'limit' in optional_dict.keys():
            img_limit=optional_dict['limit']*len(image_type_list)
        if "expand" in optional_dict.keys():
            expand_bones=True
            logger.write("Expand skeletal objects...")
        else:
            expand_bones=False
        db_client = MongoClient(ip, username=username, password=password)[db]
        result = []

        logger.write("Enter entity search.")
        logger.write("Enter database: "+db)
        for coll in coll_list:
            t_coll_start_time=time.time()
            coll=coll+".vis"
            logger.write("Enter collection: "+coll)
            # Change to .vis collection
            client = db_client[coll]
            for class_name,param_dict in class_dict.items():
                t_class_start_time=time.time()
                logger.write("Search class: "+class_name)
                if param_dict!={}:
                    logger.write("Parameter dict: "+str(optional_dict))
                result.extend(search_one(
                    client, class_name,param_dict, image_type_list=image_type_list,expand_bones=expand_bones))
                t_class_end_time=time.time()
                t_class_time=convert_duration_time(t_class_end_time,t_class_start_time)
                logger.write("Class: "+class_name+" finished with: "+t_class_time+"s.")
            t_coll_end_time=time.time()
            t_coll_time=convert_duration_time(t_coll_end_time,t_coll_start_time)
            logger.write("Collection: "+coll+" finished with: "+t_coll_time+"s.")
        t_end_entity_search=time.time()
        t_entity_search=convert_duration_time(t_end_entity_search,t_start_entity_search)
        logger.write("Entity search finished with: "+t_entity_search+ "s.")
        if len(result) == 0:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(result)
            df['file_id'] = df['file_id'].astype(str)
            df[['x_min', 'x_max', 'y_min', 'y_max']] = df[[
                'x_min', 'x_max', 'y_min', 'y_max']].astype('int32')
    elif query_dict["search_type"] == "scan":
        logger.write("Enter scan search.")
        t_start_scan_search=time.time()
        db = query_dict["database"]
        coll = query_dict["collection"]
        class_list = query_dict["class"]
        df = scan_search(db, coll, class_list, image_type_list, config_path)
        logger.write("Scan search finished with "+convert_duration_time(time.time(),t_start_scan_search)+"s.")
    elif query_dict["search_type"] == "event":
        logger.write("Enter event search.")
        t_start_event_search=time.time()
        db = query_dict["database"]
        coll = query_dict["collection"]
        camera_view_list = query_dict['camera_view']
        timestamp = query_dict['timestamp']
        df = event_search(db, coll, timestamp, camera_view_list, config_path)
        logger.write("Event search finished with "+convert_duration_time(time.time(),t_start_event_search)+"s.")
    
    if "limit" in optional_dict.keys() and query_dict['search_type']=="entity":
        unique_img_list=[]
        for i, row in df.iterrows():
            if row['file_id'] not in unique_img_list:
                unique_img_list.append(row['file_id'])
            if len(unique_img_list)>img_limit:
                break
        new_df=df[:i]
        unique_documents=new_df.document.unique()
        df=df[df.document.isin(unique_documents)]
    logger.write("Length of results: "+str(df.shape[0]))
    return df
