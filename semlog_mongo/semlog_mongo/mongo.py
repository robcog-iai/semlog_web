import itertools
import sys
import time
sys.path.append("../..")
from semlog_mongo.semlog_mongo.utils import *




def compile_optional_data(data):
    """Compile Optional input.
    
    Args:
        data (str): Input string from website.
    
    Returns:
        dict: An optional dict recording target parameters.
    """
    optional_data=data.lower().replace(" ","").split(",")
    return_dict={}

    if len(optional_data) != 0:
        for param in optional_data:
            if "label_classes" in param:
                return_dict['label'] = True
            if "crop" in param:
                return_dict['crop'] = True
            if "detection" in param:
                return_dict['detection'] = True
            if "classifier" in param:
                return_dict['classifier'] = True
            if "label_bones" in param:
                return_dict['expand'] = True
            if "limit" in param:
                return_dict['limit']=int(param.split("=")[1])
            if "use_and_operator" in param:
                return_dict['and'] =True
    return return_dict
    
def compile_type_data(data):
    """Compile type input from website.
    
    Args:
        data (str): Input string from the website.
    
    Returns:
        list: A image type list.
    """
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
    """Compile query input.
    
    Args:
        data (str): Query input string.
    
    Returns:
        dict: A information dict after compiling.
    """

    def compile_class_list(class_info):
        """Compile classes with optional params
        
        Args:
            class_info (str): A string of classes.
        
        Returns:
            dict: A class dict with params.
        """
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
        copy_data=data
        data = data.split(" ")
        optional_data = data[1:]
        data = data[0].split("@")
        search_type = data[0]
        query = {}

        if search_type == "entity":
            query["search_type"] = "entity"
            query['database'] = data[1]
            query['collection'] = data[2].split("+")
            query['logic'] = 'or'
            query['class'] = compile_class_list(data[3])

        elif search_type == "scan":
            query["search_type"] = "scan"
            query['database'] = data[1]
            # Change to scan collection
            query['collection'] = data[1]+".scans"
            query['class'] = data[2].split("+")

        elif search_type == "event":
            query["search_type"] = "event"
            query["event_list"]=[]
            # Remove optional params, whitespace and the first event@
            data=copy_data.split(" ")[0].replace(" ","")[6:]
            # if "&" in data:
            event_list=data.split("&")
            for each_event in event_list:
                return_dict={}
                event_params=each_event.split("@")
                if len(event_params)==3:
                    each_event+="@0.1"
                    event_params=each_event.split("@")
                return_dict['database']=event_params[0]
                return_dict['collection']=event_params[1]+".vis"
                return_dict['camera_view']=event_params[2].split("+")
                # add dummy timestamp
                if "+" not in event_params[3]:
                    return_dict['timestamp']=event_params[3]
                    query['event_list'].append(return_dict)
                else:
                    time_list=event_params[3].split("+")
                    for each_time in time_list:
                        return_dict['timestamp']=each_time
                        query['event_list'].append({"database":event_params[0],'collection':event_params[1]+".vis",
                        "camera_view":event_params[2].split("+"),"timestamp":each_time})
            query['class'] = ['Event']
    except Exception as e:
        print(e)
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
    if query_dict["search_type"] == "entity":
        t_start_entity_search=time.time()
        db = query_dict["database"]
        coll_list = query_dict["collection"]
        class_dict = query_dict["class"]
        if 'limit' in optional_dict.keys():
            img_limit=optional_dict['limit']
        else:
            img_limit=None
        if "expand" in optional_dict.keys():
            expand_bones=True
            logger.write("expand skeletal objects...")
        else:
            expand_bones=False
        db_client = MongoClient(ip, username=username, password=password)[db]
        result = []

        logger.write("start search...")
        logger.write("enter database: "+db)

        for coll in coll_list:
            t_coll_start_time=time.time()
            coll=coll+".vis"
            logger.write("start search in episode: "+coll)
            # Change to .vis collection
            client = db_client[coll]
            meta_client=db_client[db+".meta"]

            if "and" in optional_dict.keys():
                r,skel_list=and_search(client,meta_client,list(class_dict.keys()),image_type_list=image_type_list,limit=img_limit)
                result=compile_and_search(r,skel_list,expand_bones)

            else:
                skel_list=[]
                for class_name,param_dict in class_dict.items():
                    flag_is_skel=check_skel(meta_client,class_name)
                    if flag_is_skel:
                        skel_list.append(class_name)
                    t_class_start_time=time.time()
                    logger.write("start search for: "+class_name)


                    result.extend(search_one(
                        client, class_name,param_dict, image_type_list=image_type_list,expand_bones=expand_bones))


                    t_class_end_time=time.time()
                    t_class_time=convert_duration_time(t_class_end_time,t_class_start_time)
                    logger.write("search for: "+class_name+" finished. ("+t_class_time+"s)")
            t_coll_end_time=time.time()
            t_coll_time=convert_duration_time(t_coll_end_time,t_coll_start_time)
            logger.write("search in episode: "+coll+" finished. ("+t_coll_time+"s)")
            t_end_entity_search=time.time()
            t_entity_search=convert_duration_time(t_end_entity_search,t_start_entity_search)
            logger.write("entity search finished. ("+t_entity_search+ "s)")
        if len(result) == 0:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(result)

                # if len(skel_list)!=0 and "expand" in optional_dict.keys():
                #     bone_list=[]
                #     for each_skel in skel_list:
                #         bone_list.extend(get_bones_from_skel(meta_client,each_skel))
                #     class_list=list(query_dict['class'].keys())
                #     df=find_conjunct_images_from_df(df,class_list,bone_list=bone_list)
                # else:
                #     df=find_conjunct_images_from_df(df,list(query_dict['class'].keys()))
                # print(df.shape)
            df['file_id'] = df['file_id'].astype(str)
            df[['x_min', 'x_max', 'y_min', 'y_max']] = df[[
                'x_min', 'x_max', 'y_min', 'y_max']].astype('int32')
    elif query_dict["search_type"] == "scan":
        logger.write("starting scan search...")
        t_start_scan_search=time.time()
        db = query_dict["database"]
        coll = query_dict["collection"]
        class_list = query_dict["class"]
        df = scan_search(db, coll, class_list, image_type_list, config_path)
        logger.write("scan search finished. ("+convert_duration_time(time.time(),t_start_scan_search)+"s)")
    elif query_dict["search_type"] == "event":
        logger.write("start event search...")
        t_start_event_search=time.time()
        event_list=query_dict['event_list']
        frames=[]
        print(query_dict)
        for each_event in event_list:
            db = each_event["database"]
            coll = each_event["collection"]
            camera_view_list = each_event['camera_view']
            if 'timestamp' in each_event.keys():
                timestamp = each_event['timestamp']
            else:
                timestamp=0.1
            df = event_search(db, coll, timestamp, camera_view_list, config_path)
            if not df.empty:
                frames.append(df)
        if frames==[]:
            return pd.DataFrame()
        else:
            df=pd.concat(frames)
        logger.write("event search finished. ("+convert_duration_time(time.time(),t_start_event_search)+"s)")
    

    if "limit" in optional_dict.keys() and query_dict['search_type']=="entity" and df.shape[0]!=0:
        df=df.sort_values(by=['file_id'])
        df=df.reset_index()
        unique_img_list=[]
        final_ind=1
        for i, row in df.iterrows():
            if row['file_id'] not in unique_img_list:
                unique_img_list.append(row['file_id'])
            if len(unique_img_list)>img_limit:
                break
        new_df=df[:i]
        df=new_df
        # unique_documents=new_df.document.unique()
        # df=df[df.document.isin(unique_documents)]

    if df.shape[0]==0:
        logger.write("found 0 images.")
    else:
        logger.write("found "+str(df['file_id'].value_counts().shape[0])+" images.")
    return df   
