import pymongo
from pymongo import MongoClient
import pprint
import pandas as pd
import gridfs
import os
from bson.objectid import ObjectId


def load_mongo_account(config_path):
    """Load account info from a local file."""
    config=eval(open(config_path,'r').read())
    ip=config['ip']
    username=config['username']
    password=config['password']
    return ip,username,password

def check_mongodb_state(config_path):
    """Check if local mongodb server is available."""
    ip,username,password=load_mongo_account(config_path)
    client=pymongo.MongoClient(ip,username=username,password=password,serverSelectionTimeoutMS=100)
    try:
        print(client.list_database_names())
        return True
    except Exception as e:
        return False



def search_entities(client,object_identification,object_pattern='class',image_type_list=None,view_id_list=[]):
    """Principle searching method to search images that contains one entity.
    
    Args:
        client (pymongo.MongoClient): Client to connect database.
        object_identification (String): Name of this object
        object_pattern (str, optional): Search for id or class. Defaults to 'class'.
        image_type_list (List, optional): List of target image types. Defaults to None.
        view_id_list (list, optional): List for target camera views. Defaults to [].
    
    Returns:
        List: Result of qualified image data.
    """
    pipeline=[]

    # Remove docs without vision data
    pipeline.append({"$match":{"vision":{"$exists":1}}})

    # Unwind views for filtering multiple camera views
    pipeline.append({"$unwind":{"path":"$vision.views"}})

    # Right now only view name is supported, can be extended to view id easily
    if view_id_list !=[]:
        # Change class to id for view id filtering
        pipeline.append({"$match":{"vision.views.class":{"$in":view_id_list}}})

    if object_pattern=='class':
        match_key="vision.views.entities.class"
    else:
        match_key="vision.views.entities.id"
    # Find docs with the given class name or object id
    pipeline.append({"$unwind":{"path":"$vision.views.entities"}})
    pipeline.append({"$match":{match_key:object_identification}})

    # Add info to each image
    pipeline.append({"$addFields": {

                                # Add db/cols
                                "vision.views.images.database": client.database.name,
                                "vision.views.images.collection": client.name,

                                # Add object info
                                "vision.views.images.object": "$vision.views.entities.id",
                                "vision.views.images.class": "$vision.views.entities.class",
                                "vision.views.images.percentage": "$vision.views.entities.img_perc",
                                "vision.views.images.x_min": "$vision.views.entities.img_bb.min.x",
                                "vision.views.images.y_min": "$vision.views.entities.img_bb.min.y",
                                "vision.views.images.x_max": "$vision.views.entities.img_bb.max.x",
                                "vision.views.images.y_max": "$vision.views.entities.img_bb.max.y",

                                # Add document id
                                "vision.views.images.document": "$_id"}})

    # Remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$vision.views"}})

    # Add image type filter
    pipeline.append({"$unwind": {"path": "$images"}})
    if image_type_list is not None:
        or_list = {"$match": {"$or": []}}
        for image_type in image_type_list:
            or_list["$match"]["$or"].append({"images.type": image_type})
        pipeline.append(or_list)

    # Again remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$images"}})



    result=list(client.aggregate(pipeline))

    return result

def search_skel(client,object_identification,object_pattern='class',image_type_list=None,view_id_list=[]):
    """Search for skeletal object.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
        object_identification (String): Name of the object
        object_pattern (str, optional): Id or class. Defaults to 'class'.
        image_type_list (list, optional): List of targe image types. Defaults to None.
        view_id_list (list, optional): List of qualified camera views. Defaults to [].
    
    Returns:
        List: Result of qualified image data.
    """
    pipeline=[]

    # Remove docs without vision data
    pipeline.append({"$match":{"vision":{"$exists":1}}})

    # Unwind views for filtering multiple camera views
    pipeline.append({"$unwind":{"path":"$vision.views"}})

    # Right now only view name is supported, can be extended to view id easily
    if view_id_list != []:
        # Change class to id for view id filtering
        pipeline.append({"$match":{"vision.views.class":{"$in":view_id_list}}})

    if object_pattern=='class':
        match_key="vision.views.skel_entities.class"
    else:
        match_key="vision.views.skel_entities.id"
    # Find docs with the given class name or object id
    pipeline.append({"$unwind":{"path":"$vision.views.skel_entities"}})
    pipeline.append({"$match":{match_key:object_identification}})

    # Add info to each image
    pipeline.append({"$addFields": {

                                # Add db/cols
                                "vision.views.images.database": client.database.name,
                                "vision.views.images.collection": client.name,

                                # Add object info
                                "vision.views.images.object": "$vision.views.skel_entities.id",
                                "vision.views.images.class": "$vision.views.skel_entities.class",
                                "vision.views.images.percentage": "$vision.views.skel_entities.img_perc",
                                "vision.views.images.x_min": "$vision.views.skel_entities.img_bb.min.x",
                                "vision.views.images.y_min": "$vision.views.skel_entities.img_bb.min.y",
                                "vision.views.images.x_max": "$vision.views.skel_entities.img_bb.max.x",
                                "vision.views.images.y_max": "$vision.views.skel_entities.img_bb.max.x",

                                # Add document id
                                "vision.views.images.document": "$_id"}})

    # Remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$vision.views"}})

    # Add image type filter
    pipeline.append({"$unwind": {"path": "$images"}})
    if image_type_list is not None:
        or_list = {"$match": {"$or": []}}
        for image_type in image_type_list:
            or_list["$match"]["$or"].append({"images.type": image_type})
        pipeline.append(or_list)

    # Again remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$images"}})


    result=list(client.aggregate(pipeline))

    return result

def search_bones(client,object_identification,object_pattern='class',image_type_list=None,view_id_list=[]):
    """Search for bone object.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
        object_identification (String): Name of the object
        object_pattern (str, optional): Id or class. Defaults to 'class'.
        image_type_list (list, optional): List of targe image types. Defaults to None.
        view_id_list (list, optional): List of qualified camera views. Defaults to [].
    
    Returns:
        List: Result of qualified image data.
    """

    pipeline=[]

    # Remove docs without vision data
    pipeline.append({"$match":{"vision":{"$exists":1}}})

    # Unwind views for filtering multiple camera views
    pipeline.append({"$unwind":{"path":"$vision.views"}})

    # Right now only view name is supported, can be extended to view id easily
    if view_id_list !=[]:
        # Change class to id for view id filtering
        pipeline.append({"$match":{"vision.views.class":{"$in":view_id_list}}})

    if object_pattern=='class':
        match_key="vision.views.skel_entities.bones.class"
    else:
        match_key="vision.views.skel_entities.bones.id"
    # Find docs with the given class name or object id
    pipeline.append({"$unwind":{"path":"$vision.views.skel_entities"}})
    # Expand bones with unwind
    pipeline.append({"$unwind":{"path":"$vision.views.skel_entities.bones"}})
    pipeline.append({"$match":{match_key:object_identification}})

    # Add info to each image
    pipeline.append({"$addFields": {

                                # Add db/cols
                                "vision.views.images.database": client.database.name,
                                "vision.views.images.collection": client.name,

                                # Add object info
                                "vision.views.images.object": "$vision.views.skel_entities.id",
                                "vision.views.images.class": "$vision.views.skel_entities.bones.class",
                                "vision.views.images.percentage": "$vision.views.skel_entities.bones.img_perc",
                                "vision.views.images.x_min": "$vision.views.skel_entities.bones.img_bb.min.x",
                                "vision.views.images.y_min": "$vision.views.skel_entities.bones.img_bb.min.y",
                                "vision.views.images.x_max": "$vision.views.skel_entities.bones.img_bb.max.x",
                                "vision.views.images.y_max": "$vision.views.skel_entities.bones.img_bb.max.y",

                                # Add document id
                                "vision.views.images.document": "$_id"}})

    # Remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$vision.views"}})

    # Add image type filter
    pipeline.append({"$unwind": {"path": "$images"}})
    if image_type_list is not None:
        or_list = {"$match": {"$or": []}}
        for image_type in image_type_list:
            or_list["$match"]["$or"].append({"images.type": image_type})
        pipeline.append(or_list)

    # Again remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$images"}})

    # pprint.pprint(pipeline)


    result=list(client.aggregate(pipeline))

    return result

def search_all_bones_from_skel(client,object_identification,object_pattern='class',image_type_list=None,view_id_list=[]):
    """Search for bones from one skeletal object.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
        object_identification (String): Name of the object
        object_pattern (str, optional): Id or class. Defaults to 'class'.
        image_type_list (list, optional): List of targe image types. Defaults to None.
        view_id_list (list, optional): List of qualified camera views. Defaults to [].
    
    Returns:
        List: Result of qualified image data.
    """
    pipeline=[]

    # Remove docs without vision data
    pipeline.append({"$match":{"vision":{"$exists":1}}})

    # Unwind views for filtering multiple camera views
    pipeline.append({"$unwind":{"path":"$vision.views"}})

    # Right now only view name is supported, can be extended to view id easily
    if view_id_list !=[]:
        # Change class to id for view id filtering
        pipeline.append({"$match":{"vision.views.class":{"$in":view_id_list}}})

    if object_pattern=='class':
        match_key="vision.views.skel_entities.class"
    else:
        match_key="vision.views.skel_entities.id"
    # Find docs with the given class name or object id
    pipeline.append({"$unwind":{"path":"$vision.views.skel_entities"}})
    pipeline.append({"$match":{match_key:object_identification}})

    # Expand bones with unwind
    pipeline.append({"$unwind":{"path":"$vision.views.skel_entities.bones"}})


    # Add info to each image
    pipeline.append({"$addFields": {

                                # Add db/cols
                                "vision.views.images.database": client.database.name,
                                "vision.views.images.collection": client.name,

                                # Add object info
                                "vision.views.images.object": "$vision.views.skel_entities.id",
                                "vision.views.images.class": "$vision.views.skel_entities.bones.class",
                                "vision.views.images.percentage": "$vision.views.skel_entities.bones.img_perc",
                                "vision.views.images.x_min": "$vision.views.skel_entities.bones.img_bb.min.x",
                                "vision.views.images.y_min": "$vision.views.skel_entities.bones.img_bb.min.y",
                                "vision.views.images.x_max": "$vision.views.skel_entities.bones.img_bb.max.x",
                                "vision.views.images.y_max": "$vision.views.skel_entities.bones.img_bb.max.y",

                                # Add document id
                                "vision.views.images.document": "$_id"}})

    # Remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$vision.views"}})

    # Add image type filter
    pipeline.append({"$unwind": {"path": "$images"}})
    if image_type_list is not None:
        or_list = {"$match": {"$or": []}}
        for image_type in image_type_list:
            or_list["$match"]["$or"].append({"images.type": image_type})
        pipeline.append(or_list)

    # Again remove unnecessary info
    pipeline.append({"$replaceRoot":{"newRoot":"$images"}})

    # pprint.pprint(pipeline)


    result=list(client.aggregate(pipeline))

    return result

def search_one(client,object_identification,object_pattern='class',image_type_list=None,view_id_list=[],expand_bones=False):
    """Search for one object.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
        object_identification (String): Name of the object
        object_pattern (str, optional): Id or class. Defaults to 'class'.
        image_type_list (list, optional): List of targe image types. Defaults to None.
        view_id_list (list, optional): List of qualified camera views. Defaults to [].
        expand_bones (bool, optional): Flag of expanding searching for bones.
    
    Returns:
        List: Result of qualified image data.
    """

    # First Search in entities
    result=search_entities(client,object_identification,object_pattern,image_type_list,view_id_list)

    # Not an entity, search for skel then.
    if len(result)==0:
        print("No an entity.")
        result=search_skel(client,object_identification,object_pattern,image_type_list,view_id_list)
        # Not a skel, search for bones then
        if len(result)==0:
            print("No a bone.")
            result=search_bones(client,object_identification,object_pattern,image_type_list,view_id_list)
    # Is a skel, if expanding, expand all bones
    if expand_bones is True:
        print("Expand to search bones")
        bones_result=search_all_bones_from_skel(client,object_identification,object_pattern,image_type_list,view_id_list)
        print("Bone results:",len(bones_result),"Skel results:",len(result))
        result.extend(bones_result)

    return result


def find_conjunct_images_from_df(df,id_list,object_pattern):
    """Used to remove unqualified entries in an AND search."""
    remove_index_list=[]
    grouped_df=df.groupby(['file_id'])
    for each_file_id,grouped_set in grouped_df:
        if object_pattern=='class':
            num_unique=grouped_set['class'].value_counts().shape[0]
        else:
            num_unique=grouped_set['object'].value_counts().shape[0]
        
        if num_unique!=len(id_list):
            remove_index_list.extend(grouped_set.index.values)
    df=df.drop(index=remove_index_list)
    return df


def download_one(download_db, image, abs_path='', header=''):
    """Multiprocessing version of download.

       Args:
            download_db: A GridFS instance of the target collection. Example: download_db = gridfs.GridFSBucket(MongoClient(ip)[database], collection)
            image: A list contains file_id and type.
            abs_path: Path of root folder.
            header: Name of root folder.
    """
    image_type = image[1]
    image_file_id = image[0]
    root_folder = os.path.join(abs_path, header)
    type_folder = os.path.join(root_folder, image_type)
    if not os.path.exists(root_folder):
        try:
            os.makedirs(root_folder)
            print("make folder:", root_folder)
        except Exception as e:
            pass
    if image_type not in os.listdir(root_folder):
        os.makedirs(type_folder)

    saving_path = os.path.join(type_folder, str(image_file_id) + '.png')
    file = open(saving_path, "wb+")
    download_db.download_to_stream(file_id=ObjectId(image_file_id), destination=file)

def download_images(root_folder_path, root_folder_name, df,config_path):
    """Function to download all images with the DataFrame()."""
    print("Enter downloading!")
    df = df[['file_id', 'type', 'database', 'collection']]
    download_df=df.drop_duplicates()
    grouped_df = download_df.groupby(['database', 'collection'])
    for database_collection, group in grouped_df:
        print("Enter collection:", database_collection)
        print("Length of images", group.shape[0])
        group = group[['file_id', 'type']].values
        database = database_collection[0]
        collection = database_collection[1]

        ip,username,password=load_mongo_account(config_path)
        download_agent = gridfs.GridFSBucket(
            MongoClient(ip,username=username,password=password)[database], collection)

        for each_image in group:
            download_one(download_agent, each_image, root_folder_path, root_folder_name)


def get_db_info(db_coll_dict,config_path):
        """Get all databases and their collection info.
        
        Args:
            db_coll_dict (dict): Dict contains target dbs and collections
            config_path (str): Path to config file.
        
        Returns:
            dict: A info dict contains retrieved info.
        """
        ip,username,password=load_mongo_account(config_path)
        client=MongoClient(ip,username=username,password=password)
        detail_dict={}
        for db in db_coll_dict.keys():
            meta_client=client[db][db+".meta"]
            detail_dict[db]={}

            detail_dict[db]['collections']=[i for i in client[db].list_collection_names() if ".meta" not in i and ".files" not in i and ".chunks" not in i]

            # Add task description
            detail_dict[db]['task_description']=get_task_description(meta_client)

            # Add class and id info for entities
            detail_dict[db]['entities']={}
            class_info=get_all_class(meta_client)
            for each_object in class_info:
                obj_id=each_object['id']
                obj_class=each_object['class']
                if obj_class not in detail_dict[db]['entities'].keys():
                    detail_dict[db]['entities'][obj_class]=[obj_id]
                else:
                    detail_dict[db]['entities'][obj_class].append(obj_id)
            
            # Add skel entities
            detail_dict[db]['skels']={}
            bone_info=get_bone(meta_client)
            for each_bone in bone_info:
                bone=each_bone['bone']
                skel=each_bone['skel']
                if skel not in detail_dict[db]['skels'].keys():
                    detail_dict[db]['skels'][skel]=[bone]
                else:
                    detail_dict[db]['skels'][skel].append(bone)

            # Add camera view
            detail_dict[db]['camera_views']=get_camera_view(meta_client)
            # pprint.pprint(detail_dict)
        
        return detail_dict


def get_task_description(client):
    """Get the task description from a db.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
    
    Returns:
        str: The task description.
    """
    pipeline=[
        {"$match":{"task_description":{"$exists":1}}},
        {"$project":{"task_description":1,"_id":0}}
    ]
    result=list(client.aggregate(pipeline))
    if len(result)==1:
        return result[0]['task_description']
    else:
        return "No description"

def get_all_class(client):
    """Get class info from meta collection.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
    
    Returns:
        list: A list of class info.
    """
    pipeline=[
        {"$match":{"task_description":{"$exists":1}}},
        {"$unwind":{"path":"$entities"}},
        {"$replaceRoot":{"newRoot":"$entities"}},
        {"$project":{"id":1,"class":1,"mask_hex":1}}
    ]
    result=list(client.aggregate(pipeline))
    return result

def get_bone(client):
    """Get bone info from meta collection.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
    
    Returns:
        list: A list of bone info.
    """
    pipeline=[
    {
        '$match': {
            'task_description': {
                '$exists': 1
            }
        }
    }, {
        '$unwind': {
            'path': '$skel_entities'
        }
    }, {
        '$unwind': {
            'path': '$skel_entities.bones'
        }
    }, {
        '$addFields': {
            'skel_entities.bones.skel': '$skel_entities.class',
            'skel_entities.bones.bone': '$skel_entities.bones.name'
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$skel_entities.bones'
        }
    }, {
        '$project': {
            'bone': 1, 
            'skel': 1
        }
    }]

    result=list(client.aggregate(pipeline))
    return result


def get_camera_view(client):
    """Get camera view info from meta collection.
    
    Args:
        client (pymongo.MongoClient): Client to connect db.
    
    Returns:
        list: A list of camera view info.
    """
    pipeline=[
    {
        '$match': {
            'task_description': {
                '$exists': 1
            }
        }
    }, {
        '$unwind': {
            'path': '$camera_views'
        }
    }, {
        '$replaceRoot': {
            'newRoot': '$camera_views'
        }
    }, {
        '$project': {
            'class': 1
        }
    }]
    result=list(client.aggregate(pipeline))
    result=[i['class'] for i in result]
    return result





def event_search(db,collection,timestamp,camera_view,config_path=None):
    """Search with event sentences.

        Args:
            db: Target database.
            collection: Target collection.
            timestamp: Target timestamp.
            camera_view: Name of the camera view.
            config_path: File records account information.

        Return:
            A df contains qualified results.

    """
    def search_single_image_by_view(client, timestamp, view_id):
        """Search with the give camera name and timestamp.
            
            Args:
                client: A MongoClient instance.
                timestamp: Search for the first image closest to this timestamp.
                view_id: Class name of camera_views

            Returns:
                A list of qualified results.
        """

        pipeline = []
        pipeline.append({"$match":{"vision":{"$exists":1}}})
        pipeline.append({"$match": {"timestamp": {"$gte": timestamp}}})
        pipeline.append({"$unwind": {"path": "$vision.views"}})
        pipeline.append({"$match": {"vision.views.class": view_id}})
        pipeline.append({"$limit": 1})
        pipeline.append({"$replaceRoot": {"newRoot": "$vision.views"}})
        pipeline.append({"$unwind": {"path": "$images"}})
        pipeline.append({"$replaceRoot": {"newRoot": "$images"}})
        pipeline.append({"$addFields":{"database":client.database.name}})
        pipeline.append({"$addFields":{"collection":client.name}})
        result=list(client.aggregate(pipeline))
        return result

    ip,username,password=load_mongo_account(config_path)
    client = MongoClient(ip,username=username,password=password)[db][collection]
    image_info = search_single_image_by_view(client, timestamp=float(timestamp), view_id=camera_view)


    info_df = pd.DataFrame(image_info)
    if 'df' in locals():
        df = df.append(info_df, ignore_index=True)
    else:
        df = info_df
    df['file_id'] = df['file_id'].astype(str)
    return df


def scan_search(db,collection,scan_class_list,image_type_list,config_path):
    """Search for scan images.

        Args:
            db: Name of the database.
            collection: Name of the collection.
            scan_class_list: A list contains class names.
            image_type_list: A list conatians image types.
            config_path: Path to config file for user name and pwd.

        Return:
            A result DataFrame.

    """

    def get_scans_by_class(meta_client,class_name=None,image_type_list=None):
        """Get scan images by class name.

        Args:
            meta_client: A MongoClient instance.
            class_name: Name of the class in the meta collection.

        Returns:
            A list of images.

        """

        pipeline=[]
        if class_name is not None:
            pipeline.append({"$match":{"class":class_name}})
        pipeline.append({"$unwind":{"path":"$scans"}})
        pipeline.append({"$unwind":{"path":"$scans.images"}})

        if image_type_list is not None:
            or_list = {"$match": {"$or": []}}
            for image_type in image_type_list:
                or_list["$match"]["$or"].append({"scans.images.type": image_type})
            pipeline.append(or_list)
        pipeline.append({"$addFields":{
                            "scans.images.class":"$class",
                            "scans.images.xmin":"$scans.img_bb.min.x",
                            "scans.images.ymin":"$scans.img_bb.min.y",
                            "scans.images.xmax":"$scans.img_bb.max.x",
                            "scans.images.ymax":"$scans.img_bb.max.y",
                        }})
        pipeline.append({"$replaceRoot":{"newRoot":"$scans.images"}})
        
        return list(meta_client.aggregate(pipeline))

    result=[]
    ip,username,password=load_mongo_account(config_path)
    meta_client=MongoClient(ip,username=username,password=password)[db][collection]
    if scan_class_list==[]:
        result=get_scans_by_class(meta_client,image_type_list=image_type_list)
    else:
        for each_class in scan_class_list:
            result.extend(get_scans_by_class(meta_client,each_class,image_type_list))
    df=pd.DataFrame(result)
    df['database']=db
    df['collection']=collection
    return df


    










