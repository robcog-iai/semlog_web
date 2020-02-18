import pprint

def get_image_information(client):
    """This function is used for data analysis. 
    Retrieve image information (num_entities, average linear/angular distance and timestamp).
    
    Args:
        client: A MongoClient to search for.

    Returns:
        A list of dicts contains average information.

     """

    pipeline = [{"$match": {"camera_views": {"$exists": 1}}}, {"$unwind": {"path": "$camera_views"}}, {"$addFields": {
        "camera_views.average_linear_distance": {
            "$divide": [
                "$camera_views.total_linear_distance",
                "$camera_views.num_entities"
            ]
        },
        "camera_views.average_angular_distance": {
            "$divide": [
                "$camera_views.total_angular_distance",
                "$camera_views.num_entities"
            ]
        },
        "camera_views.timestamp": "$timestamp",
        "camera_views._id": "$_id",
        "camera_views.database": client.database.name,
        "camera_views.collection": client.name,
        'camera_views.file_id':"$camera_views.images.file_id", #Add the Color image id for downloading and testing
    }}, {"$replaceRoot": {"newRoot": "$camera_views"}}, {"$project": {
        "_id": 1,
        "num_entities": 1,
        "average_linear_distance": 1,
        "average_angular_distance": 1,
        "timestamp": 1,
        "duplicate": 1,
        "database":1,
        "collection":1,
        "file_id":{"$arrayElemAt":["$images.file_id",0]}, # Only keep the first file id (The Color image)
    }}]
    pprint.pprint(pipeline)
    result = list(client.aggregate(pipeline))
    return result