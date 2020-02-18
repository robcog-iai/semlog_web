import sys
sys.path.append("..")
from pymongo import MongoClient
from bson.objectid import ObjectId
from mongo import MongoDB
import pprint
import pandas as pd


def reset_duplicate(client):
    client.update_many({"camera_views":{"$exists":1}},{"$set":{"camera_views.0.duplicate":False}})


def download_info(save=False):
    db=MongoDB(database="MessyKitchen",collection="a2")
    r=db.get_image_information()

    df={"_id":[],"num_entities":[],"average_linear_distance":[],"average_angular_distance":[],"timestamp":[],"duplicate":[]}
    for p in r:
        df["_id"].append(str(p["_id"]))
        df['num_entities'].append(p['num_entities'])
        df['average_linear_distance'].append(p['average_linear_distance'])
        df['average_angular_distance'].append(p['average_angular_distance'])
        df['timestamp'].append(p['timestamp'])
        df['duplicate'].append(p["duplicate"])

    df=pd.DataFrame.from_dict(df)
    df['total_linear_distance']=df['average_linear_distance']*df['num_entities']
    df['total_angular_distance']=df['average_angular_distance']*df['num_entities']
    if save is True:
        df.to_csv('info.csv',index=False)
    print(df['duplicate'].value_counts())
    return df
#---------------------Create support csv for loop----------------------#


def clone_collection():
    client=MongoClient("localhost")['MessyKitchen']['a2']
    client.aggregate([{"$match":{}},{"$out":"a2_origin"}])


def check_duplicate(client,linear_distance_tolerance,angular_distance_tolerance):
    # Set threshold params
    linear_distance_tolerance=50
    angular_distance_tolerance=1

    pipeline=[{"$match":{"camera_views":{"$exists":1}}},
            {"$unwind":{"path":"$camera_views"}},
            {"$match":{"camera_views.duplicate":False}},]
    document_list=list(client.aggregate(pipeline))
    duplicate_dict={i["_id"]:False for i in document_list}

    #------------------------loop all documents------------------------#
    for document in document_list:

        # Skip if marked duplicated before
        # if duplicate_dict[document["_id"]] is True:
        #     continue

        pipeline1=[{"$match":{"_id":document["_id"]}},
                    {"$unwind":{"path":"$camera_views"}},
                    {"$replaceRoot":{"newRoot":"$camera_views"}},
                    {"$project":{"duplicate":1}}]
        flag_duplicate=list(client.aggregate(pipeline1))[0]['duplicate']
        if flag_duplicate is True:
            continue
        upper_linear=document['camera_views']['total_linear_distance']+linear_distance_tolerance
        lower_linear=document['camera_views']['total_linear_distance']-linear_distance_tolerance
        upper_angular=document['camera_views']['total_angular_distance']+angular_distance_tolerance
        lower_angular=document['camera_views']['total_angular_distance']-angular_distance_tolerance

        match_pattern={ "_id":{"$ne":document["_id"]},
                        "camera_views":{"$exists":1},
                        "camera_views.num_entities":document['camera_views']['num_entities'],
                        "$and":[
                            {"camera_views.total_linear_distance":{"$gte":lower_linear}},
                            {"camera_views.total_linear_distance":{"$lte":upper_linear}},
                            {"camera_views.total_angular_distance":{"$gte":lower_angular}},
                            {"camera_views.total_angular_distance":{"$lte":upper_angular}},
                            {"camera_views.duplicate":False}]
                        }

        
        new_value={"$set":{"camera_views.0.duplicate":True}}
        # duplicate_list=[i["_id"] for i in list(client.find(match_pattern)) if i["_id"]!=document["_id"]]
        # for d in duplicate_list:
        #     duplicate_dict[d]=True
        result=client.update_many(match_pattern,new_value)       


if __name__=="__main__":

    client=MongoClient("localhost")['MessyKitchen']['a2']
    reset_duplicate(client)
    # check_duplicate(client)
    # download_info(True)
