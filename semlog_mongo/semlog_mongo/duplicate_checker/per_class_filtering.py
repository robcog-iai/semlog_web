import sys
sys.path.append("..")
from pymongo import MongoClient
from mongo import MongoDB
from pprint import pprint
import pandas as pd


def reset_duplicate(client):
    for i in range(50):
        try:
           client.update_many({"camera_views":{"$exists":1}},{"$set":{"camera_views.0.entities."+str(i)+".duplicate":False}})
        except Exception as e:
            continue

def download_class_info(client,class_id):
    pipeline=[]
    df={"id":[],"timestamp":[],"num_pixels":[],"linear_distance":[],"angular_distance":[],"duplicate":[]}
    pipeline.append({"$match":{"$and":[{'camera_views':{"$exists":1}},
                                        {"camera_views.duplicate":False}]}})
    pipeline.append({"$unwind":{"path":"$camera_views"}})
    pipeline.append({"$unwind":{"path":"$camera_views.entities"}})
    pipeline.append({"$match":{"$and":[{"camera_views.entities.class":class_id},
                                       {}]}})
    result=list(client.aggregate(pipeline))
    for i in result:
        df['id'].append(i['camera_views']['entities']['id'])
        df['timestamp'].append(i['timestamp'])
        df['num_pixels'].append(i['camera_views']['entities']['num_pixels'])
        df['linear_distance'].append(i['camera_views']['entities']['linear_distance'])
        df['angular_distance'].append(i['camera_views']['entities']['angular_distance'])
        df['duplicate'].append(i['camera_views']['entities']['duplicate'])
    df=pd.DataFrame.from_dict(df)
    print(df['duplicate'].value_counts())
    df.to_csv("class_info.csv",index=False)


    # pprint(result)

def check_and_update_duplicate_per_class(client,class_list,num_pixels_tolerance=100,linear_distance_tolerance=0.002,angular_distance_tolerance=0.0005):
    for class_id in class_list:
        print("Check duplicate for class:",class_id)
        pipeline=[]
        pipeline.append({"$match":{"$and":[{'camera_views':{"$exists":1}},
                                            {"camera_views.duplicate":False}]}})
        pipeline.append({"$unwind":{"path":"$camera_views"}})
        pipeline.append({"$unwind":{"path":"$camera_views.entities"}})
        pipeline.append({"$match":{"$and":[{"camera_views.entities.class":class_id},
                                           {"camera_views.entities.duplicate":False}]}})
        result=list(client.aggregate(pipeline))
        # length_result=len(result)
        # pprint(result)
        # print(pipeline)
        skip_list=[]
        for each_entity in result:
            if [each_entity["_id"],each_entity["camera_views"]["entities"]["id"]] in skip_list:
                print("Already marked duplicate. Skip")
                continue
            num_pixels=each_entity['camera_views']['entities']['num_pixels']
            linear_distance=each_entity['camera_views']['entities']['linear_distance']
            angular_distance=each_entity['camera_views']['entities']['angular_distance']
            pipeline2=pipeline.copy()
            pipeline2.append({"$match":{"$and":[
                {"camera_views.entities.num_pixels":{"$lte":num_pixels+num_pixels_tolerance}},
                {"camera_views.entities.num_pixels":{"$gte":num_pixels-num_pixels_tolerance}},
                {"camera_views.entities.linear_distance":{"$lte":linear_distance+linear_distance_tolerance}},
                {"camera_views.entities.linear_distance":{"$gte":linear_distance-linear_distance_tolerance}},
                {"camera_views.entities.angular_distance":{"$lte":angular_distance+angular_distance_tolerance}},
                {"camera_views.entities.angular_distance":{"$gte":angular_distance-angular_distance_tolerance}}
                ]}})
            duplicate_result=list(client.aggregate(pipeline2))
            # pprint(duplicate_result)
            duplicate_result=[[i["_id"],i["camera_views"]["entities"]["id"]] for i in duplicate_result if i['_id']!=each_entity['_id']]
            skip_list.extend(duplicate_result)
            print(len(duplicate_result))
            for each_duplicate in duplicate_result:
                match_pattern={"_id":each_duplicate[0],
                                "camera_views.0.entities.id":each_duplicate[1]}
                set_value={"$set":{"camera_views.0.entities.$.duplicate":True}}
                client.update_many(match_pattern,set_value)

        print("Checked")
            # pprint(duplicate_result)
            # print([i['timestamp'] for i in duplicate_result])








if __name__=="__main__":
    num_pixels_tolerance=150
    linear_distance_tolerance=0.05
    angular_distance_tolerance=0.005
    client=MongoClient("localhost")['MessyKitchen']['a2']

    # check_and_update_duplicate_per_class(client,['MPOnionYellow'],
    #     num_pixels_tolerance=num_pixels_tolerance
    #     ,linear_distance_tolerance=linear_distance_tolerance,
    #     angular_distance_tolerance=angular_distance_tolerance)
    # download_class_info(client,'MPGarlic')
    reset_duplicate(client)
