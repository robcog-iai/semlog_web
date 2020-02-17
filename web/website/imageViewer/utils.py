import uuid
import cv2
import itertools
import os
import pprint

from multiprocessing.dummy import Pool

try:
    from web.semlog_mongo.semlog_mongo.mongo import *
    from web.semlog_mongo.semlog_mongo.utils import *
    from web.semlog_vis.semlog_vis.image import *
    from web.website.settings import CONFIG_PATH, IMAGE_ROOT
    from web.image_path.logger import Logger
except Exception as e:
    os.system("git submodule init")
    os.system("git submodule update")
    from web.semlog_mongo.semlog_mongo.mongo import *
    from web.semlog_mongo.semlog_mongo.utils import *
    from web.semlog_vis.semlog_vis.image import *
    from web.website.settings import CONFIG_PATH, IMAGE_ROOT
    from web.image_path.logger import Logger


def compile_query(data):
    data=data.split(" ")
    optional_data=data[1:]
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

        if len(optional_data)!=0:
            if "label" in optional_data:
                query['label']=True
            if "crop" in optional_data:
                query['crop']=True
            if "detection" in optional_data:
                query['detection']=True
            if "classifier" in optional_data:
                query['classifier']=True

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
        query['class']=['Event']
        query['type'] = data[5].split("+")
    return query

def compile_customization(data):
        data=data.split(" ")
        optional_data=data[1:]
        data = data[0].split("@")
        return_dict = {}
        return_dict['resize_type']=data[0]
        return_dict['width']=int(data[1])
        return_dict['height']=int(data[2])
        return_dict['padding_type']='reflect'

        if len(optional_data)!=0:
            optional_data=optional_data[0]
            if "=" in optional_data:
                return_dict['pad_type']='constant'
                color_list=optional_data.split("=")[1].split(",")
                return_dict['constant_color']=color_list
            else:
                return_dict['pad_type']=optional_data
        return return_dict
        


def customize_image_resolution(customization_dict,image_dir):
    """Wrapper for three different resize functions for all images."""
    print("Entering resolution changing!!!!!!!!!!!!!!")
    print(customization_dict)
    resize_type=customization_dict['resize_type']
    padding_type=customization_dict['padding_type']
    width=customization_dict['width']
    height=customization_dict['height']

    if resize_type == 'pad':
        padding_type = convert_padding_type(padding_type)
        pad_all_images(image_dir, width, height,
                        padding_type, customization_dict['constant_color'])
    else:
        resize_all_images(image_dir, width,
                            height, resize_type)


def convert_padding_type(padding_type):
    """Convert text input to cv2 padding type."""
    padding_type = padding_type.casefold()
    if 'constant' in padding_type:
        padding_type = cv2.BORDER_CONSTANT
    elif 'reflect' in padding_type:
        padding_type = cv2.BORDER_REFLECT
    elif 'reflect_101' in padding_type:
        padding_type = cv2.BORDER_REFLECT_101
    elif 'replicate' in padding_type:
        padding_type = cv2.BORDER_REPLICATE
    else:
        padding_type = cv2.BORDER_REFLECT
    return padding_type

def recalculate_bb(df,customization_dict,image_dir):
    """After resizing images, bb coordinates are recalculated.
    
    Args:
        df (Dataframe): A df for image info.
        customization_dict (dict): Resize dict.
        image_dir (list): Image path list
    
    Returns:
        Dataframe: Updated dataframe.
    """
    img = cv2.imread(image_dir[0])
    h,w,_=img.shape
    new_width=customization_dict['width']
    new_height=customization_dict['height']
    w_ratio=new_width/w
    h_ratio=new_height/h
    df['x_min']=df['x_min']*w_ratio
    df['x_max']=df['x_max']*w_ratio
    df['y_min']=df['y_min']*h_ratio
    df['y_max']=df['y_max']*h_ratio
    df.x_min=df.x_min.astype("int16")
    df.x_max=df.x_max.astype("int16")
    df.y_min=df.y_min.astype("int16")
    df.y_max=df.y_max.astype("int16")
    return df
