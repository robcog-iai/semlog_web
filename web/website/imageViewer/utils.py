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
        return_dict['width']=data[1]
        return_dict['height']=data[2]
        return_dict['pad_type']='reflect'

        if len(optional_data)!=0:
            optional_data=optional_data[0]
            if "=" in optional_data:
                return_dict['pad_type']='constant'
                color_list=optional_data.split("=")[1].split(",")
                return_dict['constant_color']=color_list
            else:
                return_dict['pad_type']=optional_data
        return return_dict
        


def customize_image_resolution(self, image_dir):
    """Wrapper for three different resize functions for all images."""
    print(image_dir)
    if self.flag_resize_type == 'pad':
        self.padding_type = convert_padding_type(self.padding_type)
        pad_all_images(image_dir, self.width, self.height,
                        self.padding_type, self.padding_constant_color)
    else:
        resize_all_images(image_dir, self.width,
                            self.height, self.flag_resize_type)


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
