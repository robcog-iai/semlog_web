import uuid
import cv2
import itertools
import os

from multiprocessing.dummy import Pool

try:
    from web.semlog_mongo.semlog_mongo.mongo import *
    from web.semlog_mongo.semlog_mongo.utils import *
    from web.semlog_vis.semlog_vis.image import cut_object, resize_image
except Exception as e:
    os.system("git submodule init")
    os.system("git submodule update")
    from web.semlog_mongo.semlog_mongo.mongo import *
    from web.semlog_mongo.semlog_mongo.utils import *
    from web.semlog_vis.semlog_vis.image import cut_object, resize_image


class WebsiteData():
    """Data is used to parse input from html.

        Attributes:
            form_dict: Input dict from html.
            ip: ip_address to MongoDB

    """

    def __init__(self, form_dict, ip):
        """Clean all inputs from form_dict."""

        print(form_dict)
        user_id = str(uuid.uuid4())
        object_id_list = []
        image_type_list = []
        view_list = []
        bounding_box_dict = {}
        search_pattern = 'entity_search'
        dataset_pattern = None
        flag_ignore_duplicate_image = False
        flag_apply_filtering = False
        flag_class_ignore_duplicate_image = False
        flag_class_apply_filtering = False
        flag_split_bounding_box = False

        checkbox_object_pattern = form_dict['checkbox_object_pattern']
        flag_resize_type = form_dict['checkbox_resize_type']
        width = int(form_dict['width']) if form_dict['width'] != "" else ""
        height = int(form_dict['height']
                     ) if form_dict['height'] != "" else ""
        linear_distance_tolerance = float(form_dict['linear_distance_tolerance'])
        angular_distance_tolerance = float(form_dict['angular_distance_tolerance'])
        class_linear_distance_tolerance = float(form_dict['class_linear_distance_tolerance'])
        class_angular_distance_tolerance = float(form_dict['class_angular_distance_tolerance'])
        class_num_pixels_tolerance = int(form_dict['class_num_pixels_tolerance'])

        object_logic = form_dict['checkbox_object_logic']
        for (key, value) in form_dict.items():
            if key.startswith("checkbox_ignore_duplicate_image"):
                flag_ignore_duplicate_image = True
            if key.startswith("checkbox_apply_filtering"):
                flag_apply_filtering = True
            if key.startswith("checkbox_class_ignore_duplicate_image"):
                flag_class_ignore_duplicate_image = True
            if key.startswith("checkbox_class_ignore_duplicate_image"):
                flag_class_apply_filtering = True
            if key.startswith("checkbox_split_bounding_box"):
                flag_split_bounding_box = True
            if key.startswith('checkbox_search_pattern'):
                search_pattern = value
            if key.startswith('checkbox_dataset_pattern'):
                dataset_pattern = value
            if key.startswith("view_object_id") and value != '':
                v = value.split('-')
                if len(v) != 4:
                    raise ValueError("The input search gramma is wrong.")
                else:
                    view_list.append(v)
            if key.startswith('database_collection_list'):
                m = MongoClient(ip, 27017)
                if value == '':
                    # Append all available collections 
                    database_collection_list = []
                    neglect_list = ['admin', 'config', 'local', 'semlog_web']
                    db_list = m.list_database_names()
                    db_list = [i for i in db_list if i not in neglect_list]
                    for db in db_list:
                        for c in m[db].list_collection_names():
                            if '.' not in c:
                                database_collection_list.append([db, c])
                else:
                    # Convert all to available collections
                    database_collection_list = value.split("@")
                    database_collection_list = [
                        i for i in database_collection_list if i != ""]
                    new_list = []
                    for database_collection in database_collection_list:
                        dc = database_collection.split("$")
                        if dc[1] == "ALL":
                            extend_list = [dc[0] + "$" + i for i in m[dc[0]].list_collection_names() if '.' not in i]
                            new_list.extend(extend_list)
                        else:
                            new_list.append(database_collection)
                    database_collection_list = sorted(list(set(new_list)))
                    database_collection_list = [i.split("$") for i in database_collection_list]
                    print("new db-col lists:", database_collection_list)

            # Get multiply objects/classes from input fields
            if key.startswith('object_id') and value != '':
                object_id_list.append(value)
            # Get selected image type checkbox
            if key.startswith('rgb'):
                image_type_list.append('Color')
            if key.startswith('depth'):
                image_type_list.append('Depth')
            if key.startswith('normal'):
                image_type_list.append('Normal')
            if key.startswith('mask'):
                image_type_list.append('Mask')

            # Get the logic of object/class
            if key.startswith('checkbox_object_logic'):
                if value != 'and':
                    object_logic = 'or'

        m = MongoDB(database_collection_list, ip)
        if checkbox_object_pattern == 'class':
            self.class_id_list=object_id_list.copy()
            self.object_rgb_dict_dict = m.get_object_rgb_dict(object_id_list, checkbox_object_pattern)
            self.object_id_list = list(self.object_rgb_dict.keys())
        else:
            self.object_rgb_dict = m.get_object_rgb_dict(object_id_list, checkbox_object_pattern)
            self.object_id_list = object_id_list
            self.class_id_list = None

        self.image_type_list = image_type_list
        self.bounding_box_dict = bounding_box_dict
        self.object_logic = object_logic
        self.flag_ignore_duplicate_image = flag_ignore_duplicate_image
        self.flag_apply_filtering = flag_apply_filtering
        self.flag_class_ignore_duplicate_image = flag_class_ignore_duplicate_image
        self.flag_class_apply_filtering = flag_class_apply_filtering
        self.flag_split_bounding_box = flag_split_bounding_box
        self.similar_dict = {"linear_distance_tolerance": linear_distance_tolerance,
                             "angular_distance_tolerance": angular_distance_tolerance,
                             "class_linear_distance_tolerance": class_linear_distance_tolerance,
                             "class_angular_distance_tolerance": class_angular_distance_tolerance,
                             "class_num_pixels_tolerance": class_num_pixels_tolerance}
        self.search_pattern = search_pattern
        self.dataset_pattern = dataset_pattern
        self.checkbox_object_pattern = checkbox_object_pattern
        self.flag_resize_type = flag_resize_type
        self.width = width
        self.height = height
        self.database_collection_list = database_collection_list
        self.user_id = user_id
        self.ip = ip
        self.view_list = view_list
