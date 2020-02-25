from pathlib import Path
import pandas as pd
import sys
sys.path.append("../..")

from semlog_vis.semlog_vis.image import *
from image_path.image_path import *

def download_bounding_box(df, root_folder_path, root_folder_name):
    """Main function for download bounding boxes.

    Args:
        df: Information Data Frame.
        root_folder_path: Root path for images.
        root_folder_name: Root folder name.

    """
    def download_bounding_box_by_object(object_id):
        """Support function for downloading bounding box.

        Args:
            df: Information Data Frame.
            object_id: Target object.
            rgb: Mask color of targe object.
            image_root: Root path for images.
            folder_header: Root folder name.

        """
        image_dir, image_type_list, class_name = get_image_path_for_bounding_box(
            df, object_id, root_folder_path, root_folder_name)

        if "/" in object_id:
            print("weird object id,neglect!")
            return 0

        for t in image_type_list:
            if t != "Color":
                continue
            folder_name = object_id + "$" + t + "$" + 'boundingBox'
            saving_folder = os.path.join(
                root_folder_path, root_folder_name, 'BoundingBoxes', folder_name)
            if not os.path.isdir(saving_folder):
                os.makedirs(saving_folder)
                print("make folder for:", saving_folder)
            object_df = df[(df.object == object_id) & (df.type == t)]
            for _, each_row in object_df.iterrows():
                crop_object_from_image(
                    saving_folder, root_folder_path, root_folder_name, each_row)

            # Create and save cut images
            # for rgb_img, mask_img in zip(rgb_img_list, mask_img_list):
            #     img_saving_path = os.path.join(
            #         saving_folder, os.path.basename(rgb_img[:-4]) + "_" + class_name + '.png')
            #     cut_object(rgb_img, mask_img, rgb, saving_path=img_saving_path)

    object_id_list = list(df.object.unique())
    for object_id in object_id_list:
        download_bounding_box_by_object(object_id)


def calculate_bounding_box(df, object_rgb_dict, root_folder_path, root_folder_name):
    """Main function for calculate bounding boxes.

    Args:
        df: Information Data Frame.
        object_rgb_dict: A dict maps object id to mask colors.
        root_folder_path: Root path for images.
        root_folder_name: Root folder name.

    """

    def calculate_bounding_box_by_object(object_id, rgb):
        """Support function for calculating bounding box.

        Args:
            df: Information Data Frame.
            object_id: Target object.
            rgb: Mask color of targe object.
            image_root: Root path for images.
            folder_header: Root folder name.
            bounding_box_columns: columns for storing coordinates of bounding boxes.

        """
        image_dir, image_type_list, class_name = get_image_path_for_bounding_box(
            df, object_id, root_folder_path, root_folder_name)
        print("Object id: %s, rgb_color: %s" % (object_id, rgb))

        # Create dict and folder
        for t in image_type_list:
            if t != "Color":
                continue
            folder_name = object_id + "$" + t + "$" + 'boundingBox'
            rgb_img_list = sorted(image_dir[t])
            mask_img_list = sorted(image_dir['Mask'])
            print("****length of rgb_img_list****:", len(rgb_img_list))

            # Read resolution of origin image and add to info collection
            if len(rgb_img_list) == 0:
                origin_width = origin_height = 0
            else:
                sample_img = cv2.imread(rgb_img_list[0])
                origin_width, origin_height = sample_img.shape[
                    1], sample_img.shape[0]
            count = 0

            # Create and save cut images
            for rgb_img, mask_img in zip(rgb_img_list, mask_img_list):

                # Cut object and update location to the collection
                hmin, hmax, wmin, wmax = cut_object(rgb_img, mask_img, rgb)
                if wmin == -1:
                    print("ignore this bounding box!")
                    continue
                _wmin = 1 if wmin == 0 else wmin
                _wmax = 1 if wmax == 0 else wmax
                _hmin = 1 if hmin == 0 else hmin
                _hmax = 1 if hmax == 0 else hmax

                wmin = int(_hmin)
                wmax = int(_hmax)
                hmin = int(_wmin)
                hmax = int(_wmax)
                x_center = ((wmax + wmin) / 2) / origin_width
                y_center = ((hmax + hmin) / 2) / origin_height
                width = (wmax - wmin) / origin_width
                height = (hmax - hmin) / origin_height
                update_values = [wmin, wmax, hmin, hmax,
                                 x_center, y_center, width, height]
                file_id = os.path.basename(rgb_img)[:-4]

                if df.loc[(df.object == object_id) & (df.file_id == file_id), bounding_box_columns]['wmin'].values == "":
                    df.loc[(df.object == object_id) & (df.file_id ==
                                                       file_id), bounding_box_columns] = update_values

                count = count + 1
        return df

    bounding_box_columns = ['wmin', 'wmax', 'hmin',
                            'hmax', 'x_center', 'y_center', 'width', 'height']
    for col in bounding_box_columns:
        df[col] = ""
    for object_id, rgb in object_rgb_dict.items():
        rgb = object_rgb_dict[object_id]
        df = calculate_bounding_box_by_object(object_id, rgb)
    return df


def crop_with_all_bounding_box(df, image_dir):
    """Crop full images with max boundary of all objects.

        Args:
            df: A dataframe contains objects and their mask colors.
            image_dir: A dict contains all target images
    """

    type_dir = image_dir['Color']
    parent_path = Path(type_dir[0]).parent.parent   

    print(parent_path)

    grouped_df = df.groupby(['file_id'])
    for name, group in grouped_df:
        xmin = group['x_min'].min()
        ymin = group['y_min'].min()
        xmax = group['x_max'].max()
        ymax = group['y_max'].max()
        img_type=group['type'].values[0]
        img_dir = os.path.join(parent_path,img_type, name+".png")
        img = cv2.imread(img_dir)
        img = img[ymin:ymax, xmin:xmax]
        cv2.imwrite(img_dir, img)


def recalculate_bb(df, customization_dict, image_dir):
    """After resizing images, bb coordinates are recalculated.

    Args:
        df (Dataframe): A df for image info.
        customization_dict (dict): Resize dict.
        image_dir (list): Image path list

    Returns:
        Dataframe: Updated dataframe.
    """
    img = cv2.imread(image_dir[0])
    h, w, _ = img.shape
    new_width = customization_dict['width']
    new_height = customization_dict['height']
    w_ratio = new_width/w
    h_ratio = new_height/h
    df['x_min'] = df['x_min']*w_ratio
    df['x_max'] = df['x_max']*w_ratio
    df['y_min'] = df['y_min']*h_ratio
    df['y_max'] = df['y_max']*h_ratio
    df.x_min = df.x_min.astype("int16")
    df.x_max = df.x_max.astype("int16")
    df.y_min = df.y_min.astype("int16")
    df.y_max = df.y_max.astype("int16")
    return df
