import cv2
import numpy as np
import random
import itertools
import os
from multiprocessing.dummy import Pool


def compile_customization(data):
        """Compile input string for resolution customization.
        
        Args:
            data (str): A sentence with customization data.
        
        Returns:
            dict: A dict contains customization info.
        """
        data=data.split(" ")
        optional_data=data[1:]
        data = data[0].split("@")
        return_dict = {}
        return_dict['resize_type']=data[0]
        return_dict['width']=int(data[1])
        return_dict['height']=int(data[2])
        return_dict['padding_type']='reflect'
        return_dict['constant_color']=[255,0,0]

        if len(optional_data)!=0:
            optional_data=optional_data[0]
            if "," in optional_data:
                return_dict['padding_type']='constant'
                color_list=optional_data.split(",")
                return_dict['constant_color']=tuple([int(i) for i in color_list])
            else:
                return_dict['padding_type']=optional_data
        return return_dict


def customize_image_resolution(customization_dict,image_dir):
    """Wrapper for three different resize functions for all images."""
    resize_type=customization_dict['resize_type']
    padding_type=customization_dict['padding_type']
    width=customization_dict['width']
    height=customization_dict['height']

    print(customization_dict)
    if resize_type == 'pad':
        print("!!!!!!!!!!!!!!!!")
        print(padding_type)
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
    print("pad type:",padding_type)
    return padding_type




def load_img_and_mask(img, mask):
    """Load a pair of image and its mask, convert BGR to RGB of the mask image."""
    img = cv2.imread(img)
    mask = cv2.imread(mask)
    mask = cv2.cvtColor(mask, cv2.COLOR_BGR2RGB)
    return img, mask


def replace_single_color(img, color, new_color):
    """Replace one color in an image to another color.

        Args:
            img: The source image.
            color: The color in the image that you want to replace.
            new_color: New color to replace.

        Returns:
            New image as ndarray.

    """
    backgound_binary = np.where((img == color).all(axis=2))
    img[backgound_binary] = new_color
    return img


def resize_image(img_path, width="", height="", type='cut'):
    """Resize one image.

    Args:
        img_path: Path of the image
        width: Target width.
        height: Target height.
        type: 'cut' -> Stretch the image absolutely, otherwise scale the image by width or height.

    """
    img = cv2.imread(img_path)
    (h, w) = img.shape[:2]
    if type == 'cut':
        if width == "":
            width = w
        if height == "":
            height = h
        dim = (width, height)
        img = cv2.resize(img, dim, interpolation=cv2.INTER_AREA)
    else:
        if width == "":
            r = height / float(h)
            dim = (int(w * r), height)
        else:
            r = width / float(w)
            dim = ((width, int(h * r)))
        img = cv2.resize(img, dim, interpolation=cv2.INTER_AREA)
    cv2.imwrite(img_path, img)


def scale_image(img_path, ratio):
    """Scale an image with a given ratio.

        Args:
            img_path: Path to the image.
            ratio: ratio to scale the image.

    """
    img = cv2.imread(img_path)
    (h, w) = img.shape[:2]
    h = int(h * ratio)
    w = int(w * ratio)
    img = cv2.resize(img, (w, h))
    cv2.imwrite(img_path, img)


def pad_image(img_path, width, height, pad_type, value=(0, 0, 0)):
    """Pad the give image with different size and padding type.

        Args:
            img_path: Path of the source image.
            width: The target width.
            height: The target height.
            pad_type: The padding type in cv2.copyMakeBorder.
            value: If pad_type="cv2.Border_CONSTANT", this value is used as the padding constant color.

        Returns:
            The processed image.
    """



    def get_left_right(margin_width):
        if margin_width % 2 == 0:
            left = margin_width // 2
            right = margin_width // 2
        else:
            left = margin_width // 2
            right = margin_width // 2 + 1
        return left, right

    def get_top_bottom(margin_height):
        if margin_height % 2 == 0:
            top = margin_height // 2
            bottom = margin_height // 2
        else:
            top = margin_height // 2
            bottom = margin_height // 2 + 1
        return top, bottom
    
    img = cv2.imread(img_path)
    h, w, _ = img.shape
    img_ratio=h/w
    target_ratio=height/width
    margin_width = width - w
    margin_height = height - h
    # if h >= height and w >= width:
    margin_width = abs(margin_width)
    margin_height = abs(margin_height)
    if img_ratio<target_ratio:
        resize_image(img_path, width=width, type='scale')
        img = cv2.imread(img_path)
        _h, _w, _ = img.shape
        # print("new h:",_h)
        # print("new w:",_w)
        # print("t w:",width)
        # print("t h:",height)
        margin_height = height - _h
        top, bottom = get_top_bottom(margin_height)
        # print(top,bottom)
        img = cv2.copyMakeBorder(img, top, bottom, 0, 0, pad_type, value=value)

    else:
        resize_image(img_path, height=height, type='scale')
        img = cv2.imread(img_path)
        _h, _w, _ = img.shape
        margin_width = width - _w
        left, right = get_left_right(margin_width)
        img = cv2.copyMakeBorder(img, 0, 0, left, right, pad_type, value=value)

    # elif h <= height and w <= width:
    #     img = cv2.resize(img, (width, height))
    # elif h >= height:
    #     img = cv2.resize(img, (w, height))
    #     h, w, _ = img.shape
    #     left, right = get_left_right(margin_width)
    #     img = cv2.copyMakeBorder(img, 0, 0, left, right, pad_type, value=value)
    # elif w >= width:
    #     img = cv2.resize(img, (width, h))
    #     h, w, _ = img.shape
    #     top, bottom = get_top_bottom(margin_height)
    #     img = cv2.copyMakeBorder(img, top, bottom, 0, 0, pad_type, value=value)
    cv2.imwrite(img_path, img)


def pad_all_images(image_dir, width, height, pad_type, value=(0, 0, 0)):
    """Multi processing the padding function.
        Args:
            image_dir: A list contains path to all images.
            width: Target width.
            height: Target height.
            pad_type: Different type of padding in opencv-python
            value: If constant padding is chosen, use this rgb color to fill the padding areas.


    """
    pool = Pool(1)
    pool.starmap(pad_image, zip(
        image_dir, itertools.repeat(width), itertools.repeat(height), itertools.repeat(pad_type),
        itertools.repeat(value)))
    pool.close()
    pool.join()


def scale_all_images(image_dir, ratio):
    """Multi processing the scaling function.
        Args:
            image_dir: A list contains path to all images.
            ratio: Target ratio to scale.

    """
    pool = Pool(1)
    pool.starmap(scale_image, zip(
        image_dir, itertools.repeat(ratio)))
    pool.close()
    pool.join()


def resize_all_images(image_dir, width, height, resize_type):
    """Multiprocessing function for resize images.

    Args:
        image_dir: A dict of images to be resized.
        width: Target width.
        height: Target height.
        resize_type: Stretch or sclae depending on the input.


    """
    if width == "" and height == "":
        return 0
    print("Enter resizing image.")
    print("Enter resizing.", width)
    pool = Pool(1)
    pool.starmap(resize_image, zip(
        image_dir, itertools.repeat(width), itertools.repeat(height), itertools.repeat(resize_type)))
    pool.close()
    pool.join()


def get_random_color():
    """Get a random color for bb color."""
    r=random.randint(0,255)
    g=random.randint(0,255)
    b=random.randint(0,255)
    return(r,g,b)

def draw_all_labels(df,root_folder_path,root_folder_name,logger):
    """Draw all the labels from a df.

        Args:
            df: A pd.DataFrame() containing objects and images info.
            root_folder_path: Path to root folder.
            root_folder_name: Folder name in root path.
    """
    # df=df[df.type=="Color"]
    len_images=df['file_id'].nunique()
    perc_list=[i*0.05 for i in range(0,20,1)]
    grouped_df=df.groupby(['file_id','class'])
    coordinate_names=['x_max','x_min','y_max','y_min']
    group_len=len(grouped_df)

    class_label_dict={}
    label_info_list=[]
    for ind,(name, group) in enumerate(grouped_df):
        img_name,class_name=name
        img_type=group['type'].values[0]
        bb_list=group[coordinate_names].values.astype(int)
        if class_name not in class_label_dict.keys():
            class_label_dict[class_name]=get_random_color()
        bb_color=class_label_dict[class_name]
        label_info_list.append([img_name,img_type,class_name,bb_color,bb_list])
        draw_label_on_image(root_folder_path,root_folder_name,img_name,img_type,class_name,bb_color,bb_list)
        perc=float("{:.2f}".format((ind+1)/group_len))
        if perc in perc_list:
            perc_list.remove(perc)
            logger.write("Classes annotated: "+str(ind+1)+"/"+str(group_len))
    # print("Label list generated.")
    # pool = Pool(1)
    # pool.starmap(draw_label_on_one_image, zip(
    #     label_info_list, itertools.repeat(root_folder_path), itertools.repeat(root_folder_name)))
    # pool.close()
    # pool.join()
    # print("Drawing labels is finished.")


# def draw_label_on_one_image(label_info,root_folder_path,root_folder_name):
#     img_name,img_type,class_name,bb_color,bb_list=label_info

#     img_path=os.path.join(root_folder_path,root_folder_name,img_type,img_name+".png")
#     img=cv2.imread(img_path)
#     if img is None:
#         print("img is not readable. pass")
#         return 0
#     for each_bb in bb_list:
#         cv2.rectangle(img,(each_bb[0],each_bb[2]),(each_bb[1],each_bb[3]),bb_color,3)
#         cv2.putText(img,class_name,(each_bb[0],each_bb[3]),cv2.FONT_HERSHEY_SIMPLEX,1,(255,0,0),2,cv2.LINE_AA)
#     cv2.imwrite(img_path,img)

def draw_label_on_image(root_folder_path,root_folder_name,img_name,img_type,class_name,bb_color,bb_list):
    """Create all object annotation on an image.
    
        Args:
            root_folder_path: Path to root folder.
            root_folder_name: Folder name in root path.
            img_name: the file id of the image.
            class_name: name of the object class to be drawn.
            bb_color: Color of the bounding box.
            bb_list: All bounding boxws coordinates.
    """
    img_path=os.path.join(root_folder_path,root_folder_name,img_type,img_name+".png")
    img=cv2.imread(img_path)
    for each_bb in bb_list:
        cv2.rectangle(img,(each_bb[0],each_bb[2]),(each_bb[1],each_bb[3]),bb_color,3)
        cv2.putText(img,class_name,(each_bb[0],each_bb[3]),cv2.FONT_HERSHEY_SIMPLEX,1,(255,0,0),2,cv2.LINE_AA)
    cv2.imwrite(img_path,img)

def crop_object_from_image(saving_folder,root_folder_path,root_folder_name,row_info):
    """Crop one object from an image.

        Args:
            saving_folder: Path for origin image.
            root_folder_path: Path to root folder.
            root_folder_name: Folder name in root path.
            row_info: One row in Dataframe for cropping the object
    """
    class_name=row_info['class']
    file_id=row_info['file_id']
    img_type=row_info['type']
    xmin=row_info['x_min']
    xmax=row_info['x_max']
    ymin=row_info['y_min']
    ymax=row_info['y_max']


    origin_img_path=os.path.join(root_folder_path,root_folder_name,img_type,file_id+".png")
    crop_img_path=os.path.join(saving_folder,file_id+"_"+class_name+".png")

    origin_img=cv2.imread(origin_img_path)
    crop_img=origin_img[ymin:ymax-1,xmin:xmax-1]

    # If width or height only contain 1 pixel, do not crop.
    if xmax-xmin<=2 or ymax-ymin<=2:
        print("Only one pixel, pass!")
        return 0
    # print(origin_img.shape)
    # print(xmin,xmax,ymin,ymax)
    # print(crop_img.shape)
    # print(crop_img_path)
    cv2.imwrite(crop_img_path,crop_img)



