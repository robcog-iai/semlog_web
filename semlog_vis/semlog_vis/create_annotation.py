import pandas as pd
import os


def convert_df_to_annotation(df, bb_path, mapping_path):
    """Convert a result df to annotation and store data as txt file

    Args:
        df: Result df from calcualte bounding box.
        bb_path: path to save bounding box data.
        mapping_path: path to save class mapping dict.

    """
    df, class_mapping_dict = clean_df_for_annotation(df)
    group_by_image = df.groupby('file_id')
    result = {}
    for name, group in group_by_image:
        bb = group[['class', 'x_min', 'x_max', 'y_min', 'y_max']]
        result[name] = bb.values.tolist()
    write_bb_to_txt(result, bb_path)
    write_mapping_to_txt(class_mapping_dict, mapping_path)


def clean_df_for_annotation(df):
    """ Remove unused columns and create number mapping to classes.

    Args:
        df: Result df from calculate bounding box.

    Returns:
        df: Cleaned df.
        class_mapping_dict: Map class name to number.

    """
    class_list = sorted(list(pd.unique(df['class'])))
    class_mapping_dict = {class_name: class_list.index(class_name) + 1 for class_name in class_list}
    df = df[df.type == 'Color']
    df = df[['file_id', 'class', 'x_min', 'x_max', 'y_min', 'y_max']]
    df = df.replace({"class": class_mapping_dict})
    return df, class_mapping_dict


def write_mapping_to_txt(class_mapping_dict, mapping_path):
    """ Write mapping dict to txt.

    Args:
        class_mapping_dict: A dict maps class to number.
        mapping_path: Path to save the txt file.

    Returns:

    """
    f = open(mapping_path, 'w')
    f.write(str(class_mapping_dict))
    f.close()


def write_bb_to_txt(result, txt_path):
    """Wrtie bounding boxes to txt file

    Args:
        result: A result df contains bb information.
        txt_path: Path to save the data.

    """
    f = open(txt_path, "w")
    for each_image_id, bbs in result.items():
        f.write(os.path.join("Color", each_image_id + ".png") + " ")
        for bb in bbs:
            bb = str(bb).strip('[]').replace(" ", "")
            f.write(bb)
            f.write(" ")
        f.write('\n')
    f.close()
