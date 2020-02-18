
from django.shortcuts import render
from django.http import HttpResponse
import uuid
import sys
import threading
from multiprocessing.dummy import Pool
import time
import json
import shutil

import os

try:
    from website.settings import IMAGE_ROOT, CONFIG_PATH
except Exception as e:
    os.system("git submodule init")
    os.system("git submodule update")
    from website.settings import IMAGE_ROOT, CONFIG_PATH
from image_path.utils import create_a_folder
from image_path.image_path import *
from image_path.logger import Logger
from semlog_vis.semlog_vis.bounding_box import *
from semlog_vis.semlog_vis.image import *
from semlog_mongo.semlog_mongo.utils import *
from semlog_mongo.semlog_mongo.mongo import *
try:
    import models.classifier.train as classifier_train
except Exception as e:
    print("Please install pytorch to use the training functions.")


def clean_folder(x):
    """delete old folders."""

    t1 = time.time()
    try:
        shutil.rmtree(x)
    except Exception as e:
        print(e)
    print(os.listdir(x))
    print("remove", x)
    print("delete folder for:", time.time() - t1)
    return x


def log_out(request):
    user_id = request.session['user_id']
    user_folder = os.path.join(IMAGE_ROOT, user_id)
    try:
        shutil.rmtree(user_folder)
    except Exception as e:
        print(e)
    print("User: ", user_id, " is logged out.")
    return HttpResponse("Logged out.")


def login(request):
    server_state = check_mongodb_state(CONFIG_PATH)
    state = "Online" if server_state == True else "Offline"
    return_dict = {"server_state": state}
    print("Server state: ", state)
    return render(request, 'login.html', return_dict)


def search(request):
    """Delete old folders before search."""
    t1 = time.time()
    if request.method == "GET":
        login_dict = request.GET.dict()
        request.session['user_id'] = user_id = login_dict['user_id']
        request.session['user_root'] = os.path.join(IMAGE_ROOT, user_id)

    if os.path.isdir(IMAGE_ROOT) is False:
        print("Create image root.")
        os.makedirs(IMAGE_ROOT)
    delete_path = os.listdir(IMAGE_ROOT)
    user_list = delete_path
    delete_path = [os.path.join(IMAGE_ROOT, i)
                   for i in delete_path]
    try:
        pool = Pool(12)
        pool.map(clean_folder, delete_path)
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        pass
    try:
        shutil.rmtree(IMAGE_ROOT)
    except Exception as e:
        print(e)
    print("Delete all folders for:", time.time() - t1)
    if os.path.isdir(IMAGE_ROOT) is False:
        print("Create image root.")
        os.makedirs(IMAGE_ROOT)

    if user_id in user_list:
        return HttpResponse("<h1 style='text-align:center;margin-top:300px;'>This user name is occupied. Please use another name.<h1>")

    return render(request, 'main.html')


def training(request):
    """Entrance for training the multiclass classifier."""
    user_id = request.session['user_id']
    user_root = request.session['user_root']
    search_id = request.session['search_id']
    classifier_train.train(
        dataset_path=os.path.join(user_root, search_id, "BoundingBoxes"),
        model_saving_path=os.path.join(user_root, search_id)
    )
    return HttpResponse("Model starts training. Progress can be seen in port 8097.")


def read_log(request):
    if request.method == 'POST':
        user_root = request.session['user_root']
        search_id = request.session['search_id']
        logger = Logger(os.path.join(user_root, search_id))
        log_data = logger.read()
        return_dict = {"data": log_data}
        return_dict = json.dumps(return_dict)
        return HttpResponse(return_dict)


def update_database_info(request):
    """Show avaiable database-collection in real time with ajax."""
    return_dict = {}
    neglect_list = ['admin', 'config', 'local', 'semlog_web']
    if request.method == 'POST':
        print("enter update database!")
        print(request.POST.dict())
        ip, username, password = load_mongo_account(CONFIG_PATH)
        m = MongoClient(ip, username=username, password=password)
        db_list = m.list_database_names()
        db_list = [i for i in db_list if i not in neglect_list]
        for db in db_list:
            return_dict[db] = [
                i for i in m[db].list_collection_names() if "." not in i]

        return_dict = get_db_info(return_dict, CONFIG_PATH)
        return_dict = json.dumps(return_dict)

        return HttpResponse(return_dict)
    else:
        return HttpResponse("Failed!")


def show_one_image(request):
    img_path = request.GET['img_path']
    dic = {}
    dic['img_path'] = img_path
    return render(request, 'origin_size.html', dic)


def main_search(form_dict, user_id, search_id):
    # Create root folder
    user_root = os.path.join(IMAGE_ROOT, user_id)
    create_a_folder(user_root)
    create_a_folder(os.path.join(user_root, search_id))
    logger = Logger(os.path.join(user_root, search_id))
    query_data = form_dict['query_data']
    print(form_dict)
    if form_dict['customization_data'] != "":
        customization_data = form_dict['customization_data']
        customization_dict = compile_customization(customization_data)
        logger.write("customization input: "+str(customization_dict))
    query_dict = compile_query(query_data)

    df = search_mongo(query_dict, logger, CONFIG_PATH)
    # Download images
    logger.write("Start downloading images...")
    download_images(root_folder_path=user_root,
                    root_folder_name=search_id, df=df, config_path=CONFIG_PATH)
    logger.write("Download finished.")

    # Draw labels on images
    if 'label' in query_dict.keys():
        logger.write("Start annotating images...")
        draw_all_labels(df, user_root, search_id)
        logger.write("Annotation finished.")

    # Perform origin image crop if selected.
    if "crop" in query_dict.keys():
        logger.write("Cropping images with all bounding boxes..")
        image_dir = scan_images(root_folder_path=user_root,
                                root_folder_name=search_id, image_type_list=query_dict['type'])
        crop_with_all_bounding_box(df, image_dir)
        logger.write("Cropping finished.")

    # Retrieve local image paths
    image_dir = scan_images(root_folder_path=user_root, root_folder_name=search_id,
                            image_type_list=query_dict['type'], unnest=True)

    # Move scan images to the right folders
    if query_dict['search_type'] == "scan":
        logger.write("Rearange scan images...")
        arrange_scan_by_class(df, user_root, search_id)
    # Prepare dataset
    elif "detection" in query_dict.keys():
        logger.write("Prepare dataset for object detection.")
        df = recalculate_bb(df, customization_dict, image_dir)
        df.to_csv(os.path.join(user_root, search_id, 'info.csv'), index=False)

    elif "classifier" in query_dict.keys():
        logger.write("Prepare dataset for classifier.")
        download_bounding_box(df, user_root, search_id)
        bounding_box_dict = scan_bb_images(
            user_root, search_id, unnest=True)
        if form_dict['customization_data'] != "":
            customize_image_resolution(customization_dict, bounding_box_dict)
    elif form_dict['customization_data'] != "":
        customize_image_resolution(customization_dict, image_dir)

    logger.write("Query succeeded.")
    logger.write("Click buttons below to utilize results.")

    # Store static info in local json file
    info = {'image_type_list': query_dict['type'],
            'object_id_list': query_dict['class'],
            'search_pattern': query_dict['search_type']}
    with open(os.path.join(user_root, search_id, 'info.json'), 'w') as f:
        json.dump(info, f)

    # return render(request, 'make_your_choice.html')


def start_search(request):
    """The most important function of the website.
        Read the form and search the db, download images to static folder."""
    user_id = request.session['user_id']
    form_dict = request.GET.dict()
    search_id = str(uuid.uuid4())
    request.session['search_id'] = search_id
    thr = threading.Thread(target=main_search, args=(
        form_dict, user_id, search_id))
    thr.start()
    return render(request, "terminal.html")


def view_images(request):
    """Entrance of viewing mode of the website."""
    user_root = request.session['user_root']
    search_id = request.session['search_id']
    with open(os.path.join(user_root, search_id, 'info.json')) as f:
        info = json.load(f)
    object_id_list = info['object_id_list']
    image_type_list = info['image_type_list']
    search_pattern = info['search_pattern']
    image_dir = scan_images(user_root, search_id, image_type_list)
    flag_scan = False
    if search_pattern == "scan":
        flag_scan = True
        bounding_box_dict = scan_bb_images(
            user_root, search_id, folder_name="scans")
    else:
        bounding_box_dict = scan_bb_images(user_root, search_id)

    return render(request, 'gallery.html',
                  {"object_id_list": object_id_list, "image_dir": image_dir, "bounding_box": bounding_box_dict, "flag_scan": flag_scan})


def download(request):
    """Download images as .zip file. """

    def make_archive(source, destination):
        print(source, destination)
        base = os.path.basename(destination)
        name = base.split('.')[0]
        format = base.split('.')[1]
        archive_from = os.path.dirname(source)
        archive_to = os.path.basename(source.strip(os.sep))
        print(source, destination, archive_from, archive_to)
        shutil.make_archive(name, format, archive_from, archive_to)
        shutil.move('%s.%s' % (name, format), destination)

    user_id = request.session['user_id']
    user_root = request.session['user_root']
    search_id = request.session['search_id']
    zip_target = os.path.join(user_root, search_id)
    zip_path = os.path.join(user_root, search_id, "Color_images.zip")
    make_archive(zip_target, zip_path)
    print("finish zip.")
    zip_file = open(zip_path, '+rb')
    response = HttpResponse(zip_file, content_type='application/zip')
    response[
        'Content-Disposition'] = 'attachment; filename=%s' % "dataset.zip"
    response['Content-Length'] = os.path.getsize(zip_path)
    zip_file.close()

    return response
