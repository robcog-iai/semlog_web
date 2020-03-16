from django.shortcuts import render
from django.http import HttpResponse
from django.shortcuts import redirect
import uuid
import sys
import threading
from multiprocessing.dummy import Pool
import time
import json
import shutil
import os
import datetime

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

    num_users=len(os.listdir(IMAGE_ROOT))
    return_dict = {"server_state": state,"num_users":num_users}
    print("Server state: ", state)
    return render(request, 'login.html', return_dict)

def modification_date(filename):
    t = os.path.getmtime(filename)
    t=datetime.datetime.fromtimestamp(t)
    t=t.hour*60+t.minute
    return t

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
    current_date=datetime.datetime.now()
    current_minute=current_date.hour*60+current_date.minute
    path_time=[abs(current_minute-modification_date(i)) for i in delete_path]
    timeover_list=[ind for ind,i in enumerate(path_time) if i >=120]
    delete_path_list=[delete_path[ind] for ind in timeover_list]
    try:
        pool = Pool(12)
        pool.map(clean_folder, delete_path_list)
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        pass
    # try:
    #     shutil.rmtree(IMAGE_ROOT)
    # except Exception as e:
    #     print(e)
    print("Delete all folders for:", time.time() - t1)
    if os.path.isdir(IMAGE_ROOT) is False:
        print("Create image root.")
        os.makedirs(IMAGE_ROOT)

    if user_id in user_list:
        # return HttpResponse("<h1 style='text-align:center;margin-top:300px;'>This user name is occupied. Please use another name.<h1>")
        return render(request,'message.html')

    return render(request, 'main.html')


def training(request):
    # Specify the port number, you could get this dynamically
    # through a config file or something if you wish
    new_port = '4445'

    # `request.get_host()` gives us {hostname}:{port}
    # we split this by colon to just obtain the hostname
    hostname = request.get_host().split(':')[0]
    # Construct the new url to redirect to
    url = 'http://' + hostname + ':' + new_port + '/'


    """Entrance for training the multiclass classifier."""
    user_id = request.session['user_id']
    user_root = request.session['user_root']
    search_id = request.session['search_id']


    # return HttpResponse(" <h1 style='text-align:center;margin-top:300px;'>Please install Visdom and run 'visdom' in a new terminal to start training!<h1>")

    thr = threading.Thread(target=classifier_train.train, args=(
        os.path.join(user_root,search_id,"BoundingBoxes"),10, 0.2,os.path.join(user_root,search_id),0.00001))
    thr.start()
    # classifier_train.train(
    #     dataset_path=os.path.join(user_root, search_id, "BoundingBoxes"),
    #     model_saving_path=os.path.join(user_root, search_id)
    # )
    return redirect(url)
    # return HttpResponse(" <h1 style='text-align:center;margin-top:300px;'>Training started using visdom (https://github.com/facebookresearch/visdom, see progress at http://134.102.206.230:4445/<h1>")

def read_log(request):
    if request.method == 'POST':
        user_root = request.session['user_root']
        search_id = request.session['search_id']
        user_id=request.session['user_id']
        create_a_folder(user_root)
        logger = Logger(user_root,user_id)
        log_data = logger.read()

        return_dict = {"data": log_data}
        return_dict = json.dumps(return_dict)
        return HttpResponse(return_dict)





def update_database_info(request):
    """Show avaiable database-collection in real time with ajax."""
    return_dict = {}
    neglect_list = ['admin', 'config', 'local', 'semlog_web']
    if request.method == 'POST':
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
    # Delete previous folders
    all_folders=os.listdir(user_root)
    delete_path=[os.path.join(user_root,i) for i in all_folders if i!=user_id+".log"]
    try:
        pool = Pool(12)
        pool.map(clean_folder, delete_path)
        pool.close()
        pool.join()
    except Exception as e:
        print(e)
        pass

    # Read input data
    query_input=form_dict['query_input']
    resize_input=form_dict['resize_input']
    optional_input=form_dict['optional_input']
    type_input=form_dict['type_input']

    # Create logger and folder
    create_a_folder(os.path.join(user_root, search_id))
    logger = Logger(user_root,user_id)

    logger.write("query: ")
    logger.write(query_input)
    logger.write("optional parameters: ")
    logger.write(optional_input)
    logger.write("requested image types: ")
    logger.write(type_input)
    logger.write("data manipulation requests: ")
    logger.write(resize_input)
    # Compile data
    if resize_input != "":
        customization_dict = compile_customization(resize_input)
        # logger.write("customization input: "+str(customization_dict))
    query_dict = compile_query(query_input)
    optional_dict=compile_optional_data(optional_input)
    image_type_list=compile_type_data(type_input)

    if query_dict== False or query_dict=={}:
        logger.write("no results found. Query is stopped.")
        logger.write("----------------DIVIDING LINE --------------------")
        return HttpResponse("Invalid input.")

    df = search_mongo(query_dict,optional_dict,image_type_list, logger, CONFIG_PATH)
    # Download images
    if df.shape[0]==0:
        logger.write("no results found. Query is stopped.")
        logger.write("----")
    else:
        logger.write("start downloading images...")
        t_start_download=time.time()
        download_images(root_folder_path=user_root,
                        root_folder_name=search_id, df=df, config_path=CONFIG_PATH,logger=logger)
        t_download=convert_duration_time(time.time(),t_start_download)
        logger.write("download finished ("+t_download+"s)")

        # Draw labels on images
        if 'label' in optional_dict.keys() and query_dict['search_type']=="entity":
            logger.write("start annotating images...")
            t_start_label=time.time()
            # Need to implement multirpocessing to speed up this part.
            draw_all_labels(df, user_root, search_id,logger)
            t_label=convert_duration_time(time.time(),t_start_label)
            logger.write("annotation finished ("+t_label+"s)")

        # Perform origin image crop if selected.
        if "crop" in optional_dict.keys() and query_dict['search_type']=="entity":
            logger.write("start cropping images with all bounding boxes...")
            t_start_crop=time.time()
            image_dir = scan_images(root_folder_path=user_root,
                                    root_folder_name=search_id, image_type_list=image_type_list)
            crop_with_all_bounding_box(df, image_dir)
            t_crop=convert_duration_time(time.time(),t_start_crop)
            logger.write("cropping finished ("+t_crop+"s)")

        # Retrieve local image paths
        image_dir = scan_images(root_folder_path=user_root, root_folder_name=search_id,
                                image_type_list=image_type_list, unnest=True)

        # Move scan images to the right folders
        if query_dict['search_type'] == "scan":
            # logger.write("Rearange scan images...")
            arrange_scan_by_class(df, user_root, search_id)
        # Prepare dataset
        elif "detection" in optional_dict.keys() and query_dict['search_type']=="entity":
            logger.write("start preparing dataset for object detection...")
            t_start_detection=time.time()
            if resize_input!="":
                df = recalculate_bb(df, customization_dict, image_dir)
            df.to_csv(os.path.join(user_root, search_id, 'info.csv'), index=False)
            t_detection=convert_duration_time(time.time(),t_start_detection)
            logger.write("detection preparation finished ("+t_detection+"s)")

        elif "classifier" in optional_dict.keys() and query_dict['search_type']=="entity":
            logger.write("start prepare dataset for classifier...")
            t_start_classifier=time.time()
            download_bounding_box(df, user_root, search_id)
            bounding_box_dict = scan_bb_images(
                user_root, search_id, unnest=True)
            if resize_input != "":
                customize_image_resolution(customization_dict, bounding_box_dict)
            logger.write("classifier preparation finished ("+convert_duration_time(time.time(),t_start_classifier)+"s)")
        elif resize_input != "":
            customize_image_resolution(customization_dict, image_dir)

        logger.write("results available.")
        logger.write("----")

        # Store static info in local json file
        flag_classifier=True if 'classifier' in optional_dict.keys() else False
        info = {'image_type_list': image_type_list,
                'object_id_list': query_dict['class'],
                'search_pattern': query_dict['search_type'],
                'flag_classifier':flag_classifier}
        with open(os.path.join(user_root, search_id, 'info.json'), 'w') as f:
            json.dump(info, f)

    # return render(request, 'make_your_choice.html')


def search_database(request):
    form_dict=request.POST.dict()
    print(form_dict)
    user_id=request.session['user_id']
    search_id = str(uuid.uuid4())
    request.session['search_id'] = search_id
    thr = threading.Thread(target=main_search, args=(
        form_dict, user_id, search_id))
    thr.start()
    return HttpResponse('.....')

# def start_search(request):
#     """The most important function of the website.
#         Read the form and search the db, download images to static folder."""
#     user_id = request.session['user_id']
#     form_dict = request.GET.dict()
#     search_id = str(uuid.uuid4())
#     request.session['search_id'] = search_id
#     thr = threading.Thread(target=main_search, args=(
#         form_dict, user_id, search_id))
#     thr.start()
#     return render(request, "terminal.html")


def view_images(request):
    """Entrance of viewing mode of the website."""
    user_root = request.session['user_root']
    search_id = request.session['search_id']
    with open(os.path.join(user_root, search_id, 'info.json')) as f:
        info = json.load(f)
    object_id_list = info['object_id_list']
    image_type_list = info['image_type_list']
    search_pattern = info['search_pattern']
    image_dir = scan_images(user_root, search_id, image_type_list,relative_path=True)

    # Add flag for conditional representation.
    flag_scan = False
    flag_classifier=info['flag_classifier']
    if search_pattern == "scan":
        flag_scan = True
        bounding_box_dict = scan_bb_images(
            user_root, search_id, folder_name="scans")
    else:
        bounding_box_dict = scan_bb_images(user_root, search_id)

    return render(request, 'gallery.html',
                  {"object_id_list": object_id_list,
                   "image_dir": image_dir,
                    "bounding_box": bounding_box_dict,
                     "flag_scan": flag_scan,
                     "flag_classifier":flag_classifier,
                     "image_type_list":image_type_list})


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
    logger = Logger(user_root,user_id)
    logger.write("start compressing images..")
    t_start_zip=time.time()
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
    logger.write("compressing images finished ("+convert_duration_time(time.time(),t_start_zip)+"s)")

    return response
