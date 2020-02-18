
import os
import sys
import numpy as np
import cv2
import numpy as np
sys.path.append('..')
from semlog_vis.image import *




if __name__=="__main__":

    img=cv2.imread('rgb.png')
    # img=pad_image(img,400,100,cv2.BORDER_CONSTANT,(255,000,000))
    # mask=cv2.imread('mask.png')
    # mask=cv2.cvtColor(mask,cv2.COLOR_BGR2RGB)

    img=pad_image(img_path='rgb2.png',width=30,height=80,pad_type=cv2.BORDER_CONSTANT,value=[255,0,0])
    # color=(94,101,210)
    # a,b,c,d=cut_object('rgb.png','mask.png',color)
    # print(a,b,c,d)
    # cv2.rectangle(img,(c,a),(d,b),(255,0,0),1)
    # # img=remove_image_background(img,mask,color)
    # cv2.imshow("img",img)
    # cv2.waitKey(0)
    # cv2.destroyAllWindows()

