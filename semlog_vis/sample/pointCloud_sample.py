import sys

sys.path.append("..")
from semlog_vis.PointCloudGenerator import PointCloudGenerator
rgb = 'd.png'
depth = 'd.png'


a = PointCloudGenerator(rgb, depth, 
                        focal_length=30, scalingfactor=10)
a.pc_file='pc.ply'
a.calculate()
a.write_ply('pc.ply')
a.show_point_cloud()
# a.save_npy()
# a.save_csv()
