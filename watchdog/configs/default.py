output_path = './outputs'
cell_size = (640, 360)
appsink = dict(mode='gst',  # `gst` or `watchdog`
               seg_maxtime=1e10,  # 10 seconds
               dst_path='/media/nexretail/NEXDISK/py_workspace/_my/bitbucket/pytorch-video-pipeline')

watchdog = dict(
    dir='/home/nexretail/Downloads/watchdog_dir',
    extension="*.mp4"
)

# test only
num_streams = 2