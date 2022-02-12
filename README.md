# pytorch-video-pipeline

## Run this command first before executing python scripts.
`xhost +`

## Install requirements
```
sudo add-apt-repository ppa:nnstreamer/ppa
sudo apt-get update
sudo apt-get install nnstreamer

sudo apt install libprotobuf-dev
sudo apt-get install python-gi python3-gi
sudo apt-get install python-gst-1.0 python3-gst-1.0
sudo apt-get install python-gst-1.0-dbg python3-gst-1.0-dbg

sudo apt-get install libgstreamer1.0-0 gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav gstreamer1.0-doc gstreamer1.0-tools

apt-get install libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev libgstreamer-plugins-bad1.0-dev gstreamer1.0-plugins-base gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-plugins-ugly gstreamer1.0-libav gstreamer1.0-doc gstreamer1.0-tools gstreamer1.0-x gstreamer1.0-alsa gstreamer1.0-gl gstreamer1.0-gtk3 gstreamer1.0-qt5 gstreamer1.0-pulseaudio

sudo apt install nnstreamer nnstreamer-cpp nnstreamer-cpp-dev nnstreamer-dev nnstreamer-flatbuf nnstreamer-misc nnstreamer-protobuf nnstreamer-python3 nnstreamer-pytorch nnstreamer-tensorflow nnstreamer-tensorflow-lite
sudo apt install nnstreamer nnstreamer-cpp nnstreamer-cpp-dev nnstreamer-dev nnstreamer-flatbuf nnstreamer-misc nnstreamer-protobuf nnstreamer-python3 nnstreamer-pytorch 

sudo apt install tensorflow-lite-dev tensoorflow2-lite-dev nnstreamer-tensorflow2-lite
sudo apt install tensorflow-lite-dev tensorflow2-lite-dev nnstreamer-tensorflow2-lite

sudo apt-get install python3 python3-pip python3-setuptools python3-wheel ninja-build
```

## Setup environment variables
```
FULL_RTSP_LINK=rtsp://<IP_ADDRESS>/stream1
DEMO_LINK=rtsp://wowzaec2demo.streamlock.net/vod/mp4:BigBuckBunny_115k.mov
DEMO_LINK=https://www.freedesktop.org/software/gstreamer-sdk/data/media/sintel_trailer-480p.webm
DEMO_VIDEO=demo/city_walk.h264

USERNAME=username
PASSWORD=password
RTSP_LINK=rtsp://<IP_ADDRESS>/stream2
```


## GStreamer demo examples
```
gst-launch-1.0 -v videotestsrc pattern=ball ! video/x-raw,width=1280,height=720 ! queue ! autovideosink
gst-launch-1.0 -v videotestsrc pattern=ball ! x264enc ! mp4mux ! filesink location=output.mp4
gst-launch-1.0 -v videotestsrc num-buffers=120 ! video/x-raw,width=1920,height=1080,rate=30/1 ! xvimagesink
gst-launch-1.0 -v videotestsrc ! timeoverlay ! autovideosink
gst-launch-1.0 -e videotestsrc ! timeoverlay ! videoconvert ! x264enc !  mp4mux ! filesink location=output.mp4
gst-launch-1.0 -v filesrc location=ocean.mkv ! matroskademux ! h264parse ! splitmuxsink location=output%02d.mkv max-size-time=300000000000 muxer=matroskamux
gst-launch-1.0 -v uridecodebin uri=DEMO_LINK  ! videoconvert ! videoscale ! autovideosink
```

`-e`: wait for EOS signals to save files

## Read from a live RTSP streaming
```
# read frames from RTSP streaming
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! queue ! avdec_h264 ! videoconvert ! xvimagesink
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! queue ! avdec_h264 ! videoconvert ! autovideosink

# get frames from RTSP streaming and save as a video
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! mp4mux ! filesink location=output.mp4

# get frames from RTSP streaming and save as a split video every 10 seconds
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! queue ! splitmuxsink location=output%02d.mp4 max-size-time=10000000000

# get frames from RTSP streaming and use `tee` to split data flow and use `queue` to provide separate threads for each branch
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! tee name=t0 ! queue ! mp4mux ! filesink name=f0 location=c0_raw.mp4 t0. ! queue ! avdec_h264 ! videoconvert ! xvimagesink name=xvi0 rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! tee name=t1 ! queue ! mp4mux ! filesink name=f1 location=c1_raw.mp4 t1. ! queue ! avdec_h264 ! videoconvert ! xvimagesink name=xvi1
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! tee name=outsink ! queue ! mp4mux ! filesink location=c1_raw.mp4 outsink. ! queue ! splitmuxsink location=c1_%03d.mp4 max-size-time=10000000000
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! tee name=outsink1 ! queue ! mp4mux ! filesink location=cam1/c1_raw.mp4 outsink1. ! queue ! splitmuxsink location=cam1/c1_%03d.mp4 max-size-time=10000000000  rtspsrc location=$FULL_RTSP_LINK ! rtph264depay ! h264parse ! tee name=outsink2 ! queue ! mp4mux ! filesink location=cam2/c2_raw.mp4 outsink2. ! queue! splitmuxsink location=cam2/c2_%03d.mp4 max-size-dtime=10000000000

gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! application/x-rtp, payload=96 ! rtph264depay ! h264parse ! avdec_h264 ! decodebin ! videoconvert ! video/x-raw,format=(string)RGB ! videoconvert  ! xvimagesink
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! application/x-rtp, payload=96 ! rtph264depay ! h264parse ! video/x-h264 ! queue ! splitmuxsink location=output_%02d.mp4 max-size-time=10000000000
gst-launch-1.0 -e rtspsrc location=$FULL_RTSP_LINK ! application/x-rtp, payload=96 ! rtph264depay ! h264parse ! tee name=t ! queue ! avdec_h264 ! decodebin ! videoconvert ! video/x-raw,format=(string)RGB ! videoconvert ! xvimagesink t. ! queue ! video/x-h264 ! splitmuxsink location=output_%02d.mp4 max-size-time=10000000000

# use username and password for RTSP streaming
gst-launch-1.0 -e rtspsrc user-id="${USERNAME}" user-pw="${PASSWORD}" location=${RTSP_LINK} ! application/x-rtp, payload=96 ! rtph264depay ! h264parse ! tee name=t ! queue ! avdec_h264 ! decodebin ! videoconvert ! video/x-raw,format=(string)BGR ! videoconvert ! appsink emit-signals=true sync=false max-buffers=2 drop=true
gst-launch-1.0 -e rtspsrc user-id="${USERNAME}" user-pw="${PASSWORD}" location=${RTSP_LINK} ! application/x-rtp, payload=96 ! rtph264depay ! h264parse ! video/x-h264 ! splitmuxsink location=output_%02d.mp4 max-size-time=20000000000
gst-launch-1.0 -e rtspsrc user-id="${USERNAME}" user-pw="${PASSWORD}" location=${RTSP_LINK} ! application/x-rtp, payload=96 ! rtph264depay ! h264parse ! tee name=t ! queue ! avdec_h264 ! decodebin ! videoconvert ! video/x-raw,format=(string)BGR ! videoconvert ! appsink emit-signals=true sync=false max-buffers=2 drop=true t. ! queue ! video/x-h264 ! splitmuxsink location=output_%02d.mp4 max-size-time=10000000000
```

## Read from a video
```
gst-launch-1.0 -e filesrc location=$DEMO_VIDEO ! decodebin ! progressreport update-freq=1 ! fakesink sync=true
gst-launch-1.0 -e filesrc location=$DEMO_VIDEO ! h264parse ! queue ! avdec_h264 ! videoconvert ! fakesink sync=true
```

## References:
* https://stackoverflow.com/questions/25840509/how-to-save-a-rtsp-video-stream-to-mp4-file-via-gstreamer
* https://developer.ridgerun.com/wiki/index.php/Video_segmenter
* https://stackoverflow.com/questions/36682573/record-camera-stream-from-gstreamer/36688777
* https://forums.developer.nvidia.com/t/how-to-capture-video-using-gst-launch-1-0/60540/5
* https://stackoverflow.com/questions/4809676/splitting-segmenting-video-stream-with-gstreamer/50192019
* https://gstreamer.freedesktop.org/data/doc/gstreamer/head/gst-plugins-good/html/gst-plugins-good-plugins-splitmuxsink.html#GstSplitMuxSink--muxer
