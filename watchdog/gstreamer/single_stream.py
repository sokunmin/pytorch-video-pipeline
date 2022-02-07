import os
import time
import cv2
import gi
import numpy as np
import argparse
from mmcv import Config
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject


# > http://ardusub.com/developers/opencv.html
# > https://github.com/wanZzz6/Modules-Learn/blob/e1f95405e8ebe87909c685112783c114cee80530/%E6%A1%86%E6%9E%B6/Gstreamer/OpenCV%E4%B8%8EGstreamer.ipynb
# > https://github.com/wanZzz6/Modules-Learn/blob/e1f95405e8ebe87909c685112783c114cee80530/%E6%A1%86%E6%9E%B6/Gstreamer/gstreamer%20-opencv.ipynb
TO_SECOND = 1e9


class VideoStream():
    """BlueRov video capture class constructor

    Attributes:
        port (int): Video UDP port
        codec (string): Source h264 parser
        branch_osd (string): Transform YUV (12bits) to BGR (24bits)
        pipeline (object): GStreamer top-level pipeline
        video_sink (object): Gstreamer sink element
        appsink (string): Sink configuration
        source (string): Udp source ip and port
    """

    def __init__(self, config):
        """Summary

        Args:
            port (int, optional): UDP port
        """
        assert isinstance(config, Config)
        self.config = config
        self._frame = None
        self._real_base = None
        assert config.appsink.mode in ['gst', 'watchdog']
        self.build_pipeline(config)
        self.launch()

    def build_pipeline(self, config):
        """ Build entire pipeline
            Get frame to update _frame
            References:
                [Software component diagram](https://www.ardusub.com/software/components.html)
                [Rasp raw image](http://picamera.readthedocs.io/en/release-0.7/recipes2.html#raw-image-capture-yuv-format)
        """
        pipeline = ''
        num_src = len(config.uri)
        if config.appsink.mode == 'gst':
            for i, u in enumerate(config.uri):
                pipeline += self.build_pipeline_branch(i, u, config.appsink.seg_maxtime)
        elif config.appsink.mode == 'watchdog':
            watchdog = None
        else:
            raise ValueError("> Unknown args: ", config.mode)
        print('> command=', pipeline)
        self.pipeline = Gst.parse_launch(pipeline)
        self.bus = self.pipeline.get_bus()

        if config.appsink.mode == 'gst':
            for i in range(num_src):
                self.connect_sink(src_id=i)

    def build_pipeline_branch(self, src_id, uri, seg_maxtime):
        # > [Source]
        pipeline = 'rtspsrc location={uri}'.format(uri=uri)
        # > [Codec] Cam -> CSI-2 -> H264 Raw (YUV 4-4-4 (12bits) I420)
        pipeline += ' ! application/x-rtp, payload=96 ! rtph264depay ! h264parse'

        # > [Decode] Python don't have nibble, convert YUV nibbles (4-4-4) to OpenCV standard BGR bytes (8-8-8)
        pipeline += \
            ' ! tee name=t{src_id} ! queue ! avdec_h264 ! decodebin' \
            ' ! videoconvert ! video/x-raw,format=(string)BGR ! videoconvert' \
                .format(src_id=src_id)
        # > [Appsink]
        pipeline += \
            ' ! appsink name=appsink{src_id} emit-signals=true sync=false max-buffers=2 drop=true' \
                .format(src_id=src_id)
        # > [Filesink]
        pipeline += \
            ' t{src_id0}. ! queue ! video/x-h264 ! splitmuxsink name=splitmuxsink{src_id1}' \
            ' max-size-time={maxtime} sync=false async=false ' \
                .format(src_id0=src_id, src_id1=src_id, maxtime=str(int(seg_maxtime + 0.5)))
        return pipeline

    def connect_sink(self, src_id):
        self.sample_appsink = self.pipeline.get_by_name('appsink{}'.format(src_id))
        self.sample_filesink = self.pipeline.get_by_name('splitmuxsink{}'.format(src_id))

        # > connect plugin by callback
        self.sample_appsink.connect('new-sample', self.on_video_new_sample)
        if self.sample_filesink is not None:
            self.sample_filesink.connect("format-location-full",
                                         self.on_fileformat_new_sample,
                                         src_id)

    def launch(self):
        """ Start gstreamer pipeline and sink
        Pipeline description list e.g:
            [
                'videotestsrc ! decodebin', \
                '! videoconvert ! video/x-raw,format=(string)BGR ! videoconvert',
                '! appsink'
            ]

        Args:
            config (list, optional): Gstreamer pileline description list
        """
        self.pipeline.set_state(Gst.State.PLAYING)  # TODO: move to the end of this function

    @staticmethod
    def gst_to_opencv(sample):
        """Transform byte array into np array

        Args:
            sample (TYPE): Description

        Returns:
            TYPE: Description
        """
        buf = sample.get_buffer()
        caps = sample.get_caps()
        array = np.ndarray(
            (
                caps.get_structure(0).get_value('height'),
                caps.get_structure(0).get_value('width'),
                3
            ),
            # Extracts a copy of at most size bytes the data at offset into newly-allocated memory.
            buffer=buf.extract_dup(0, buf.get_size()), dtype=np.uint8)
        return array

    def frame(self):
        """ Get Frame

        Returns:
            iterable: bool and image frame, cap.read() output
        """
        return self._frame

    def frame_available(self):
        """Check if frame is available

        Returns:
            bool: true if frame is available
        """
        return type(self._frame) != type(None)

    def on_video_new_sample(self, sink):
        sample = sink.emit('pull-sample')
        new_frame = self.gst_to_opencv(sample)
        self._frame = new_frame

        return Gst.FlowReturn.OK

    def calculate_time(self, sample):
        buffer = sample.get_buffer()
        segment = sample.get_segment()
        times = {}
        times['segment.time'] = segment.time
        times['stream_time'] = segment.to_stream_time(
            Gst.Format.TIME, buffer.pts)
        return times

    def on_fileformat_new_sample(self,
                                 unused_element,
                                 unused_fragement_id,
                                 sample,
                                 src_id):
        nano_time = self.calculate_time(sample)
        print('> [pid=', os.getpid(), '] on_fileformat_new_sample() → #cam = ', src_id)
        if self._real_base is None:
            clock = Gst.SystemClock(clock_type=Gst.ClockType.REALTIME)
            self._real_base = clock.get_time()
            self._stream_base = nano_time["segment.time"]

        adjusted_time = self._real_base + (nano_time["stream_time"] - self._stream_base)
        epoch_time = time.localtime(adjusted_time / TO_SECOND)
        template = "{yearbase}/{monthbase}/{daybase}/{prefix}"
        self._dir_name = template.format(yearbase=time.strftime("%Y", epoch_time),
                                         monthbase=time.strftime("%m", epoch_time),
                                         daybase=time.strftime("%d", epoch_time),
                                         prefix='cam_{src_id}'.format(src_id=src_id))
        try:
            os.makedirs(self._dir_name)
        except FileExistsError:
            print("> Directory already exists")

        template = "{dirname}/{adjustedtime}_{time}.mp4"
        return template.format(dirname=self._dir_name,
                               adjustedtime=adjusted_time / 1000000,  # nanoseconds to milliseconds
                               time=nano_time["stream_time"] - self._stream_base)

    def close(self):
        self.pipeline.send_event(Gst.Event.new_eos())
        self.bus.timed_pop_filtered(Gst.CLOCK_TIME_NONE, Gst.MessageType.EOS)
        self.pipeline.set_state(Gst.State.NULL)


def parse_args():
    parser = argparse.ArgumentParser(description='Train a detector')
    parser.add_argument('config', default='configs/default.py', help='train config file path')
    parser.add_argument('--uri', help='the dir to save logs and models')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    GObject.threads_init()
    Gst.init(None)

    args = parse_args()
    cfg = Config.fromfile(args.config)
    if args.uri is not None:
        cfg.uri = args.uri
        if not isinstance(cfg.uri, (tuple, list)):
            cfg.uri = [cfg.uri]
    video = VideoStream(cfg)

    while True:
        # Wait for the next frame
        if not video.frame_available():
            continue

        frame = video.frame()
        cv2.imshow('frame', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    video.close()