import os
import time
import cv2
import gi
import numpy as np
import json

gi.require_version('Gst', '1.0')
from gi.repository import Gst


# > http://ardusub.com/developers/opencv.html

class Video():
    """BlueRov video capture class constructor

    Attributes:
        port (int): Video UDP port
        video_codec (string): Source h264 parser
        video_decode1 (string): Transform YUV (12bits) to BGR (24bits)
        video_pipe (object): GStreamer top-level pipeline
        video_sink (object): Gstreamer sink element
        video_sink_conf (string): Sink configuration
        video_source (string): Udp source ip and port
    """

    def __init__(self, username='sokunmin', password='eLaineBaby2020', ip='192.168.0.238', port=554):
        """Summary

        Args:
            port (int, optional): UDP port
        """

        Gst.init(None)
        self.username = username
        self.password = password
        self.ip = ip
        self.port = port
        self._frame = None

        # [Software component diagram](https://www.ardusub.com/software/components.html)
        self.video_source = 'rtspsrc user-id="{}" user-pw="{}" location=rtsp://{}:{}/stream2' \
            .format(self.username, self.password, self.ip, self.port)
        # [Rasp raw image](http://picamera.readthedocs.io/en/release-0.7/recipes2.html#raw-image-capture-yuv-format)
        # Cam -> CSI-2 -> H264 Raw (YUV 4-4-4 (12bits) I420)
        self.video_codec = '! application/x-rtp, payload=96 ! rtph264depay ! h264parse !'
        # Python don't have nibble, convert YUV nibbles (4-4-4) to OpenCV standard BGR bytes (8-8-8)
        self.video_decode1 = \
            'tee name=t ! queue ! avdec_h264 ! decodebin ! videoconvert ! video/x-raw,format=(string)BGR ! videoconvert'
        self.video_decode2 = \
            't. ! queue ! video/x-h264 ! splitmuxsink max-size-time=10000000000 sync=false async=false'
        # Create a sink to get data
        self.video_sink_conf = \
            '! appsink emit-signals=true sync=false max-buffers=2 drop=true'

        self.video_pipe = None
        self.video_sink = None
        self._real_base = None

        self.run()

    def start_gst(self, config=None):
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

        if not config:
            config = \
                [
                    'videotestsrc ! decodebin',
                    '! videoconvert ! video/x-raw,format=(string)BGR ! videoconvert',
                    '! appsink'
                ]

        command = ' '.join(config)
        self.video_pipe = Gst.parse_launch(command)
        self.video_bus = self.video_pipe.get_bus()
        self.video_pipe.set_state(Gst.State.PLAYING)
        self.video_sink = self.video_pipe.get_by_name('appsink0')
        self.file_sink = self.video_pipe.get_by_name('splitmuxsink0')
        if self.file_sink is not None:
            self.file_sink.connect("format-location-full",
                                   self.file_format_callback,
                                   None)
        print('> command=', command)

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

    def run(self):
        """ Get frame to update _frame
        """

        self.start_gst(
            [
                self.video_source,
                self.video_codec,
                self.video_decode1,
                self.video_sink_conf,
                self.video_decode2
            ])

        self.video_sink.connect('new-sample', self.video_sample_callback)

    def video_sample_callback(self, sink):
        sample = sink.emit('pull-sample')
        new_frame = self.gst_to_opencv(sample)
        self._frame = new_frame

        return Gst.FlowReturn.OK

    def calculate_times(self, sample):
        buffer = sample.get_buffer()
        segment = sample.get_segment()
        times = {}
        times['segment.time'] = segment.time
        times['stream_time'] = segment.to_stream_time(
            Gst.Format.TIME, buffer.pts)
        return times

    def file_format_callback(self,
                             unused_element,
                             unused_fragement_id,
                             sample,
                             unused_data=None):
        times = self.calculate_times(sample)
        print('> [1] file_cb=', times)
        if self._real_base is None:
            clock = Gst.SystemClock(clock_type=Gst.ClockType.REALTIME)
            self._real_base = clock.get_time()
            self._stream_base = times["segment.time"]

        adjusted_time = self._real_base + (times["stream_time"] - self._stream_base)
        print('> [2] file_cb=', adjusted_time)
        seconds = adjusted_time / 1000000000
        self._year_base = time.strftime(
            "%Y", time.localtime(adjusted_time / 1000000000))  #
        self._month_base = time.strftime(
            "%m", time.localtime(adjusted_time / 1000000000))
        self._day_base = time.strftime(
            "%d", time.localtime(adjusted_time / 1000000000))
        template = "{prefix}/{yearbase}/{monthbase}/{daybase}"
        self._dir_name = template.format(prefix='cam_0',
                                         yearbase=self._year_base,
                                         monthbase=self._month_base,
                                         daybase=self._day_base)
        try:
            os.makedirs(self._dir_name)
        except FileExistsError:
            print("> Directory already exists")

        template = "{dirname}/{adjustedtime}_{time}.mp4"
        return template.format(dirname=self._dir_name,
                               adjustedtime=adjusted_time / 1000000,  # nanoseconds to milliseconds
                               time=times["stream_time"] - self._stream_base)

    def close(self):
        self.video_pipe.send_event(Gst.Event.new_eos())
        self.video_bus.timed_pop_filtered(Gst.CLOCK_TIME_NONE, Gst.MessageType.EOS)
        self.video_pipe.set_state(Gst.State.NULL)


if __name__ == '__main__':
    video = Video()

    while True:
        # Wait for the next frame
        if not video.frame_available():
            continue

        frame = video.frame()
        cv2.imshow('frame', frame)
        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
    video.close()