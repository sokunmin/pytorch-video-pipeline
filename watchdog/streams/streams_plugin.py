import logging
import os
import threading
from queue import Queue

import cv2
import glob
import json
import math
import numpy as np
import time
from inspect import stack
from pathlib import Path
from enum import Enum
from typing import Optional, Any, Tuple, Callable
from urllib.parse import urlparse
from datetime import datetime as dt
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from nexretail.decoder import ConfigDecoder
from nexretail.pipeline import Bin, Pipeline, Worker, T, Plugin

logging.basicConfig(level=logging.INFO)


class Protocol(Enum):
    IMAGE = 0
    VIDEO = 1
    CSI = 2
    V4L2 = 3
    RTSP = 4
    HTTP = 5

    def is_live(self):
        return self.value != Protocol.IMAGE and self.value != Protocol.VIDEO


def parse_uri(uri):
    result = urlparse(uri)
    if result.scheme == 'csi':
        protocol = Protocol.CSI
    elif result.scheme == 'rtsp':
        protocol = Protocol.RTSP
    elif result.scheme == 'http':
        protocol = Protocol.HTTP
    else:
        if '/dev/video' in result.path:
            protocol = Protocol.V4L2
        elif '%' in result.path:
            protocol = Protocol.IMAGE
        else:
            protocol = Protocol.VIDEO
    return protocol


def resize(image, width=None, height=None, inter=cv2.INTER_AREA):
    (h, w) = image.shape[:2]
    if width is None and height is None:
        return image
    if width is None:
        r = height / float(h)
        dim = (int(w * r), height)
    else:
        r = width / float(w)
        dim = (width, int(h * r))
    resized = cv2.resize(image, dim, interpolation=inter)
    return resized


class VideoInfo:

    def __init__(self, filepath, fps=15):
        src_id, dirname, filename, vid, start_msec, end_msec, completed = VideoInfo.parse(filepath)
        self.src_id = src_id
        self.vid = vid
        self.dirname = dirname
        self.filepath = filepath
        self.filename = filename
        self.start_msec = start_msec
        self.end_msec = end_msec
        self.diff_msec = end_msec - start_msec
        self.avg_msec = self.diff_msec / fps
        self.completed = completed

    @staticmethod
    def parse(path):
        """
           epoch time is saved in milliseconds
           Example:
               /path/to/clips/cam01/starttime_endtime_serialnumber.avi
               /path/to/clips/cam01/1632983068.195901_1632983586.173297_001.avi
       """
        dirname = os.path.dirname(path)
        filename = os.path.basename(path)
        splits = os.path.splitext(filename)[0].split('_')
        src_id = int(dirname[-1])
        if len(splits) < 3:
            starttime = float(splits[0])
            endtime = 0.
            vid = int(splits[1])
            completed = False
        else:
            starttime = float(splits[0])
            endtime = float(splits[1])
            vid = int(splits[2])
            completed = True
        logging.info('<{}>.{}()'.format('VideoInfo', stack()[0][3]))
        return src_id, dirname, filename, vid, starttime, endtime, completed

    def to_datetime(self, format='%Y-%m-%d %H:%M:%S.%f') -> Tuple[str, str]:
        assert self.start_msec != 0. and self.start_msec != 0.
        start_dt = dt.fromtimestamp(self.start_msec).strftime(format)
        end_dt = dt.fromtimestamp(self.end_msec).strftime(format)
        return start_dt, end_dt

    def exists(self) -> bool:
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))
        return Path(self.filepath).exists()


class StreamFile(Plugin):

    def __init__(self):
        super(StreamFile, self).__init__()

    def init(self,
             cfg: dict,
             uri: str,
             protocol: Protocol,
             frame_size: Tuple[int, int],
             cap_fps: int) -> None:
        proc_fps = cfg['fps']
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        output_fps = 1 / self._cap_dt(cap_fps, proc_fps, protocol.is_live())
        fourcc = cv2.VideoWriter_fourcc(*cfg['codec'])
        self.writer = cv2.VideoWriter(uri, fourcc, output_fps, frame_size, True)
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def _cap_dt(self, cap_fps, proc_fps, is_live) -> int:
        return 1 / min(cap_fps, proc_fps) if is_live else 1 / cap_fps

    def write(self, request: Optional[T]) -> None:
        assert self.writer is not None
        self.writer.write(request)
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def release(self):
        if self.writer:
            self.writer.release()
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))


class StreamSource(Plugin, Worker):
    """
        Uri decoder that decodes RTSP links and works as a thread.
        It invokes sync callback which is implemented in `StreamMuxer`.
    """

    def __init__(self, id: int, uri: str, sync_fn: Callable[[], int]):
        super(StreamSource, self).__init__()
        self.id = id
        self.uri = uri
        self.cap = cv2.VideoCapture(uri)
        assert sync_fn is not None
        self.sync_fn = sync_fn
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def init(self, cfg: dict) -> None:
        """
            initializing plugin that is already linked.
        """
        src_cfg = cfg['stream_source']
        self.__dict__.update(**src_cfg)
        self.queue = Queue(maxsize=self.max_buffer)
        if self.has_sink():
            self.sink.init(cfg)
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def info(self) -> Tuple[int, int, int]:
        ret, frame = self.cap.read()
        if not ret:
            raise RuntimeError('Unable to read video stream')
        self.width = self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)
        self.height = self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)
        self.fps = self.cap.get(cv2.CAP_PROP_FPS)
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))
        return self.width, self.height, self.fps

    def _handle(self) -> None:
        """
            When the camera frame rate is not equal, only the smallest queue can add pictures
            If the judgment is not set, the queue will be blocked and will not be added until
            the queue is empty. In this case, the time of four frames of pictures may not
            correspond to.
        """
        ret, frame = self.cap.read()
        # TODO: get system time
        if ret:
            latest = self.sync_fn()
            if latest and self.qsize() > latest:
                for _ in range(self.qsize() - latest):
                    self.pop_buffer()
            latest = self.sync_fn()
            if self.qsize() == latest:
                self.put_buffer(frame)
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def release(self) -> None:
        if self.cap is not None:
            self.cap.release()
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))


class StreamSplitMux(Worker, Plugin):
    """
        Split streaming frames and write to a video.
        There are 3 directories that can be setup in config:
        `temp_dirname`: works as cache mechanism for recoding temporarily
        `process_dirname`: once recoding reaches `max_frames` frames, video will
                           move to this directory.
        `archive_dirname`: archive videos if they have been processed.
    """
    def __init__(self, id):
        super(StreamSplitMux, self).__init__()
        self.id = id
        self.video_id = -1
        self.frame_id = 0
        self.writer = None
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def init(self, cfg: dict) -> None:
        """
            Adding configs of mot.json directly to class attributes
            SEE the following configs in `mot.json`.
            * stream_common
            * stream_splitmux
        """
        common_cfg = cfg['stream_common']
        self.__dict__.update(**common_cfg)
        splitmux_cfg = cfg['stream_splitmux']
        self.__dict__.update(**splitmux_cfg)
        self.arch_dir = os.path.join(self.root_dir, self.archive_dirname)
        self.proc_dir = os.path.join(self.root_dir, self.process_dirname)
        self.temp_dir = os.path.join(self.root_dir, self.temp_dirname)
        self.pre_scan = splitmux_cfg['pre_scan']

        self.max_frames = self.split_seconds * self.frame_rate
        self.fourcc = cv2.VideoWriter_fourcc(*self.video_codec)
        if self.has_sink():
            self.sink.init(cfg)
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def preprocess(self) -> None:
        # > create dirs for saving video clips
        src_id = '{:>02d}'.format(self.id)
        self.temp_srcdir = self._make_dirs(self.temp_dir, src_id)
        self.proc_srcdir = self._make_dirs(self.proc_dir, src_id)
        self.arch_srcdir = self._make_dirs(self.arch_dir, src_id)
        if self.pre_scan:
            self._scan(dir=self.temp_dir,
                       ext=self.video_extension)
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def _sort_by_id(self, x):
        """
            Get video id from filename for sorting in ascending order.
            File format: <start_time>_<end_time>_<video_id>.<extension>
            For example:
                1633409816.391690_1633409816.391690_000.avi → 000.avi
        """
        return x[-7:]

    def _scan(self, dir, ext):
        files = glob.glob(dir + "/*." + ext, recursive=True)
        files.sort(key=self._sort_by_id)
        return files

    def _handle(self) -> None:
        if not self.buffer_empty():
            f = self.pop_buffer()
            self.write(f)

    def postprocess(self) -> None:
        if self.keep_frames_on_exit:
            while not self.buffer_empty():
                f = self.pop_buffer()
                self.write(f)
        self._move_video(self.filepath, self.proc_srcdir)
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def write(self, frame: np.array) -> None:
        if not self.writer:
            self._make_video()
        self.frame_id += 1
        self.writer.write(frame)
        # > save a new clip every `max_frames` frames.
        if self.writer and self.frame_id >= self.max_frames:
            self._make_video(remake=True)
            logging.info('<{}>[{}].{}() - make a new video'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def _make_dirs(self, root_dir, src_id) -> str:
        path = '{root}/cam{src_id}'.format(root=root_dir, src_id=src_id)
        Path(path).mkdir(parents=True, exist_ok=True)
        return path

    def _make_video(self, remake=False):
        if remake:
            self.frame_id = 0
            self.writer.release()
            self._move_video(self.filepath, self.proc_srcdir)
        self.video_id += 1
        self.time_start = dt.now().timestamp()
        self.filepath = '{dirs}/{time_start}_{video_id}.{ext}' \
            .format(dirs=self.temp_srcdir,
                    time_start='{:<17f}'.format(self.time_start),
                    video_id='{:>03d}'.format(self.video_id),
                    ext=self.video_extension)
        self.writer = cv2.VideoWriter(self.filepath, self.fourcc, self.frame_rate, self.frame_size)

    def _move_video(self, src_file, dst_dir) -> None:
        new_filepath = '{dirs}/{time_start}_{time_end}_{video_id}.{ext}' \
            .format(dirs=dst_dir,
                    time_start='{:<17f}'.format(self.time_start),
                    time_end='{:<17f}'.format(dt.now().timestamp()),
                    video_id='{:>03d}'.format(self.video_id),
                    ext=self.video_extension)
        Path(src_file).rename(new_filepath)

    def release(self):
        if self.writer:
            self.frame_id = 0
            self.writer.release()
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))


class StreamMuxer(Bin):
    # TODO: *******************************
    # TODO: inherit `Worker`
    # TODO: *******************************
    def __init__(self):
        super(StreamMuxer, self).__init__()
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def init(self, cfg: dict) -> None:
        for p in self._plugins:
            p.init(cfg)
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def sync(self) -> int:
        assert len(self._plugins) > 0 and all(hasattr(p, 'buffer_qsize') for p in self._plugins)
        return min([p.buffer_qsize() for p in self._plugins])

    def read(self) -> np.array:
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))
        return np.stack([p.pop_buffer() for p in self._plugins])

    def filled(self) -> bool:
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))
        return all([not p.buffer_empty() for p in self._plugins])


class VideoDemuxer(Bin, FileSystemEventHandler):
    # TODO: *******************************
    # TODO: change to `Thread`?
    # TODO: *******************************
    def __init__(self):
        super(VideoDemuxer, self).__init__()
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def init(self, cfg: dict) -> None:
        self.queues = []
        for p in self._plugins:
            p.init(cfg)
            self.queues.append(Queue())
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def on_any_event(self, event) -> None:
        if not event.is_directory:
            self.write(VideoInfo(event.src_path))
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def pop_buffer(self, idx) -> Any:
        return self.queues[idx].get()

    def read(self) -> np.array:
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))
        return np.stack([p.buffer_qsize() for p in self._plugins])

    def write(self, info: VideoInfo) -> None:
        # > there are 2 `_` if a video clip is completed correctly.
        if (0 <= info.src_id < self.size()) and len(info.filename.split('_')) >= 3:
            self.queues[info.src_id].put(info)
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def filled(self) -> bool:
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))
        return all([not p.buffer_empty() for p in self._plugins])


class VideoDecoder(Plugin, Worker):

    def __init__(self, id):
        super(VideoDecoder, self).__init__()
        self.id = id
        self.frame_count = 0
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def init(self, cfg: dict) -> None:
        if self.has_sink():
            self.sink.init(cfg)
        logging.info('<{}>[{}].{}()'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def _handle(self) -> None:
        if not self.buffer_empty():
            v = self.src.pop_buffer(self.id)  # => `VideoDemuxer.pop_buffer(src_id)`
            if v.exists():
                count = self._decode(v)
                self.frame_count += count
                logging.info('<{}>[{}].{}() - frame #{}' \
                             .format(self.__class__.__name__,
                                     self.id,
                                     stack()[0][3],
                                     self.frame_count))
            else:
                time.sleep(0.01)
        else:
            time.sleep(0.01)

    def _decode(self, info: VideoInfo) -> int:
        frame_count = 0
        cap = cv2.VideoCapture(info.filepath)
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            frame_count += 1
            self.put_buffer(frame)
        cap.release()
        return frame_count


class StreamPipeline(Bin, Worker):
    """
                  /‾‾ [Bin|Muxer] ── [Plugin|Source1] -- [Plugin|SplitMux1]
                 |               |           ...                ...
        [Pipeline]               └──[Plugin|SourceN] -- [Plugin|SplitMuxN]
                  \⎯⎯ [Bin|Demuxer] ── [Plugin|VideoDecoder1]
                                   |           ...                ...
                                   └──[Plugin|VideoDecoderN]
    """
    def __init__(self):
        super(StreamPipeline, self).__init__()
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def init(self, cfg: dict) -> None:
        self.__dict__.update(**cfg['stream_io'])
        self.__dict__.update(**cfg['stream_common'])
        splitmux_cfg = cfg['stream_splitmux']
        self.watch_path = os.path.join(splitmux_cfg['root_dir'],
                                       splitmux_cfg['process_dirname'])
        assert isinstance(self.input_uri, (list, tuple)) and len(self.input_uri) > 0
        self.protocol = parse_uri(self.input_uri[0])
        self.num_sources = len(self.input_uri)
        tile_rows = round(math.sqrt(self.num_sources))
        tile_cols = math.ceil(self.num_sources / tile_rows)
        self.tile_size = (tile_rows, tile_cols)
        self.tile_resolution = (self.frame_size[0] * tile_cols,
                                self.frame_size[1] * tile_rows)
        self._build_pipeline(cfg)
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def _build_pipeline(self, cfg: dict) -> None:
        cfg_keys = cfg.keys()
        bins = []
        if 'stream_source' in cfg_keys:
            muxer = self._build_muxer(cfg, with_splitmux='stream_splitmux' in cfg_keys)
            bins.append(muxer)

        if 'video_watchdog' in cfg_keys:
            demuxer = self._build_demuxer(cfg)
            bins.append(demuxer)

        self.main_osd = self._build_osd(bins)
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def _build_muxer(self, cfg, with_splitmux=True) -> StreamMuxer:
        muxer = StreamMuxer()
        for i, uri in enumerate(self.input_uri):
            source = StreamSource(id=i, uri=uri, sync_fn=muxer.sync)
            if with_splitmux:
                splitmux = StreamSplitMux(id=i)
                source.link(splitmux)
            muxer.add(source)
        muxer.init(cfg)
        return muxer

    def _build_demuxer(self, cfg):
        self.enable_watchdog = True
        self.observer = Observer()
        demuxer = VideoDemuxer()
        for i in range(self.num_sources):
            # TODO: move decoder out of this loop and bind it with pipeline
            decoder = VideoDecoder(id=i)
            demuxer.add(decoder, link=True)
        demuxer.init(cfg)
        return demuxer

    def _build_osd(self, bins) -> Bin:
        # > On-Screen Display (OSD)
        self.src_names = ['streams', 'videos']
        self._bins = dict(zip(self.src_names, bins))
        bin = self._bins.get(self.osd_source, None)
        if bin is None:
            raise KeyError('Unknown On-Screen Display mode found! ', self.osd_source)
        return bin

    def _tiles(self, frames) -> np.array:
        rows, cols = self.tile_size
        cell_w, cell_h = self.frame_size
        tiled_image = np.zeros([cell_h * rows, cell_w * cols, 3], np.uint8)
        for i, f in enumerate(frames):
            col_idx = i % cols
            row_idx = math.floor(i / cols)
            row_start, row_end = cell_h * row_idx, cell_h * (row_idx + 1)
            col_start, col_end = cell_w * col_idx, cell_w * (col_idx + 1)
            if cell_w != f.shape[1]:
                f = resize(f, width=cell_w)
            tiled_image[row_start:row_end, col_start:col_end] = f
        return tiled_image

    def _handle(self) -> None:
        if self.main_osd.filled():
            frames = self.main_osd.read()
            image = self._tiles(frames)
            self.put_buffer(image)

    def postprocess(self) -> None:
        if self.enable_watchdog:
            self.observer.stop()
            self.observer.join()
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def exec(self, timeout=0.01) -> None:
        super(StreamPipeline, self).exec()
        if self.enable_watchdog:
            self.observer.schedule(self.demuxer, self.watch_path, recursive=True)
            self.observer.start()
        # TODO: start muxer, demuxer and other plugins
        self.start()
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))

    def quit(self) -> None:
        super(StreamPipeline, self).quit()
        # TODO: stop muxer, demuxer and other plugins
        logging.info('<{}>.{}()'.format(self.__class__.__name__, stack()[0][3]))


def main():
    cfg_path = 'cfg.json'
    with open(cfg_path) as cfg_file:
        cfg = json.load(cfg_file, cls=ConfigDecoder)
        pipeline = StreamPipeline()
        pipeline.init(cfg)
    print()


if __name__ == '__main__':
    main()