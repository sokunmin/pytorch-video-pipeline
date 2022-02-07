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

logging.basicConfig(level=logging.WARNING)


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


class StreamSource(Plugin, Worker):
    """
        Uri decoder that decodes RTSP links and works as a thread.
        It invokes sync callback which is implemented in `StreamMuxer`.
    """

    def __init__(self, id: int, uri: str, sync_fn: Callable[[Plugin], int]):
        super(StreamSource, self).__init__()
        self.id = id
        self.uri = uri
        assert sync_fn is not None
        self.sync_fn = sync_fn

    def init(self, cfg: dict) -> None:
        """
            initializing plugin that is already linked.
        """
        logging.info('<{}>[{}].{}() begins'.format(self.__class__.__name__, self.id, stack()[0][3]))
        src_cfg = cfg['stream_source']
        self.__dict__.update(**src_cfg)
        super(StreamSource, self).init(cfg)
        logging.info('<{}>[{}].{}() ends'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def sync_buffer(self, minsize):
        minsize += self.min_sync_buffer
        # [1] called by StreamMuxer
        num_pop = max(0, self.buffer_qsize() - minsize)
        logging.warning('<StreamSource>[{}].sync_buffer() - qsize: {}, maxsize: {}, num_pop: {}' \
                        .format(self.id, self.buffer_qsize(), minsize, num_pop))
        if self.buffer_qsize() > minsize:
            for _ in range(num_pop):
                self.pop_buffer()

        # num_pop = max(0, self.buffer_qsize() - maxsize)
        # logging.warning('<StreamSource>[{}].sync_buffer() [1] - qsize: {}, maxsize: {}, num_pop: {}' \
        #                 .format(self.id, self.buffer_qsize(), maxsize, num_pop))
        # if self.buffer_qsize() > maxsize:
        #     for _ in range(num_pop):
        #         self.pop_buffer()
        #
        # logging.warning('<StreamSource>[{}].sync_buffer() [2] - qsize: {}, maxsize: {}' \
        #                 .format(self.id, self.in_queue.qsize(), maxsize))

    def sync_time(self, latest_time, current):
        t = current
        if self.buffer_qsize() > 0 and self.queue.queue[0] > latest_time:
            t = self.pop_buffer()
            logging.warning('    <StreamSource[{}].sync_time()(1) - current: {} -> t: {}'.format(self.id, current, t))
        logging.warning('    <StreamSource>[{}].sync_time()(2) - qsize: {}, new_t: {:.6f}, cur_t: {:.6f}, diff: {:.3f}' \
                        .format(self.id, self.buffer_qsize(), latest_time, t, latest_time - t))
        logging.warning('    <StreamSource>[{}].sync_time()(3) - check while - d({}): {} > {}, qsize({}): {} > {}' \
                        .format(self.id,
                                (latest_time - t) > self.min_sync_time,
                                latest_time - t,
                                self.min_sync_time,
                                self.buffer_qsize() > self.min_sync_buffer,
                                self.buffer_qsize(),
                                self.min_sync_buffer))
        while (latest_time - t) > self.min_sync_time and self.buffer_qsize() > self.min_sync_buffer:
            # the next timestamp <= latest or at least 2 frames in the queue.
            # TODO: check here, did it break?
            logging.warning('    <StreamSource>[{}].sync_time(4) - qsize: {} < 2 / next: {} < {}' \
                            .format(self.id, self.buffer_qsize(), self.queue.queue[0], latest_time))
            if self.buffer_qsize() < 2 or self.queue.queue[0] < latest_time:
                break
            t = self.pop_buffer()
            logging.warning('    <StreamSource>[{}].sync_time()(5) - qsize: {}, latest: {:.6f}, t: {:.6f}' \
                            .format(self.id, self.buffer_qsize(), latest_time, t))
        logging.warning('    <StreamSource>[{}].sync_time()(6) - qsize: {}, latest: {:.6f}, t: {:.6f}' \
                        .format(self.id, self.buffer_qsize(), latest_time, t))
        return t

    def _handle(self) -> None:
        # [1] sync by StreamMuxer
        t = dt.now().timestamp()
        self.put_buffer(t)

        # [2] new buffer sync by StreamSource itself.
        self.sync_fn(self)

        # [3] original way of sync called by StreamSource itself.
        # latest = self.sync_fn()
        # if latest and self.buffer_qsize() > latest:
        #     for _ in range(self.buffer_qsize() - latest):
        #         self.pop_buffer()
        # latest = self.sync_fn()
        #
        # if self.buffer_qsize() == latest:
        #     self.put_buffer(t)

        # logging.warning('<StreamSource>[{}]._handle() - in_qsize: {} - {:.6f}'.format(self.id, self.in_queue.qsize(), t))

    def release(self):
        cls_name = self.__class__.__name__
        logging.info('---------- <{}>.{}() begins'.format(cls_name, stack()[0][3]))
        logging.info('---------- <{}> release() -> TID: {}'.format(cls_name, threading.get_ident()))
        logging.info('---------- <{}>.{}() ends'.format(cls_name, stack()[0][3]))


class StreamSplitMux(Plugin, Worker):
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

    def init(self, cfg: dict) -> None:
        """
            Adding configs of mot.json directly to class attributes
            SEE the following configs in `mot.json`.
            * stream_common
            * stream_splitmux
        """
        logging.info('<{}>[{}].{}() begins'.format(self.__class__.__name__, self.id, stack()[0][3]))
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
        logging.info('<{}>[{}].{}() ends'.format(self.__class__.__name__, self.id, stack()[0][3]))

    def _handle(self) -> None:
        cls_name = self.__class__.__name__
        # if not self.buffer_empty():
        #     t = self.pop_buffer()
        #     logging.warning('@@@@@  <{}>[{}].{}() -> t: {}'.format(cls_name, self.id, stack()[0][3], t))

    def release(self):
        cls_name = self.__class__.__name__
        logging.info('---------- <{}>[{}].{}() begins'.format(cls_name, self.id, stack()[0][3]))
        if self.writer:
            self.frame_id = 0
            self.writer.release()
        logging.info('---------- <{}> release() -> TID: {}'.format(cls_name, threading.get_ident()))
        logging.info('---------- <{}>[{}].{}() ends'.format(cls_name, self.id, stack()[0][3]))


class StreamMuxer(Bin, Worker):

    def init(self, cfg: dict) -> None:
        logging.info('<{}> init() -> TID: {}'.format(self.__class__.__name__, threading.get_ident()))
        self.with_splitmux = 'stream_splitmux' in cfg
        super(StreamMuxer, self).init(cfg)

    def sync(self, p: Plugin) -> None:
        qsizes = np.array([p.buffer_qsize() for p in self._plugins])
        # TODO: [1] sync buffer called by StreamSource
        logging.warning('<Stream Muxer>[{}].sync(): qsizes {}'.format(p.id, qsizes))
        if qsizes.min() > 5:
            p.sync_buffer(qsizes.min())
            # NOTE: if sync here by StreamSource itself may cause deadlock.

        # TODO: [2] sync time called by StreamSource
        # return min([p.buffer_qsize() for p in self._plugins])

        # # TODO: [1] sync buffer called by StreamMuxer
        # qsizes = min([p.in_queue.qsize() for p in self._plugins])
        # logging.warning('<StreamMuxer>.in_qsizes: {}'.format(qsizes))
        # for p in self._plugins:
        #     p.sync_buffer(qsizes)
        #
        # # TODO: [2] sync time called by StreamMuxer
        # all_filled = all([not k.in_queue.empty() for k in self._plugins])
        # logging.warning('<StreamMuxer>.all_filled: {}'.format(all_filled))
        # if all_filled:
        #     ts = np.array([k.in_queue.get() for k in self._plugins])
        #     for i, (p, t) in enumerate(zip(self._plugins, ts)):
        #         if (ts.max() - t) > 0.33:
        #             t = p.sync_time(ts.max())
        #             ts[i] = t
        #         # TODO: put to splitmux queues
        #         p.put_buffer(t)
        #
        #     logging.warning('<StreamMuxer>.sync() - SYNCED- [{:.6f}, {:.6f}] - D: {:.6f}' \
        #                     .format(ts[0], ts[1], abs(ts[0] - ts[1])))

    # NOTE: the name is plural! `buffers_XXX()`, not `buffer_XXX()`
    def buffers_empty(self) -> bool:
        return all([p.buffer_empty() for p in self._plugins])

    def buffers_filled(self) -> bool:
        return all([not p.buffer_empty() for p in self._plugins])

    def buffers_qsize(self) -> int:
        return all([p.buffer_qsize() for p in self._plugins])

    def pop_buffers(self) -> Any:
        return np.stack([p.pop_buffer() for p in self._plugins])

    def put_buffers(self, frames: np.array) -> None:
        if self.with_splitmux:
            for p in self._plugins:
                p.put_buffer(frames)
        # TODO: tile()
        return self.queue.put(frames)

    def _handle(self) -> None:
        # self.sync()
        # logging.warning('######## <StreamMux>._handle() - {} '.format(self.buffers_filled()))
        # if self.buffers_filled():
        #     frames = self.pop_buffers()
        #     # TODO: put buffer to splitmux
        #     # self.put_buffers(frames)
        #     t = '[{:.6f}, {:.6f}] - diff: {:.6f}' \
        #         .format(frames[0], frames[1], frames.max() - frames.min())
        #     logging.warning('######## <StreamMux>._handle() -> {}'.format(t))

        # [2] original way
        qsizes = [k.buffer_qsize() for k in self._plugins]
        filled = [not k.buffer_empty() for k in self._plugins]
        empty = [k.buffer_empty() for k in self._plugins]
        logging.warning('######## <StreamMux> (1) check qsizes:{} '.format(qsizes))
        # TODO: test this
        # if not all(empty):
        if all(filled):
            ts = np.array([k.pop_buffer() for k in self._plugins])
            t_max, t_min = ts.max(), ts.min()
            d = ((t_max - ts) > 0.35).nonzero()
            logging.warning('######## <StreamMux> (2) p.sync_time() ts: [{:.6f}, {:.6f}, {:.6f}], i: {}, d: {}' \
                            .format(ts[0], ts[1], ts[2], d[0], t_max - t_min))
            if bool(d):
                for i in d[0]:
                    ts[i] = self._plugins[i].sync_time(t_max, ts[i])
            t_max, t_min = ts.max(), ts.min()
            t = '[{:.6f}, {:.6f}, {:.6f}] - diff: {:.6f}' \
                .format(ts[0], ts[1], ts[2], t_max - t_min)
            qsizes = [k.buffer_qsize() for k in self._plugins]
            logging.warning('######## <StreamMux> (3) qsizes: {} received {}' \
                            .format(qsizes, t))

            # for t, p in zip(ts, self._plugins):
            #     if p.has_sink():
            #         p.sink.put_buffer(t)

    def release(self):
        cls_name = self.__class__.__name__
        logging.info('---------- <{}>.{}() begins'.format(cls_name, stack()[0][3]))
        logging.info('---------- <{}> release() -> TID: {}'.format(cls_name, threading.get_ident()))
        logging.info('---------- <{}>.{}() ends'.format(cls_name, stack()[0][3]))


class VideoDemuxer(Bin, FileSystemEventHandler):
    # TODO: *******************************
    # TODO: change to `Thread`?
    # TODO: *******************************

    def init(self, cfg: dict) -> None:
        cls_name = self.__class__.__name__
        logging.info('<{}>.{}() begins'.format(cls_name, stack()[0][3]))
        self.queues = [Queue() for _ in self._plugins]
        super(VideoDemuxer, self).init(cfg)
        logging.info('<{}> init() -> TID: {}'.format(cls_name, threading.get_ident()))
        logging.info('<{}>.{}() ends'.format(cls_name, stack()[0][3]))

    def release(self):
        cls_name = self.__class__.__name__
        logging.info('---------- <{}>.{}() begins'.format(cls_name, stack()[0][3]))
        logging.info('---------- <{}> release() -> TID: {}'.format(cls_name, threading.get_ident()))
        logging.info('---------- <{}>.{}() ends'.format(cls_name, stack()[0][3]))


class VideoDecoder(Plugin, Worker):

    def __init__(self, id):
        super(VideoDecoder, self).__init__()
        self.id = id
        self.frame_count = 0

    def _handle(self) -> None:
        pass

    def release(self):
        cls_name = self.__class__.__name__
        logging.info('---------- <{}>.{}() begins'.format(cls_name, stack()[0][3]))
        logging.info('---------- <{}> release() -> TID: {}'.format(cls_name, threading.get_ident()))
        logging.info('---------- <{}>.{}()'.format(cls_name, stack()[0][3]))


class StreamPipeline(Pipeline, Worker):
    """
        N: number of streaming sources

                  /‾‾ [Bin|Muxer] ── [Plugin|Source1] -- [Plugin|SplitMux1]
                 |               |           ...                ...
        [Pipeline]               └──[Plugin|SourceN] -- [Plugin|SplitMuxN]
                  \⎯⎯ [Bin|Demuxer] ── [Plugin|VideoDecoder1]
                                   |           ...                ...
                                   └──[Plugin|VideoDecoderN]
    """

    def init(self, cfg: dict) -> None:
        cls_name = self.__class__.__name__
        logging.info('<{}>.{}() begins'.format(cls_name, stack()[0][3]))
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
        logging.info('<{}>.{}() -> init bin/plugins...'.format(cls_name, stack()[0][3]))
        super(StreamPipeline, self).init(cfg)
        logging.info('<{}>.{}() ends'.format(cls_name, stack()[0][3]))

    def _build_pipeline(self, cfg: dict) -> None:
        logging.info('<{}>.{}() begins'.format(self.__class__.__name__, stack()[0][3]))
        cfg_keys = cfg.keys()
        self._bins = []
        if 'stream_source' in cfg_keys:
            muxer = self._build_muxer(with_splitmux='stream_splitmux' in cfg_keys)
            # muxer = self._build_muxer(with_splitmux=False)
            self._bins.append(muxer)

        self.with_watchdog = 'video_watchdog' in cfg_keys
        if 'video_watchdog' in cfg_keys:
            demuxer = self._build_demuxer()
            self._bins.append(demuxer)

        self.main_osd = self._build_osd(self._bins)
        logging.info('<{}>.{}() ends'.format(self.__class__.__name__, stack()[0][3]))

    def _build_muxer(self, with_splitmux=True) -> StreamMuxer:
        muxer = StreamMuxer()
        for i, uri in enumerate(self.input_uri):
            source = StreamSource(id=i, uri=uri, sync_fn=muxer.sync)
            if with_splitmux:
                splitmux = StreamSplitMux(id=i)
                source.link(splitmux)
            muxer.add(source)
        return muxer

    def _build_demuxer(self):
        demuxer = VideoDemuxer()
        for i in range(self.num_sources):
            # TODO: move decoder out of this loop and bind it with pipeline
            decoder = VideoDecoder(id=i)
            demuxer.add(decoder, link=True)
        self.observer = Observer()
        self.observer.schedule(demuxer, self.watch_path, recursive=True)
        return demuxer

    def _build_osd(self, bins) -> Bin:
        # > On-Screen Display (OSD)
        self.src_names = ['stream_source', 'video_watchdog']
        main_idx = self.src_names.index(self.osd_source)
        return self._bins[main_idx]

    def _handle(self) -> None:
        # if not self.main_osd.buffer_empty():
        #     frames = self.main_osd.pop_buffer()
        #     self.put_buffer(frames)
        # logging.info('#### <StreamPipeline>._handel() -> {}'.format(frames))
        pass

    def release(self):
        cls_name = self.__class__.__name__
        logging.info('---------- <{}>.{}() begins'.format(cls_name, stack()[0][3]))
        logging.info('---------- <{}> release() -> TID: {}'.format(cls_name, threading.get_ident()))
        logging.info('---------- <{}>.{}() ends'.format(cls_name, stack()[0][3]))

    def exec(self, timeout=0.01) -> None:
        """
            super() call will execute children of objects or
             the inheritances of classes automatically.
        """
        logging.info('<{}>.{}() begins'.format(self.__class__.__name__, stack()[0][3]))
        super(StreamPipeline, self).exec()
        if self.with_watchdog:
            self.observer.start()
        # self.start()
        logging.info('<{}>.{}() ends'.format(self.__class__.__name__, stack()[0][3]))

    def quit(self, timeout=0.01) -> None:
        logging.info('<{}>.{}() begins'.format(self.__class__.__name__, stack()[0][3]))
        if self.with_watchdog:
            self.observer.stop()
        super(StreamPipeline, self).quit(timeout)
        logging.info('<{}>.{}() ends'.format(self.__class__.__name__, stack()[0][3]))


def main():
    cfg_path = 'nexretail/cfg.json'
    # cfg_path = 'nexretail/cfg_bak.json'
    with open(cfg_path) as cfg_file:
        cfg = json.load(cfg_file, cls=ConfigDecoder)
        pipeline = StreamPipeline()
        logging.info('> --------- START init() ------------')
        pipeline.init(cfg)
    logging.info('> --------- END of init() ------------\n')
    time.sleep(1)
    logging.info('> --------- START exec() ------------')
    pipeline.exec()
    logging.info('> --------- END of exec() ------------\n')

    # time.sleep(1)
    # logging.info('> --------- START quit() ------------')
    # pipeline.quit()
    # logging.info('> --------- END of quit() ')


if __name__ == '__main__':
    main()
