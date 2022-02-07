import os
import cv2
import logging
import math
import time
import threading
import multiprocessing
import numpy as np
from queue import Queue
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, FileSystemEventHandler
from datetime import datetime as dt
ENABLE_DEBUG_MSG = False


class VideoSource:

    def __init__(self, filepath, fps=15):
        src_id, dirname, filename, vid, start_msec, end_msec, completed = VideoSource.parse(filepath)
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
        print('> path: ', path)
        dirname = os.path.dirname(path)
        filename = os.path.basename(path)
        splits = os.path.splitext(filename)[0].split('_')
        src_id = int(dirname[-1])
        print('> splits: ', splits)
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
        return src_id, dirname, filename, vid, starttime, endtime, completed

    def to_datetime(self, format='%Y-%m-%d %H:%M:%S.%f'):
        assert self.start_msec != 0. and self.start_msec != 0.
        start_dt = dt.fromtimestamp(self.start_msec).strftime(format)
        end_dt = dt.fromtimestamp(self.end_msec).strftime(format)
        return start_dt, end_dt

    def archive(self):
        new_filepath = self.filepath.replace('/tmp/', '/save/')
        os.replace(self.filepath, new_filepath)


class VideoChecker(PatternMatchingEventHandler):

    def __init__(self, num_sources, queues, patterns="*.mp4"):
        patterns = [patterns] if not isinstance(patterns, list) else patterns
        PatternMatchingEventHandler.__init__(self, patterns=patterns)
        assert 0 < num_sources == len(queues)
        assert len(queues) == num_sources
        self.num_sources = num_sources
        self.queues = queues

    def on_created(self, event):
        print('<VideoChecker>.on_any_event()')
        if not event.is_directory:
            cs = VideoSource(event.src_path)
            print('<VideoChecker>.on_any_event() -> filename: ', cs.filename)
            if 0 <= cs.src_id < self.num_sources:
                if len(cs.filename.split('_')) >= 3:
                    self.queues[cs.src_id].put(cs)
                    debug_msg = "<VideoChecker:{}> Cam{} gets `{}`, qsize: {}" \
                        .format(os.getpid(), cs.src_id, cs.filepath, self.queues[cs.src_id].qsize())
                    logging.info(debug_msg)
            else:
                logging.error('<VideoChecker:{}> Unknown camera id #{}'.format(os.getpid(), str(cs.src_id)))


# class VideoDecoder(multiprocessing.Process):
class VideoDecoder(threading.Thread):
    def __init__(self, src_queue, dst_queue):
        # multiprocessing.Process.__init__(self)
        threading.Thread.__init__(self)
        self.src_queue = src_queue
        self.dst_queue = dst_queue
        # self.daemon = True
        self._exit = threading.Event()

    def decode(self, src):
        frame_count = 0
        cap = cv2.VideoCapture(src.filepath)
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                logging.error('<VideoDecoder:{}> [Cam{}] Cannot receive frame from:'
                              .format(os.getpid(), src.src_id, src.filepath))
                break
            frame_count += 1
            logging.info('<VideoDecoder:{}> [Cam{}] put frame #{} to frame_queue'
                         .format(os.getpid(), src.src_id, frame_count))
            self.dst_queue.put(frame)
        cap.release()
        return frame_count

    def run(self):
        frame_count = 0
        while not self._exit.isSet():
            if not self.src_queue.empty():
                src = self.src_queue.get()
                if os.path.isfile(src.filepath):
                    count = self.decode(src)
                    frame_count += count
                else:
                    logging.error('<VideoDecoder:{}> [Cam{}] Video not found: {}'
                                  .format(os.getpid(), src.src_id, src.src_path))
                    time.sleep(0.01)
            else:
                time.sleep(0.01)

    def terminate(self):
        self._exit.set()


class VideoEventWatcher(threading.Thread):

    def __init__(self, num_sources, watch_path, watch_extension, tile_size, frame_size):
        threading.Thread.__init__(self)
        self.num_sources = num_sources
        self.watch_path = watch_path
        self.watch_extension = watch_extension
        self.tile_size = tile_size
        self.frame_size = frame_size
        self.observer = Observer()
        self.file_queues = [Queue() for _ in range(num_sources)]
        self.frame_queues = [Queue() for _ in range(num_sources)]
        self.batch_queue = Queue()
        self.producer = VideoChecker(num_sources, self.file_queues, watch_extension)
        self.consumers = [VideoDecoder(q1, q2) for q1, q2 in zip(self.file_queues, self.frame_queues)]
        self._exit = threading.Event()

    def frames_filled(self):
        return all([not q.empty() for q in self.frame_queues])

    def frames_batched(self):
        return not self.batch_queue.empty()

    def batch_frames(self):
        return np.stack([q.get() for q in self.frame_queues])

    def read(self):
        return self.batch_queue.get()

    def resize(self, image, width=None, height=None, inter=cv2.INTER_AREA):
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

    def tile_frames(self, idx, frame):
        rows, cols = self.tile_size
        cell_w, cell_h = self.frame_size
        if cell_w != frame.shape[1]:
            frame = self.resize(frame, width=cell_w)
        tiled_image = np.zeros([cell_h * rows, cell_w * cols, 3], dtype=np.uint8)
        col_idx = idx % cols
        row_idx = math.floor(idx / cols)
        row_start, row_end = cell_h * row_idx, cell_h * (row_idx + 1)
        col_start, col_end = cell_w * col_idx, cell_w * (col_idx + 1)
        tiled_image[row_start:row_end, col_start:col_end] = frame
        return tiled_image

    def run(self):
        while not self._exit.isSet():
            if self.frame_queues is not None and self.frames_filled():
                frames = self.batch_frames()
                tiled_image = [self.tile_frames(i, f) for i, f in enumerate(frames)]
                tiled_image = np.sum(np.stack(tiled_image), axis=0).astype(np.uint8)
                # tiled_image = self.tile_frames(frames)
                src_qsizes = [q.qsize() for q in self.frame_queues]
                self.batch_queue.put(tiled_image)
                debug_msg = "<VideoEventWatcher:{}> src:{} -> dst:{} -> {}" \
                    .format(os.getpid(), src_qsizes, self.batch_queue.qsize(), tiled_image.shape)
                logging.info(debug_msg)

        self.observer.stop()
        self.observer.join()

    def start_watch(self):
        print('<VideoEventWatcher>.start_watch()')
        self.observer.schedule(self.producer, self.watch_path, recursive=True)
        self.observer.start()
        for c in self.consumers:
            time.sleep(0.01)
            c.start()
        time.sleep(0.01)
        self.start()

    def stop_watch(self):
        for c in self.consumers:
            c.terminate()
            time.sleep(0.1)

        for c in self.consumers:
            c.join()

        self._exit.set()