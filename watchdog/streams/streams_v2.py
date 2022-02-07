import os
import cv2
import logging
import math
import time
import threading
import numpy as np
from queue import Queue
from datetime import datetime as dt
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

logging.basicConfig(level=logging.INFO)


class VideoSource:

    def __init__(self, filepath):
        src_id, vid, msec, src_dir, filename = VideoSource.parse(filepath)
        self.src_id = src_id
        self.vid = vid
        self.src_dir = src_dir
        self.src_name = filename
        self.src_path = filepath
        self.msec = msec

    @staticmethod
    def parse(path):
        """
            Example:
                /path/to/clips/cam01/1632983068.195901_001.avi
                ...
                /path/to/clips/cam01/1632983586.173297_999.avi
        """
        print('> path: ', path)
        dirname = os.path.dirname(path)
        filename = os.path.basename(path)
        splits = os.path.splitext(filename)
        src_id = int(dirname[-1])
        vid = int(splits[0][-3:])
        msec = float(splits[0][:-4])
        return src_id, vid, msec, dirname, filename

    def to_datetime(self, format='%Y-%m-%d %H:%M:%S.%f'):
        assert self.msec != 0.
        return dt.fromtimestamp(self.msec).strftime(format)


class VideoChecker(PatternMatchingEventHandler):

    def __init__(self, num_sources, queues, patterns="*.mp4"):
        patterns = [patterns] if not isinstance(patterns, list) else patterns
        PatternMatchingEventHandler.__init__(self, patterns=patterns)
        assert 0 < num_sources == len(queues)
        assert len(queues) == num_sources
        self.num_sources = num_sources
        self.queues = queues

    def on_closed(self, event):
        print('<VideoChecker>.on_closed()')
        if not event.is_directory:
            cs = VideoSource(event.src_path)
            if 0 <= cs.src_id < self.num_sources:
                self.queues[cs.src_id].put(cs)
                debug_msg = "<VideoChecker:{}> Cam{} gets `{}`, qsize: {}" \
                    .format(os.getpid(), cs.src_id, cs.src_path, self.queues[cs.src_id].qsize())
                logging.info(debug_msg)
            else:
                logging.error('<VideoChecker:{}> Unknown camera id #{}'.format(os.getpid(), str(cs.src_id)))


# class VideoDecoder(multiprocessing.Process):
class VideoDecoder(threading.Thread):
    def __init__(self, src_id, src_queue, dst_queue):
        # multiprocessing.Process.__init__(self)
        threading.Thread.__init__(self)
        self.src_id = src_id
        self.src_queue = src_queue
        self.dst_queue = dst_queue
        # self.daemon = True
        self._exit = threading.Event()

    def decode(self, src):
        frame_count = 0
        cap = cv2.VideoCapture(src.src_path)
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                logging.error('<VideoDecoder:{}> [Cam{}/{}] Cannot receive frame from:'
                              .format(os.getpid(), self.src_id, src.src_id, src.src_path))
                break
            frame_count += 1
            logging.info('<VideoDecoder:{}> [Cam{}/{}] put frame #{} to frame_queue'
                         .format(os.getpid(), self.src_id, src.src_id, frame_count))
            self.dst_queue.put(frame)
        cap.release()
        return frame_count

    def run(self):
        frame_count = 0
        while not self._exit.isSet():
            if not self.src_queue.empty():
                src = self.src_queue.get()
                if os.path.isfile(src.src_path):
                    count = self.decode(src)
                    frame_count += count
                else:
                    logging.error('<VideoDecoder:{}> [Cam{}/{}] Video not found: {}'
                                  .format(os.getpid(), self.src_id, src.src_id, src.src_path))

    def terminate(self):
        self._exit.set()


# TODO: move `VideoDecoder` to `StreamIO`
class VideoEventWatcher(threading.Thread):

    def __init__(self, config, num_sources, watch_path, watch_extension, tile_size, frame_size):
        threading.Thread.__init__(self)
        self.enable_osd = config['enable_osd']
        self.save_clips = config['save_clips']
        self.num_sources = num_sources
        self.watch_path = watch_path
        self.watch_extension = watch_extension
        self.tile_size = tile_size
        self.frame_size = frame_size
        self.observer = Observer()
        self.file_queues = [Queue() for _ in range(num_sources)]
        self.frame_queues = [Queue() for _ in range(num_sources)]
        self.batch_queue = Queue()
        self.producer = VideoChecker(num_sources, self.file_queues, config['watch_extension'])
        self.consumers = [VideoDecoder(i, q1, q2) for i, (q1, q2) in enumerate(zip(self.file_queues, self.frame_queues))]
        self._exit = threading.Event()

    def is_osd_enabled(self):
        return self.enable_osd

    def frames_filled(self):
        return all([not q.empty() for q in self.frame_queues])

    def frames_batched(self):
        return not self.batch_queue.empty()

    def batch_frames(self):
        return np.stack([q.get() for q in self.frame_queues])

    def read(self):
        return self.batch_queue.get()

    def tile_frames(self, frames):
        rows, cols = self.tile_size
        cell_w, cell_h = self.frame_size
        tiled_image = np.zeros([cell_h * rows, cell_w * cols, 3], np.uint8)
        row_idx = 0
        for i, f in enumerate(frames):
            col_idx = i % cols
            row_idx += 1 if i > 0 and i % cols == 0 else 0
            # row_idx = math.floor(i / cols)
            row_start, row_end = cell_h * row_idx, cell_h * (row_idx + 1)
            col_start, col_end = cell_w * col_idx, cell_w * (col_idx + 1)
            tiled_image[row_start:row_end, col_start:col_end] = f
        return tiled_image

    def run(self):
        while not self._exit.isSet():
            if self.frame_queues is not None and self.frames_filled():
                frames = self.batch_frames()
                tiled_image = self.tile_frames(frames)
                self.batch_queue.put(tiled_image)
                debug_msg = "<BatchFramesWorker:{}> qsize:{} → put → {}" \
                    .format(os.getpid(), self.batch_queue.qsize(), tiled_image.shape)
                logging.info(debug_msg)

        self.observer.stop()
        self.observer.join()
        logging.info('> VideoEventWatcher() -> END of run()')

    def start_watch(self):
        print('<FileEventWatcher>.start_watch()')
        self.observer.schedule(self.producer, self.watch_path, recursive=True)
        self.observer.start()
        for c in self.consumers:
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
