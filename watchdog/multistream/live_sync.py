import os
import cv2
import time
import math
import imutils
import psutil
import argparse
import threading
import numpy as np
from queue import Queue
from mmcv import Config
from concurrent import futures


class CamSource(threading.Thread):
    def __init__(self, uri, callback):
        threading.Thread.__init__(self)
        self.uri = uri
        self.cap = cv2.VideoCapture(uri)
        self.fps = int(self.cap.get(cv2.CAP_PROP_FPS))
        self.size = (int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
                     int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))
        self.base_timestamp = self.cap.get(cv2.CAP_PROP_POS_MSEC)

        self.queue = Queue(maxsize=1)
        self.sync_fn = callback
        self.exit_event = threading.Event()

    def run(self):
        while not self.exit_event.is_set():
            ret, frame = self.cap.read()
            if ret:
                latest = self.sync_fn()
                if latest and self.queue.qsize() > latest:
                    for _ in range(self.queue.qsize() - latest):
                        self.queue.get()
                latest = self.sync_fn()

                if self.queue.qsize() == latest:
                    self.queue.put(frame)

    def close(self):
        self.exit_event.set()
        self.cap.release()


class MyVideoIO:

    def __init__(self, input_uri):
        self.input_uri = input_uri
        self.num_sources = len(input_uri)
        self.cell_size = (640, 360)
        self.tile_rowcol = self.calc_tile_size()
        cell_w, cell_h = self.cell_size
        rows, cols = self.tile_rowcol
        self.tile_size = (cell_w * cols, cell_h * rows)
        self.sources = [CamSource(input_uri[i], self.sync) for i in range(self.num_sources)]

    def start_capture(self):
        for c in self.sources:
            c.start()
            time.sleep(1)  # > momentary pause for stable connection

    def sync(self):
        return min([k.queue.qsize() for k in self.sources])

    def fill_frame(self, frame, idx):
        rows, cols = self.tile_rowcol
        cell_w, cell_h = self.cell_size
        frame = imutils.resize(frame, width=cell_w)
        tiled_image = np.zeros([cell_h * rows, cell_w * cols, 3], np.uint8)
        col_idx = idx % cols
        row_idx = math.floor(idx / cols)
        row_start, row_end = cell_h * row_idx, cell_h * (row_idx + 1)
        col_start, col_end = cell_w * col_idx, cell_w * (col_idx + 1)
        tiled_image[row_start:row_end, col_start:col_end] = frame
        return tiled_image

    def batch(self):
        # Create four thread pools and assign values​to the corresponding positions of the placeholder images
        nproc = psutil.cpu_count()
        with futures.ThreadPoolExecutor(nproc) as executor:
            frames = executor.map(
                self.fill_frame,
                [k.queue.get() for k in self.sources],
                list(range(self.num_sources)),
            )
            return np.sum(frames, axis=0)

    def calc_tile_size(self):
        tile_rows = round(math.sqrt(self.num_sources))
        tile_cols = math.ceil(self.num_sources / tile_rows)
        tile_size = (tile_rows, tile_cols)
        return tile_size

    def close(self):
        for k in self.sources:
            k.close()


# > https://www.programmersought.com/article/37436037769/

def parse_args():
    parser = argparse.ArgumentParser(description='Train a detector')
    parser.add_argument('config', default='configs/default.py', help='train config file path')
    parser.add_argument('--uri', help='the dir to save logs and models')
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    cfg = Config.fromfile(args.config)
    if args.uri is not None:
        cfg.uri = args.uri
        if not isinstance(cfg.uri, (tuple, list)):
            cfg.uri = [cfg.uri]

    start_time = time.time()
    dt_avg = 0
    ipcam_reader = MyVideoIO(cfg.uri)
    ipcam_reader.start_capture()
    fourcc = cv2.VideoWriter_fourcc(*'XVID')
    writer = cv2.VideoWriter('./output.avi', fourcc, 15, ipcam_reader.tile_size, True)
    print('> tiled size: ', ipcam_reader.tile_size)
    print('> fps: ', ipcam_reader.sources[0].fps)
    print('> resolution: ', ipcam_reader.sources[0].size)
    print('> output: ', os.getcwd() + "/output.avi")
    while True:
        queues_filled = ipcam_reader.sync()
        if queues_filled:
            tiled_image = ipcam_reader.batch()

            dt = time.time() - start_time
            start_time = time.time()
            dt_avg = .9 * dt_avg + .1 * dt
            fps = 1 / dt_avg
            cv2.putText(tiled_image, str(round(fps, 1)) + ' fps', (0, 100),
                        cv2.FONT_HERSHEY_SIMPLEX, .75, (255, 255, 255), 2)
            cv2.imshow("frame", tiled_image)
            print('tiled_image.shape=', tiled_image.shape)
            writer.write(tiled_image)
            if cv2.waitKey(1) & 0xff == ord('q'):
                break
        # Release the camera handle and window
    ipcam_reader.close()
    writer.release()
    cv2.destroyAllWindows()


if __name__ == '__main__':
    main()
