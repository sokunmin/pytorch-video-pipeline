import cv2
import math
import logging
import argparse
from mmcv import Config

from watchdog.streams.streams import VideoEventWatcher

logging.basicConfig(level=logging.INFO)


def sort_by_last_number(x):
    # filename format: c1_001.mp4 -> 001.mp4
    return x[-7:]


def calc_tile_size(num_sources):
    tile_rows = round(math.sqrt(num_sources))
    tile_cols = math.ceil(num_sources / tile_rows)
    tile_size = (tile_rows, tile_cols)
    return tile_size


def parse_args():
    parser = argparse.ArgumentParser(description='Train a detector')
    parser.add_argument('config', default='configs/default.py', help='train config file path')
    parser.add_argument('--uri', help='the dir to save logs and models')
    args = parser.parse_args()
    return args


def main():
    # > https://www.programmersought.com/article/37436037769/
    args = parse_args()
    cfg = Config.fromfile(args.config)
    if args.uri is not None:
        cfg.uri = args.uri
        if not isinstance(cfg.uri, (tuple, list)):
            cfg.uri = [cfg.uri]

    tile_size = calc_tile_size(cfg.num_streams)
    # scan existed temp videos
    print('> #cams = ', cfg.num_streams)
    print('> watch_path = ', cfg.watchdog.dir)
    watcher = VideoEventWatcher(cfg.num_streams,
                                cfg.watchdog.dir,
                                cfg.watchdog.extension,
                                tile_size,
                                cfg.cell_size)
    watcher.start_watch()

    frame_count = 0
    while True:
        # > [1] threadpool
        # tiled_image = concurrent_batch_frames(NUM_CAMERAS, watcher.frame_queues, cell_size, tile_size)
        if watcher.frames_batched():
            print('> <MainThread> batch.qsize() = ', watcher.batch_queue.qsize())
            tiled_image = watcher.read()
            frame_count += 1
            print('> <MainThread> frame #', frame_count, ', shape=', tiled_image.shape)
            cv2.imshow("Tiled images", tiled_image)
        if cv2.waitKey(1) & 0xff == ord('q'):
            break
    cv2.destroyAllWindows()

    # > NOTE: test watcher only
    watcher.stop_watch()


if __name__ == '__main__':
    main()
