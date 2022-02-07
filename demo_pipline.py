#!/usr/bin/env python3

from pathlib import Path
import argparse
import logging
import json
import cv2

import time

from watchdog.streams.streams_plugin import StreamPipeline
from watchdog.utils.decoder import ConfigDecoder, Profiler


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-c', '--config', metavar="FILE",
                        default=Path(__file__).parent / 'cfg_bak.json',
                        help='path to configuration JSON file')
    parser.add_argument('-l', '--log', metavar="FILE",
                        help='output a MOT Challenge format log (e.g. eval/results/mot17-04.txt)')
    args = parser.parse_args()

    # set up logging
    logging.basicConfig(format='%(asctime)s [%(levelname)8s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(StreamPipeline.__name__)
    logger.setLevel(logging.DEBUG)

    # load config file
    with open(args.config) as cfg_file:
        config = json.load(cfg_file, cls=ConfigDecoder)

    log = None
    pipeline = StreamPipeline()
    pipeline.init(config)
    cv2.namedWindow("Video", cv2.WINDOW_AUTOSIZE)

    logger.info('Starting video capture ...')
    pipeline.exec()
    try:
        with Profiler('app') as prof:
            start_time = time.time()
            dt_avg = 0
            while cv2.getWindowProperty("Video", 0) >= 0:
                if not pipeline.empty():
                    tiled_image = pipeline.read()
                    dt = time.time() - start_time
                    start_time = time.time()
                    dt_avg = .9 * dt_avg + .1 * dt
                    fps = 1 / dt_avg
                    cv2.putText(tiled_image, str(round(fps, 1)) + ' fps', (0, 100),
                                cv2.FONT_HERSHEY_SIMPLEX, .75, (255, 255, 255), 2)

                    cv2.imshow('Video', tiled_image)
                    if cv2.waitKey(1) & 0xFF == 27:
                        break
    finally:
        if log is not None:
            log.close()
        pipeline.quit()
        cv2.destroyAllWindows()


if __name__ == '__main__':
    main()
