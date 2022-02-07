import os
import cv2
import glob
import argparse
from datetime import datetime as dt
from mmcv import Config, DictAction


class VideoClipper:
    def __init__(self, sec_duration, root, src_id, frame_size, fps, scan_first=True):
        assert sec_duration > 0
        self.clip_id = -1
        self.sec_duration = sec_duration
        self.root_dir = root
        self.save_dir = None
        self.tmp_dir = None
        self.src_id = src_id
        self.frame_id = 0
        self.frame_size = frame_size
        self.fps = fps
        self.clips = []
        self.max_frames = sec_duration * fps
        self.fourcc = cv2.VideoWriter_fourcc(*'XVID')
        # > preprocess
        self.make_dirs()
        if scan_first:
            self.scan()
        self.make_clip()

    def _sort_by_clip_id(self, x):
        return x[-7:]  # e.g. cam01

    def scan(self, extension='*.avi'):
        """
            grab all existing videos and sort them by `clip_id`
        """
        clips = glob.glob(self.tmp_dir + "/" + extension, recursive=True)
        clips.sort(key=self._sort_by_clip_id)
        # for c in clips:
        #     self.clips.append(c)
        #     self.clip_id += 1
        self.clip_id += len(clips)
        print()

    def write(self, frame, frame_id):
        self.frame_id += 1
        # do_save = (frame_id % self.max_frames) == 0
        self.writer.write(frame)
        if self.writer and self.frame_id >= self.max_frames:  # > save a clip every `N` frames.
            # print('> write() => do_save = ', str(do_save), ', frame_id = ', frame_id, ', frame_cnt = ', self.frame_id)
            # print('')
            self.frame_id = 0
            self.writer.release()
            self.make_clip()

    def make_dirs(self):
        cam_id = '{:>02d}'.format(self.src_id)
        self.time_base = dt.now().timestamp()
        self.save_dir = "{root}/save/{date_dirs}/cam{cam_id}" \
            .format(root=self.root_dir,
                    date_dirs=dt.fromtimestamp(self.time_base).strftime("%Y/%m/%d"),
                    cam_id=cam_id)
        self.tmp_dir = "{root}/tmp/cam{cam_id}" \
            .format(root=self.root_dir, cam_id=cam_id)
        try:
            os.makedirs(self.save_dir)
            os.makedirs(self.tmp_dir)
        except FileExistsError:
            print("> Directory already exists")

    def make_clip(self):
        self.clip_id += 1
        self.time_base = dt.now().timestamp()
        self.filename = '{dirs}/{time_base}_{clip_id}.avi' \
            .format(dirs=self.tmp_dir,
                    time_base='{:<17f}'.format(self.time_base),
                    clip_id='{:>03d}'.format(self.clip_id))
        self.writer = cv2.VideoWriter(self.filename, self.fourcc, self.fps, self.frame_size)

    def move(self, src_file, dst_dir):
        filename = os.path.basename(src_file)
        dst_file = dst_dir + "/" + filename
        os.replace(src_file, dst_file)

    def remove(self, file):
        os.remove(file)

    @staticmethod
    def to_datetime(clip_name, format='%Y-%m-%d %H:%M:%S.%f'):
        assert len(clip_name) > 8
        msec = float(clip_name[:-8])
        return dt.fromtimestamp(msec).strftime(format)

    def release(self):
        if self.writer:
            self.frame_id = 0
            self.writer.release()


def parse_args():
    parser = argparse.ArgumentParser(description='Train a detector')
    parser.add_argument('config', default='configs/default.py', help='train config file path')
    parser.add_argument('--uri', help='the dir to save logs and models')
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    cfg = Config.fromfile(args.config)
    output_path = cfg.output_path
    cap = cv2.VideoCapture(cfg.uri)
    fps = int(cap.get(cv2.CAP_PROP_FPS))
    size = (int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)), int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))
    msec = cap.get(cv2.CAP_PROP_POS_MSEC)
    vid_clipper = VideoClipper(10, output_path, 1, size, fps, scan_first=True)
    print(fps, size)

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break

        msec = cap.get(cv2.CAP_PROP_POS_MSEC)
        frame_id = cap.get(cv2.CAP_PROP_POS_FRAMES)
        print('> frm.id = ', str(frame_id), ', msec = ', str(msec))
        cv2.imshow('IP camera', frame)
        vid_clipper.write(frame, frame_id)
        if cv2.waitKey(1) & 0xff == ord('q'):
            break

    cap.release()
    vid_clipper.release()
    cv2.destroyAllWindows()


if __name__ == '__main__':
    main()