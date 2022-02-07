import json
import time
from collections import Counter


class ConfigDecoder(json.JSONDecoder):
    def __init__(self, **kwargs):
        json.JSONDecoder.__init__(self, **kwargs)
        # Use the custom JSONArray
        self.parse_array = self.JSONArray
        # Use the python implemenation of the scanner
        self.scan_once = json.scanner.py_make_scanner(self)

    def JSONArray(self, s_and_end, scan_once, **kwargs):
        values, end = json.decoder.JSONArray(s_and_end, scan_once, **kwargs)
        return tuple(values), end


class Profiler:
    __call_count = Counter()
    __time_elapsed = Counter()

    def __init__(self, name, aggregate=False):
        self.name = name
        if not aggregate:
            Profiler.__call_count[self.name] += 1

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, type, value, traceback):
        self.end = time.perf_counter()
        self.duration = self.end - self.start
        Profiler.__time_elapsed[self.name] += self.duration

    @classmethod
    def reset(cls):
        cls.__call_count.clear()
        cls.__time_elapsed.clear()

    @classmethod
    def get_avg_millis(cls, name):
        call_count = cls.__call_count[name]
        if call_count == 0:
            return 0.
        return cls.__time_elapsed[name] * 1000 / call_count
