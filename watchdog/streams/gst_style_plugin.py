"""
SEE how multiple inheritances work:
[1] https://stackoverflow.com/questions/9575409/calling-parent-class-init-with-multiple-inheritance-whats-the-right-way
[2] https://stackoverflow.com/questions/3810410/python-multiple-inheritance-from-different-paths-with-same-method-name

The entire architecture follows GStreamer concept to build pipeline.
Plugin consists of src pad and sink pad, which both are used to connect previous/next plugins.
It is similar to `linked list` structure.
Each plugin can optionally inherit `Worker` to work as a thread.
Each bin can combine multiple plugins while a pipeline can have multiple bins.

`init(cfg)`:
just pass `cfg` and it will initialize and then pass down to its sink/children

`init()`, `exec()`, `quit()` will perform all the way down to the end nodes.

Design patterns use:
[1] Abstract factory
[2] Chain of responsibility
[3] Composite
[4] Specification

"""
from __future__ import annotations

import threading
import time
import logging
from abc import ABC, abstractmethod
from enum import Enum
from queue import Queue
from threading import Thread
from typing import Optional, Tuple, TypeVar, Any, List
from inspect import stack

T = TypeVar("T")
logging.basicConfig(level=logging.INFO)


class Container(Enum):
    PLUGIN = 0
    BIN = 1
    PIPELINE = 2


class BasePlugin(ABC):

    def exec(self, timeout=0.01):
        pass

    def release(self):
        logging.info('<BasePlugin|{}>.release() - NO MORE'.format(self.__class__.__name__))

    def quit(self, timeout=0.01):
        pass


class Worker(BasePlugin, Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self._exit = threading.Event()
        self.queue = Queue()

    def buffer_empty(self) -> bool:
        return self.queue.empty()

    def buffer_qsize(self) -> int:
        return self.queue.qsize()

    def pop_buffer(self) -> Any:
        return self.queue.get()

    def put_buffer(self, request: Optional[T]) -> None:
        self.queue.put(request)
        # logging.warning('> <Worker|{}>.put_buffer() -> qsize: {}'.format(self.__class__.__name__, self.queue.qsize()))

    def preprocess(self) -> None:
        pass

    def run(self) -> None:
        self.preprocess()
        while self.running():
            self._handle()
        self.postprocess()
        self.release()

    def postprocess(self) -> None:
        pass

    @abstractmethod
    def _handle(self) -> None:
        raise NotImplementedError

    def exec(self, timeout=0.01) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        i = getattr(self, 'id', -1)
        logging.info('          <Worker({})[{}]|{}>.{}() - begins'.format(tid, i, cls_name, stack()[0][3]))
        logging.info('          <Worker({})[{}]|{}>.start()'.format(tid, i, cls_name))
        time.sleep(timeout)
        self.start()
        logging.info('          <Worker({})[{}]|{}>.{}() - ends'.format(tid, i, cls_name, stack()[0][3]))

    def running(self) -> bool:
        return not self._exit.is_set()

    def quit(self, timeout=0.01) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('          <Worker|{}>.{}() - begins'.format(tid, cls_name, stack()[0][3]))
        logging.info('          <Worker|{}>.exit.set()'.format(tid, cls_name))
        self._exit.set()
        logging.info('          <Worker|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))


class Plugin(BasePlugin):

    def __init__(self, src: Optional[T] = None):
        super().__init__()
        self.src = src
        self.sink = None
        if src is not None:
            self.src.sink = self

    def init(self, *args: str, **kwargs: int) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('        <Plugin({})|{}>.{}() - {}.init() begins - has sink? {}'.format(tid, cls_name, stack()[0][3], cls_name, str(self.has_sink())))
        if self.has_sink():
            sink_name = self.sink.__class__.__name__
            logging.info('        <Plugin({})|{}>.{}() - {}.init() begins'.format(tid, cls_name, stack()[0][3], sink_name))
            self.sink.init(*args, **kwargs)
            logging.info('        <Plugin({})|{}>.{}() - {}.init() ends'.format(tid, cls_name, stack()[0][3], sink_name))
        logging.info('        <Plugin({})|{}>.{}() - {}.init() ends'.format(tid, cls_name, stack()[0][3], cls_name))

    def whatami(self):
        return Container.PLUGIN

    def has_sink(self) -> bool:
        return self.sink is not None

    def read(self, *args: str, **kwargs: int) -> Optional[T]:
        pass

    def write(self, request: Optional[T]) -> None:
        pass

    def link(self, plugin: Plugin) -> Plugin:
        plugin.src = self
        self.sink = plugin
        return self.sink

    def unlink(self) -> None:
        self.sink.src = None
        self.sink = None

    def exec(self, timeout=0.01) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('        <Plugin({})|{}>.{}() - begins - has sink? {}'.format(tid, cls_name, stack()[0][3], str(self.has_sink())))
        super(Plugin, self).exec(timeout)
        if self.has_sink():
            sink_name = self.sink.__class__.__name__
            logging.info('        <Plugin({})|{}>.{}() - {}.exec() begins'.format(tid, cls_name, stack()[0][3], sink_name))
            time.sleep(timeout)
            self.sink.exec(timeout)
            logging.info('        <Plugin({})|{}>.{}() - {}.exec() ends'.format(tid, cls_name, stack()[0][3], sink_name))
        logging.info('        <Plugin({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))

    def quit(self, timeout=0.01):
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('        <Plugin({})|{}>.{}() - begins - has_sink? {}'.format(tid, cls_name, stack()[0][3], str(self.has_sink())))
        super(Plugin, self).quit(timeout)
        if self.has_sink():
            sink_name = self.sink.__class__.__name__
            logging.info('        <Plugin({})|{}>.{}() - {}.quit() begins'.format(tid, cls_name, stack()[0][3], sink_name))
            time.sleep(timeout)
            self.sink.quit(timeout)
            logging.info('        <Plugin({})|{}>.{}() - {}.quit() ends'.format(tid, cls_name, stack()[0][3], sink_name))
        logging.info('        <Plugin({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))


class Bin(Plugin):

    def __init__(self) -> None:
        super().__init__()
        self._plugins: List[Plugin] = []

    def whatami(self):
        return Container.BIN

    def init(self, *args: str, **kwargs: int) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('      <Bin({})|{}>.{}() - begins - has {} plugins'.format(tid, cls_name, stack()[0][3], len(self._plugins)))
        for i, p in enumerate(self._plugins):
            sink_name = p.__class__.__name__
            logging.info('      <Bin({})|{}>[{}].{}() - {}.init() begins'.format(tid, cls_name, i, stack()[0][3], sink_name))
            p.init(*args, **kwargs)
            logging.info('      <Bin({})|{}>[{}].{}() - {}.init() ends'.format(tid, cls_name, i, stack()[0][3], sink_name))
            logging.info('')
        logging.info('      <Bin({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))

    def add(self, plugin: Plugin, link=False) -> None:
        if link:
            plugin.src = self
        self._plugins.append(plugin)

    def remove(self, plugin: Plugin, recursive=False) -> None:
        self._plugins.remove(plugin)
        plugin.src = None
        if recursive:
            src = plugin
            while src is not None and src.has_sink():
                sink = src.sink
                src.src = None
                src.sink = None
                src = sink

    def exec(self, timeout=0.01) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('      <Bin({})|{}>.{}() - begins - has {} plugins'.format(tid, cls_name, stack()[0][3], len(self._plugins)))
        for i, p in enumerate(self._plugins):
            sink_name = p.__class__.__name__
            logging.info('      <Bin({})|{}>[{}].{}() - {}.exec() begins'.format(tid, cls_name, i, stack()[0][3], sink_name))
            time.sleep(timeout)
            p.exec(timeout)
            logging.info('      <Bin({})|{}>[{}].{}() - {}.exec() ends'.format(tid, cls_name, i, stack()[0][3], sink_name))
        super(Bin, self).exec(timeout)
        logging.info('      <Bin({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))

    def quit(self, timeout=0.01) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('      <Bin({})|{}>.{}() - begins - has {} plugins'.format(tid, cls_name, stack()[0][3], len(self._plugins)))
        for i, p in enumerate(self._plugins):
            sink_name = p.__class__.__name__
            logging.info('      <Bin({})|{}>[{}].{}() - {}.quit() begins'.format(tid, cls_name, i, stack()[0][3], sink_name))
            time.sleep(timeout)
            p.quit(timeout)
            logging.info('      <Bin({})|{}>[{}].{}() - {}.quit() ends'.format(tid, cls_name, i, stack()[0][3], sink_name))
        logging.info('====> <Bin({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))
        super(Bin, self).quit(timeout)
        logging.info('      <Bin({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))

    def size(self) -> int:
        return len(self._plugins)


class Pipeline(Bin):

    def __init__(self) -> None:
        super().__init__()
        self._bins: dict = {}

    def whatami(self):
        return Container.PIPELINE

    def init(self, *args: str, **kwargs: int) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('  <Pipeline({})|{}>.{}() - begins - has {} bins'.format(tid, cls_name, stack()[0][3], len(self._bins)))
        for i, b in enumerate(self._bins):
            sink_name = b.__class__.__name__
            logging.info('    [{}]<Pipeline({})|{}>.{}() - {}.init() begins'.format(i, tid, cls_name, stack()[0][3], sink_name))
            b.init(*args, **kwargs)
            logging.info('    [{}]<Pipeline({})|{}>.{}() - {}.init() end'.format(i, tid, cls_name, stack()[0][3], sink_name))
            logging.info('\n')
        logging.info('  <Pipeline({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))

    def exec(self, timeout=0.01) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('  <Pipeline({})|{}>.{}() - begins - has {} bins'.format(tid, cls_name, stack()[0][3], len(self._bins)))
        for i, b in enumerate(self._bins):
            sink_name = b.__class__.__name__
            logging.info('    [{}]<Pipeline({})|{}>.{}() - {}.exec() begins'.format(i, tid, cls_name, stack()[0][3], sink_name))
            time.sleep(timeout)
            b.exec(timeout)
            logging.info('    [{}]<Pipeline({})|{}>.{}() - {}.exec() end'.format(i, tid, cls_name, stack()[0][3], sink_name))
            logging.info('\n')
        super(Pipeline, self).exec(timeout)
        logging.info('  <Pipeline({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))

    def quit(self, timeout=0.01) -> None:
        tid = threading.get_ident()
        cls_name = self.__class__.__name__
        logging.info('  <Pipeline({})|{}>.{}() - begins - has {} bins'.format(tid, cls_name, stack()[0][3], len(self._bins)))
        for i, b in enumerate(self._bins):
            sink_name = b.__class__.__name__
            logging.info('    [{}]<Pipeline({})|{}>.{}() - {}.quit() begins'.format(i, tid, cls_name, stack()[0][3], sink_name))
            time.sleep(timeout)
            b.quit(timeout)
            logging.info('    [{}]<Pipeline({})|{}>.{}() - {}.quit() end'.format(i, tid, cls_name, stack()[0][3], sink_name))
        super(Pipeline, self).quit(timeout)
        logging.info('  <Pipeline({})|{}>.{}() - ends'.format(tid, cls_name, stack()[0][3]))