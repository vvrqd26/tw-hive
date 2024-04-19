from hive import Hive
from task import Task
from queue import Queue


class Core:
    def __init__(self, **kwargs):
        self.h = Hive()
        self.h.run()
        self.task_queue = Queue(kwargs.get('task_queue') or 100)
