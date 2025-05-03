import datetime
from JQueue import JQueue
from collections import namedtuple
from MonitoringDataClasses import QUEUE_STATE_MONITORING_POINT, TAG

QU = namedtuple('QU', ['name', 'queue'])


class QueueMonitoring:

    def __init__(self, tags:[TAG] = None):
        self.queues:[QU] = []
        self.tags = tags

    def append_queue_into_monitoring(self, _queue:JQueue, _name):
        self.queues.append(QU(name=_name, queue=_queue))

    def _get_monitoring_points(self):
        _mon_points = []
        for _queue in self.queues:
            qu_size = _queue.queue.qsize()
            time_stamp = datetime.datetime.utcnow().isoformat(timespec='milliseconds')
            name = _queue.name
            mon_point = QUEUE_STATE_MONITORING_POINT(name=name,
                                                     time_stamp=time_stamp,
                                                     size=qu_size,
                                                     tags=self.tags)
            _mon_points.append(mon_point)


