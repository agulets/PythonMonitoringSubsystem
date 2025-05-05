import time
import logging
import multiprocessing
from socket import gethostname
from threading import Thread, Event

from MonitoringSubsystem import InfluxSender
from MonitoringSubsystem.Commons import get_logger_by_params_and_make_log_folder
from MonitoringSubsystem.InfluxSender import InfluxSender, get_influx_points_by_data_collector_point
from MonitoringSubsystem.JQueue import JQueue
from MonitoringSubsystem.MonitoringDataClasses import (
    MONITORING_SYSTEM_POINT,
    MONITORING_PROCESS_POINT,
    QUEUE_STATE_MONITORING_POINT,
    REQUEST_MONITORING_POINT,
    SYSTEM_ERROR_MONITORING_POINT,
    MONITORED_QUEUE,
    TAG,
)
from MonitoringSubsystem.SystemMonitoring import SystemMonitoring
from MonitoringSubsystem.ProcessMonitoring import ProcessMonitoring


class DataCollector:
    def __init__(self, data_collector_queue: JQueue = None, max_queue_size: int = 10000, logger: logging.Logger = None):
        self.data_collector_queue = data_collector_queue if data_collector_queue else JQueue()
        self.max_queue_size = max_queue_size
        self.logger = logger if logger else get_logger_by_params_and_make_log_folder()

        self.system_monitoring_enabled = Event()
        self.process_monitoring_enabled = Event()
        self.queue_monitoring_enabled = Event()
        self.system_monitoring_process = None
        self.processing_process = None

        # Start processing if queue was created internally
        if data_collector_queue is None:
            self.start_processing_process()

    def get_data_collector_queue(self) -> JQueue:
        return self.data_collector_queue

    # Process loop for host monitoring, self-process with data_collector_queue
    @staticmethod
    def _system_monitoring_loop(data_collector_queue, scrape_interval, logger=None):
        system_monitor = SystemMonitoring(logger=logger)
        process_monitor = ProcessMonitoring(process_name="self_monitoring_process", logger=logger)
        while True:
            with system_monitor:
                system_metric_point = system_monitor.get_system_monitoring_point()
                data_collector_queue.put(system_metric_point)

                self_process_metric_point = process_monitor.get_process_monitoring_point()
                data_collector_queue.put(self_process_metric_point)

                self_queue_metric = QUEUE_STATE_MONITORING_POINT(
                    host_name=gethostname(),
                    name="data_collector_queue",
                    time_stamp=time.time_ns(),
                    size=data_collector_queue.qsize(),
                    tags=[])
                data_collector_queue.put(self_queue_metric)
                time.sleep(scrape_interval)

    def start_system_monitoring_process(self, scrape_interval: int = 3):
        self.system_monitoring_process = multiprocessing.Process(
            name="host_monitoring_process",
            target=self._system_monitoring_loop,
            args=(self.data_collector_queue, scrape_interval, self.logger),
            daemon=True
        )
        self.system_monitoring_process.start()

    # Thread loop for any process monitoring
    def _process_monitoring_loop(self, process_name: str = None, scrape_interval: int = 3):
        process_monitor = ProcessMonitoring(process_name=process_name)
        while self.process_monitoring_enabled.is_set():
            process_point = process_monitor.get_process_monitoring_point()
            self.data_collector_queue.put(process_point)
            time.sleep(scrape_interval)

    def start_thread_for_monitoring_process(self, process_name: str = None, scrape_interval=3):
        self.process_monitoring_enabled.set()
        Thread(target=self._process_monitoring_loop, args=(process_name, scrape_interval,), daemon=True).start()

    # Thread loop for any queue monitoring
    def _queue_monitoring_loop(self, mon_queue: MONITORED_QUEUE, scrape_interval: int = 3):
        while self.queue_monitoring_enabled.is_set():
            point = QUEUE_STATE_MONITORING_POINT(
                host_name=gethostname(),
                name=mon_queue.name, time_stamp=time.time_ns(), size=mon_queue.queue.qsize(), tags=mon_queue.tags)
            self.add_metric_point_to_data_collector_queue(point)
            time.sleep(scrape_interval)

    # Creating thread for monitoring get queue
    def add_queue_to_monitoring(self, queue: JQueue, name: str, tags: list[TAG] = None, scrape_interval: int = 3):
        self.queue_monitoring_enabled.set()
        tags = tags if tags else []
        mon_queue = MONITORED_QUEUE(queue=queue, name=name, tags=tags)
        queue_monitoring_thread = Thread(target=self._queue_monitoring_loop, args=(mon_queue, scrape_interval,), daemon=True)
        queue_monitoring_thread.start()

    def add_metric_point_to_data_collector_queue(self, metric_point: [MONITORING_SYSTEM_POINT,
                                                                      MONITORING_PROCESS_POINT,
                                                                      QUEUE_STATE_MONITORING_POINT,
                                                                      REQUEST_MONITORING_POINT,
                                                                      SYSTEM_ERROR_MONITORING_POINT]):
        self.data_collector_queue.put(metric_point)

    @staticmethod
    def _processing_loop(metrics_queue: JQueue, max_size: int):
        # Self-monitoring setup
        # process_monitor = ProcessMonitoring()
        while True:
            # Check queue size
            if metrics_queue.qsize() > max_size:
                print(" -- ! --")
                # Handle overflow (e.g., log warning, drop old metrics)
                pass

            # Process metrics
            while not metrics_queue.is_empty():
                item = metrics_queue.get()
                print(get_influx_points_by_data_collector_point(data_collector_point=item))

            # Collect self-metrics
            # self_metrics = process_monitor.get_process_monitoring_point()
            # metrics_queue.put(self_metrics)
            time.sleep(1)

    def start_processing_process(self):
        self.processing_process = multiprocessing.Process(
            target=self._processing_loop,
            args=(self.data_collector_queue, self.max_queue_size),
            daemon=True
        )
        self.processing_process.start()

    def shutdown(self):
        self.system_monitoring_enabled.clear()
        self.process_monitoring_enabled.clear()
        self.queue_monitoring_enabled.clear()
        if self.processing_process:
            self.processing_process.terminate()
            self.processing_process.join()


def influx_sender_thread(influx_host, influx_port, influx_user_name, influx_user_pass, influx_db_name,
                         data_collector_queue: JQueue,
                         _logger: logging.Logger,
                         chunk_size=1000):

    _logger.info(f"Thread 'influx_sender_thread' started")
    influx_db = InfluxSender(host=influx_host, port=influx_port, user_name=influx_user_name,
                             user_pass=influx_user_pass, db_name=influx_db_name,
                             raise_exceptions=False, logger=_logger)
    influx_db.check_db_existing()

    while True:
        try:
            points_for_send_pack = []
            for element in range(data_collector_queue.qsize()):
                if not data_collector_queue.empty():
                    points_for_send_pack.append(data_collector_queue.get())
            influx_db.insert_points_to_db(points=points_for_send_pack, chunk_size=chunk_size)

        except Exception as influx_sender_thread_exception:
            _logger.exception(f"Exception in influx_sender_thread: {influx_sender_thread_exception}")
            tags = [TAG(name='process_name', value=multiprocessing.current_process().name),
                    TAG(name='action_type', value='influx_sender_thread')]
            system_error_point = SYSTEM_ERROR_MONITORING_POINT(name='system_error',
                                                               host_name=gethostname(),
                                                               time_stamp=time.time_ns(),
                                                               err_code=1,
                                                               tags=tags)
            data_collector_queue.put(system_error_point)
        time.sleep(1)
