import time
import logging
import threading
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
from MonitoringSubsystem.ProcessMonitoring import ProcessMonitoring, get_current_process_id, get_current_process_name


class DataCollector:
    def __init__(self, data_collector_queue: JQueue = None, max_queue_size: int = 10000, default_scrape_interval:int = 3,
                 log_dir=None, log_size=104857608, log_file_count=2, log_level=30, log_formatter=None, influx_sender_enable = False,
                 influx_host='localhost', influx_port=8086, influx_user_name='root', influx_user_pass='root', influx_db_name=None,
                 influx_token=None, influx_org='-', influx_bucket=None,
                 influx_use_udp=False, influx_udp_port=4444, influx_proxies=None, influx_timeout=30, influx_retries=3, influx_pool_size=10,
                 influx_chunk_size=1000, influx_use_ssl=False, verify_ssl=False, influx_cert_path=None, influx_use_gzip=False,
                 influx_session=None, influx_headers=None, influx_url_postfix='', influx_raise_exceptions=True,

                 victoria_sender_enable=False):
        self.log_params = {
            'log_dir': log_dir,
            'log_size': log_size,
            'log_file_count': log_file_count,
            'log_level': log_level,
            'formatter': log_formatter
        }
        self.logger = get_logger_by_params_and_make_log_folder(log_name='main_data_collector', **self.log_params)

        self.influx_sender_config = {
            'enable': influx_sender_enable,
            'host': influx_host,
            'port': influx_port,
            'user_name': influx_user_name,
            'user_pass': influx_user_pass,
            'token': influx_token,
            'org': influx_org,
            'bucket': influx_bucket,
            'db_name': influx_db_name,
            'use_udp': influx_use_udp,
            'udp_port': influx_udp_port,
            'proxies': influx_proxies,
            'timeout': influx_timeout,
            'retries': influx_retries,
            'pool_size': influx_pool_size,
            'chunk_size': influx_chunk_size,
            'use_ssl': influx_use_ssl,
            'verify_ssl': verify_ssl,
            'cert_path': influx_cert_path,
            'use_gzip': influx_use_gzip,
            'session': influx_session,
            'headers': influx_headers,
            'url_postfix': influx_url_postfix,
            'raise_exceptions': influx_raise_exceptions,
        }
        self.victoria_sender_config = {
            'enable': victoria_sender_enable,
        }

        self.data_collector_queue = data_collector_queue or JQueue()
        self.max_queue_size = max_queue_size
        self.default_scrape_interval = default_scrape_interval
        self.system_monitoring_enabled = Event()
        self.process_monitoring_enabled = Event()
        self.queue_monitoring_enabled = Event()
        self.system_monitoring_process = None
        self.processing_process = None

        if data_collector_queue is None:
            # Start process for metrics processing if queue was created internally
            self.start_processing_process()

            # Add self data_collector_queue for monitoring if queue was created internally
            self.add_queue_to_monitoring(queue=self.data_collector_queue,
                                         name="data_collector_queue",
                                         scrape_interval=self.default_scrape_interval)

    def get_data_collector_queue(self) -> JQueue:
        return self.data_collector_queue

    # Process loop for host monitoring, self-process with data_collector_queue
    @staticmethod
    def _host_monitoring_loop(data_collector_queue, scrape_interval, log_params: dict):
        logger = get_logger_by_params_and_make_log_folder( log_name="host_monitoring", **log_params)
        host_monitor = SystemMonitoring(logger=logger)
        process_monitor = ProcessMonitoring(process_name="self_monitoring_process", logger=logger)
        with host_monitor:
            while True:
                host_metric_point = host_monitor.get_system_monitoring_point()
                data_collector_queue.put(host_metric_point)

                self_process_metric_point = process_monitor.get_process_monitoring_point()
                data_collector_queue.put(self_process_metric_point)

                time.sleep(scrape_interval)

    def start_system_monitoring_process(self, scrape_interval: int = None):
        scrape_interval = scrape_interval or self.default_scrape_interval
        self.system_monitoring_process = multiprocessing.Process(
            name="host_monitoring_process",
            target=self._host_monitoring_loop,
            args=(self.data_collector_queue, scrape_interval,  self.log_params),
            daemon=True
        )
        self.system_monitoring_process.start()
        self.logger.info(f"Start new process:'{self.system_monitoring_process.name}' "
                         f"with pid:'{self.system_monitoring_process.pid}' for monitoring host.")

    # Thread loop for any process monitoring
    def _process_monitoring_loop(self, process_name: str = None, scrape_interval: int = None):
        scrape_interval = scrape_interval or self.default_scrape_interval
        logger = get_logger_by_params_and_make_log_folder( log_name=f"process_monitoring_{process_name}", **self.log_params)
        process_monitor = ProcessMonitoring(process_name=process_name, logger=logger)
        while self.process_monitoring_enabled.is_set():
            process_point = process_monitor.get_process_monitoring_point()
            self.data_collector_queue.put(process_point)
            time.sleep(scrape_interval)

    def start_thread_for_monitoring_process(self, process_name: str = None, scrape_interval: int = None):
        scrape_interval = scrape_interval or self.default_scrape_interval
        self.process_monitoring_enabled.set()
        monitoring_process_thread = Thread(target=self._process_monitoring_loop, args=(process_name, scrape_interval), daemon=True)
        monitoring_process_thread.start()
        self.logger.info(f"Start new thread:{monitoring_process_thread.name} for monitoring process:'{process_name}'")

    # Thread loop for any queue monitoring
    def _queue_monitoring_loop(self, mon_queue: MONITORED_QUEUE, scrape_interval: int = None):
        scrape_interval = scrape_interval or self.default_scrape_interval
        logger = get_logger_by_params_and_make_log_folder(log_name=f"queue_monitoring_{mon_queue.name}", **self.log_params)
        while self.queue_monitoring_enabled.is_set():
            point = QUEUE_STATE_MONITORING_POINT(
                host_name=gethostname(),
                name=mon_queue.name,
                time_stamp=time.time_ns(),
                size=mon_queue.queue.qsize(),
                tags=mon_queue.tags)
            logger.debug(f"Get Queue monitoring point:'{point}'")
            self.add_metric_point_to_data_collector_queue(point)
            time.sleep(scrape_interval)

    # Creating thread for monitoring get queue
    def add_queue_to_monitoring(self, queue: JQueue, name: str, tags: list[TAG] = None, scrape_interval: int = None):
        scrape_interval = scrape_interval or self.default_scrape_interval
        self.queue_monitoring_enabled.set()
        tags = tags if tags else []
        mon_queue = MONITORED_QUEUE(queue=queue, name=name, tags=tags)
        self.logger.info(f"Next queue added for monitoring:'{mon_queue}'")
        queue_monitoring_thread = Thread(target=self._queue_monitoring_loop, args=(mon_queue, scrape_interval,), daemon=True)
        queue_monitoring_thread.start()
        self.logger.info(f"Start new thread:{queue_monitoring_thread.name} for monitoring queue:'{mon_queue.name}'")

    def add_metric_point_to_data_collector_queue(self, metric_point: (MONITORING_SYSTEM_POINT,
                                                                      MONITORING_PROCESS_POINT,
                                                                      QUEUE_STATE_MONITORING_POINT,
                                                                      REQUEST_MONITORING_POINT,
                                                                      SYSTEM_ERROR_MONITORING_POINT)):
        self.data_collector_queue.put(metric_point)
        self.logger.debug(f"Next point added to data_collector_queue:'{metric_point}'")

    @staticmethod
    def _metric_processing_loop(metrics_queue: JQueue, max_size: int, log_params: dict, scrape_interval: int,
                                influx_sender_config: dict = None, victoria_sender_config: dict = None):
        logger = get_logger_by_params_and_make_log_folder(log_name="metric_processing", **log_params)

        # ___ example of adding thread in any process for monitoring it ______________________________________
        # Create data collector in worker process with shared metrics queue
        metric_processing_data_collector = DataCollector(data_collector_queue=metrics_queue)
        # Start thread for monitoring this process
        metric_processing_data_collector.start_thread_for_monitoring_process(process_name="metric_processing")
        # ____________________________________________________________________________________________________

        influx_queue = JQueue()
        victoria_queue = JQueue()

        if influx_sender_config['enable']:
            influx_sender = InfluxSender(host=influx_sender_config['host'],
                                         port=influx_sender_config['port'],
                                         user_name=influx_sender_config['user_name'],
                                         user_pass=influx_sender_config['user_pass'],
                                         token=influx_sender_config['token'],
                                         org=influx_sender_config['org'],
                                         bucket=influx_sender_config['bucket'],
                                         db_name=influx_sender_config['db_name'],
                                         use_udp=influx_sender_config['use_udp'],
                                         udp_port=influx_sender_config['udp_port'],
                                         proxies=influx_sender_config['proxies'],
                                         timeout=influx_sender_config['timeout'],
                                         retries=influx_sender_config['retries'],
                                         pool_size=influx_sender_config['pool_size'],
                                         use_ssl=influx_sender_config['use_ssl'],
                                         verify_ssl=influx_sender_config['verify_ssl'],
                                         cert_path=influx_sender_config['cert_path'],
                                         use_gzip=influx_sender_config['use_gzip'],
                                         session=influx_sender_config['session'],
                                         headers=influx_sender_config['headers'],
                                         url_postfix=influx_sender_config['url_postfix'],
                                         logger=logger,
                                         raise_exceptions=influx_sender_config['raise_exceptions'])

            influx_thread = threading.Thread(
                target=influx_sender.influx_sender_thread,
                args=(influx_queue, ),
                daemon=True
            )
            influx_thread.start()
            logger.info(f"Started InfluxDB sender thread: {influx_thread.name}")

        if victoria_sender_config['enable']:
            metric_processing_data_collector.add_queue_to_monitoring(queue=victoria_queue,
                                                                     name="victoria_queue",
                                                                     scrape_interval=scrape_interval)

            victoria_thread = threading.Thread(
                target=victoria_sender_thread,
                args=(victoria_queue, logger, victoria_sender_config),
                daemon=True
            )
            victoria_thread.start()
            logger.info(f"Started VictoriaMetrics logger thread: {victoria_thread.name}")

        while True:
            # Check for queues overflown
            for _queue in [{"queue": metrics_queue, "queue_name": "main_metrics_queue"},
                          {"queue": victoria_queue, "queue_name": "victoria_queue"},
                          {"queue": influx_queue, "queue_name": "influx_queue"}]:
                if _queue["queue"].qsize() > max_size:
                    logger.warning(f"DataCollector {_queue['queue_name']} is overflown! "
                                   f"Max size set as {max_size}' and queue size is:'{_queue['queue'].qsize()}'! "
                                   f"Queue will be cleared!")
                    _queue["queue"].clear()

            # Put metrics in sender queues
            while not metrics_queue.empty():
                item = metrics_queue.get()
                if influx_sender_config['enable'] :
                    influx_points = get_influx_points_by_data_collector_point(item)
                    for influx_point in influx_points:
                        influx_queue.put(influx_point)
                if victoria_sender_config['enable']:
                    victoria_queue.put(item)


            # Process metrics
            # while not metrics_queue.is_empty():
            #     item = metrics_queue.get()
            #     logger.debug(f"Get next monitoring point from queue:'{item}'")
            time.sleep(1)

    def start_processing_process(self):
        self.processing_process = multiprocessing.Process(
            name=f"metric_processing_process_by_{get_current_process_name()}_{get_current_process_id()}",
            target=self._metric_processing_loop,
            args=(self.data_collector_queue, self.max_queue_size, self.log_params,
                  self.default_scrape_interval, self.influx_sender_config, self.victoria_sender_config),
            daemon=True
        )
        self.processing_process.start()
        self.logger.info(f"Start new process:'{self.processing_process.name}' "
                          f"with pid:'{self.processing_process.pid}' for metric processing host.")

    def shutdown(self):
        self.system_monitoring_enabled.clear()
        self.process_monitoring_enabled.clear()
        self.queue_monitoring_enabled.clear()
        if self.processing_process:
            self.processing_process.terminate()
            self.processing_process.join()


# def influx_sender_thread(influx_sender_config: dict,
#                          influx_sender_queue: JQueue,
#                          _logger: logging.Logger):
#
#     _logger.info(f"Start new influx sender thread:{threading.current_thread().name}'")
#     influx_db = InfluxSender(host=influx_sender_config['host'],
#                              port=influx_sender_config['port'],
#                              user_name=influx_sender_config['user_name'],
#                              user_pass=influx_sender_config['user_pass'],
#                              db_name=influx_sender_config['db_name'],
#                              use_udp=influx_sender_config['use_udp'],
#                              udp_port=influx_sender_config['udp_port'],
#                              proxies=influx_sender_config['proxies'],
#                              timeout=influx_sender_config['timeout'],
#                              retries=influx_sender_config['retries'],
#                              pool_size=influx_sender_config['pool_size'],
#                              use_ssl=influx_sender_config['use_ssl'],
#                              verify_ssl=influx_sender_config['verify_ssl'],
#                              cert_path=influx_sender_config['cert_path'],
#                              use_gzip=influx_sender_config['use_gzip'],
#                              session=influx_sender_config['session'],
#                              headers=influx_sender_config['headers'],
#                              url_postfix=influx_sender_config['url_postfix'],
#                              logger=_logger,
#                              raise_exceptions=influx_sender_config['raise_exceptions'])
#     influx_db.check_db_existing()
#
#     while True:
#         try:
#             points_for_send_pack = []
#             for element in range(influx_sender_queue.qsize()):
#                 if not influx_sender_queue.empty():
#                     points_for_send_pack.append(influx_sender_queue.get())
#             influx_db.insert_points_to_db(points=points_for_send_pack, chunk_size=influx_sender_config['chunk_size'])
#
#         except Exception as influx_sender_thread_exception:
#             _logger.exception(f"Exception in influx_sender_thread: {influx_sender_thread_exception}")
#             tags = [TAG(name='process_name', value=multiprocessing.current_process().name),
#                     TAG(name='action_type', value='influx_sender_thread')]
#             system_error_point = SYSTEM_ERROR_MONITORING_POINT(name='system_error',
#                                                                host_name=gethostname(),
#                                                                time_stamp=time.time_ns(),
#                                                                err_code=1,
#                                                                tags=tags)
#             influx_error_points = get_influx_points_by_data_collector_point(system_error_point)
#             for influx_error_point in influx_error_points:
#                 influx_sender_queue.put(influx_error_point)
#         time.sleep(1)


def victoria_sender_thread(victoria_queue: JQueue, logger: logging.Logger):
    logger.info(f"VictoriaMetrics sender thread started: {threading.current_thread().name}")
    while True:
        try:
            if not victoria_queue.empty():
                item = victoria_queue.get()
                logger.debug(f"VictoriaMetrics would send: {item}")
        except Exception as e:
            logger.error(f"Error in VictoriaMetrics logger thread: {e}")
        time.sleep(1)