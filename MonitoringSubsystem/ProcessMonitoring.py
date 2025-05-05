import os
import time
import psutil
from socket import gethostname
from concurrent.futures import ThreadPoolExecutor
from MonitoringSubsystem.Commons import get_logger_by_params_and_make_log_folder
from MonitoringSubsystem.MonitoringDataClasses import MONITORING_PROCESS_POINT

global cpu_usage_percent_process


def get_current_process_id():
    return os.getpid()


def get_process_name_by_pid(process_id):
    return psutil.Process(pid=process_id).name()


class ProcessMonitoring:
    global cpu_usage_percent_process

    def _cpu_monitoring(self, cpu_mon_interval):
        global cpu_usage_percent_process
        cpu_usage_percent_process = 0.0
        while self.realtime_cpu_monitoring:
            if self.cpu_per_core:
                cpu_usage_percent_process = self.process.cpu_percent(interval=cpu_mon_interval)
            else:
                cpu_usage_percent_process = self.process.cpu_percent(interval=cpu_mon_interval) / self.cpu_count
            # print(f" in thread: {cpu_usage_percent_process}")

    def __init__(self, process_id=None, process_name=None, logger=None, cpu_mon_interval=0.1, cpu_per_core=False):
        self.process_id = process_id if process_id else get_current_process_id()
        self.process_name = process_name if process_name else get_process_name_by_pid(process_id=self.process_id)
        self.process = psutil.Process()
        self.cpu_count = psutil.cpu_count()
        self.logger = logger if logger else get_logger_by_params_and_make_log_folder()
        self.cpu_mon_interval = cpu_mon_interval
        self.realtime_cpu_monitoring = False
        self.cpu_per_core = cpu_per_core

    def __enter__(self):
        self.realtime_cpu_monitoring = True
        self.executor = ThreadPoolExecutor(1)
        self.executor.submit(self._cpu_monitoring, self.cpu_mon_interval).running()
        time.sleep(self.cpu_mon_interval * 3)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.executor.shutdown(wait=False, cancel_futures=True)
        self.realtime_cpu_monitoring = False

    def _set_cpu_mon_interval(self, cpu_mon_interval):
        self.cpu_mon_interval = cpu_mon_interval

    def _get_process_cpu_usage_time(self):
        return self.process.cpu_times().user

    def _get_process_cpu_usage_percent(self):
        if self.realtime_cpu_monitoring:
            return cpu_usage_percent_process
        else:
            return self.process.cpu_percent(self.cpu_mon_interval)

    def _get_process_memory_usage_rss(self):
        return self.process.memory_info().rss

    def _get_process_memory_usage_vms(self):
        return self.process.memory_info().vms

    def _get_process_memory_usage_percent(self):
        return self.process.memory_percent()

    def _get_threads_count(self):
        return self.process.num_threads()

    def get_process_monitoring_point(self):
        process_monitoring_point = None
        try:
            process_monitoring_point = MONITORING_PROCESS_POINT(
            host_name=gethostname(),
            process_name=self.process_name,
            process_id=self.process_id,
            cpu_usage_time=self._get_process_cpu_usage_time(),
            cpu_usage_percent=self._get_process_cpu_usage_percent(),
            memory_usage_rss=self._get_process_memory_usage_rss(),
            memory_usage_vms=self._get_process_memory_usage_vms(),
            memory_usage_percent=self._get_process_memory_usage_percent(),
            threads_count=self._get_threads_count(),
            time_stamp=time.time_ns()
            )
        except Exception as exc:
            self.logger.exception(exc)
        return process_monitoring_point


if __name__ == "__main__":
    from multiprocessing import current_process

    # Process monitoring example
    process_monitoring = ProcessMonitoring(process_name=current_process().name)
    # with process_monitoring:
    #     while True:
    #         print(f"{process_monitoring.get_process_monitoring_point()}")
    #         time.sleep(1)

    while True:
        print(f"{process_monitoring.get_process_monitoring_point()}")
        time.sleep(1)
