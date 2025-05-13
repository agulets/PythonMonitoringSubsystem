import time
import psutil
import platform
from socket import gethostname
from concurrent.futures import ThreadPoolExecutor

from MonitoringSubsystem.Commons import get_logger_by_params_and_make_log_folder
from MonitoringSubsystem.MonitoringDataClasses import METRIC, MONITORING_SYSTEM_POINT

global cpu_usage_percent_host
global cpu_usage_percent_cores


class SystemMonitoring:
    global cpu_usage_percent_host
    global cpu_usage_percent_cores
    CPU_USAGE_TYPES = ['system', 'user', 'idle', 'nice']

    def _get_system_info(self):
        return {'boot_time': psutil.boot_time(),
                'cpu_logical_core_count': psutil.cpu_count(logical=True),
                'cpu_physical_core_count': psutil.cpu_count(logical=False),
                'memory': psutil.virtual_memory(),
                'partitions': self._get_partitions_in_system(),
                'disks': self._get_disks_in_system(),
                'network': psutil.net_if_addrs(),
                'load_avg': psutil.getloadavg()}

    def _cpu_monitoring(self, ):
        global cpu_usage_percent_host
        global cpu_usage_percent_cores

        while self.realtime_cpu_monitoring:
            try:
                cpu_usage_percent_host = psutil.cpu_times_percent(interval=self.cpu_mon_interval)
                cpu_usage_percent_cores = psutil.cpu_times_percent(interval=self.cpu_mon_interval, percpu=True)
            except Exception as err_getting_cpu_metric:
                self.logger.exception(f"Scraping process error:{str(err_getting_cpu_metric)}\n {err_getting_cpu_metric}")

    def __init__(self, cpu_mon_interval=1, logger=None):
        self.start_info = self._get_system_info()
        self.realtime_cpu_monitoring = False
        self.cpu_mon_interval = cpu_mon_interval
        self.logger = logger if logger else get_logger_by_params_and_make_log_folder()

    def __enter__(self):
        self.realtime_cpu_monitoring = True
        self.executor = ThreadPoolExecutor(1)
        self.executor.submit(self._cpu_monitoring).running()
        time.sleep(self.cpu_mon_interval * 3)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.realtime_cpu_monitoring = False
        self.executor.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def _get_partitions_in_system():
        return [sdiskpart.mountpoint for sdiskpart in psutil.disk_partitions()]

    @staticmethod
    def _get_disks_in_system():
        return [key for key in psutil.disk_io_counters(perdisk=True).keys()]

    @staticmethod
    def _get_net_adapters():
        return [key for key in psutil.net_if_addrs().keys()]

    @staticmethod
    def _cpu_user_time():
        return psutil.cpu_times().user

    @staticmethod
    def _cpu_system_time():
        return psutil.cpu_times().system

    @staticmethod
    def _cpu_io_wait_time():
        if platform.system().lower() in ['linux']:
            return psutil.cpu_times().iowait
        else:
            return 0

    @staticmethod
    def _cpu_idle_time():
        return psutil.cpu_times().idle

    def _cpu_usage_percent(self):
        global cpu_usage_percent_host
        global cpu_usage_percent_cores

        if self.realtime_cpu_monitoring:
            _cpu_usage_percent_host = cpu_usage_percent_host
            _cpu_usage_percent_cores = cpu_usage_percent_cores
        else:
            _cpu_usage_percent_host = psutil.cpu_times_percent(interval=self.cpu_mon_interval)
            _cpu_usage_percent_cores = psutil.cpu_times_percent(interval=self.cpu_mon_interval, percpu=True)

        metrics = []
        host_stat_dict = _cpu_usage_percent_host._asdict()
        for key in host_stat_dict.keys():
            metrics.append(METRIC(name=f"cpu_usage_host_{key}",
                                  value=host_stat_dict.get(key)))

        for core_num in range(len(_cpu_usage_percent_cores)):
            core_stat_dict = _cpu_usage_percent_cores[core_num]._asdict()
            for key in core_stat_dict.keys():
                metrics.append(METRIC(name=f"cpu_usage_core_{core_num}_{key}",
                                      value=core_stat_dict.get(key)))
        return metrics

    def _get_cpu_freq(self):
        metrics = []
        # Получаем общую частоту процессора
        overall_freq = psutil.cpu_freq()
        if overall_freq:
            if overall_freq.current is not None:
                metrics.append(METRIC(name="cpu_freq_current", value=overall_freq.current))
            if overall_freq.min is not None:
                metrics.append(METRIC(name="cpu_freq_min", value=overall_freq.min))
            if overall_freq.max is not None:
                metrics.append(METRIC(name="cpu_freq_max", value=overall_freq.max))

        # Получаем частоты для каждого ядра если можем вернуть данные больше чем по одному ядру
        per_core_freq = psutil.cpu_freq(percpu=True)
        if len(per_core_freq) > 1:
            for core_id, core_freq in enumerate(per_core_freq):
                if core_freq.current is not None:
                    metrics.append(METRIC(name=f"cpu_freq_core_{core_id}_current", value=core_freq.current))
                if core_freq.min is not None:
                    metrics.append(METRIC(name=f"cpu_freq_core_{core_id}_min", value=core_freq.min))
                if core_freq.max is not None:
                    metrics.append(METRIC(name=f"cpu_freq_core_{core_id}_max", value=core_freq.max))
        return metrics

    def __get_metrics_by_name_part(self, name_part, metrics=None):
        metrics = metrics if metrics else self._cpu_usage_percent()
        metrics_for_return = []
        for metric in metrics:
            if metric.name.find(name_part) > -1:
                metrics_for_return.append(metric)
        return metrics_for_return

    def _cpu_percent_usage_system(self):
        return self.__get_metrics_by_name_part(name_part='system')

    def _cpu_percent_usage_user(self):
        return self.__get_metrics_by_name_part(name_part='user')

    def _cpu_percent_usage_idle(self):
        return self.__get_metrics_by_name_part(name_part='idle')

    def _cpu_percent_usage_nice(self):
        return self.__get_metrics_by_name_part(name_part='nice')

    @staticmethod
    def _memory_virtual_total():
        return psutil.virtual_memory().total

    @staticmethod
    def _memory_virtual_used():
        return psutil.virtual_memory().used

    @staticmethod
    def _memory_virtual_available():
        return psutil.virtual_memory().available

    @staticmethod
    def _memory_virtual_free():
        return psutil.virtual_memory().free

    @staticmethod
    def _memory_virtual_usage_percent():
        return psutil.virtual_memory().percent

    @staticmethod
    def _memory_swap_total():
        return psutil.swap_memory().total

    @staticmethod
    def _memory_swap_usage():
        return psutil.swap_memory().used

    @staticmethod
    def _memory_swap_usage_percent():
        return psutil.swap_memory().percent

    @staticmethod
    def _memory_swap_free():
        return psutil.swap_memory().free

    def _disks_total(self):
        partitions = self._get_partitions_in_system()
        return [METRIC(name=partition, value=psutil.disk_usage(path=partition).total) for partition in partitions]

    def _disks_used(self):
        partitions = self._get_partitions_in_system()
        return [METRIC(name=partition, value=psutil.disk_usage(path=partition).used) for partition in partitions]

    def _disks_free(self):
        partitions = self._get_partitions_in_system()
        return [METRIC(name=partition, value=psutil.disk_usage(path=partition).free) for partition in partitions]

    def _disks_used_percent(self):
        partitions = self._get_partitions_in_system()
        return [METRIC(name=partition, value=psutil.disk_usage(path=partition).percent) for partition in partitions]

    def _disks_io_read_bytes(self):
        disks = self._get_disks_in_system()
        return [METRIC(name=disk, value=psutil.disk_io_counters(perdisk=True)[f'{disk}'].read_bytes) for
                disk in disks]

    def _disks_io_write_bytes(self):
        disks = self._get_disks_in_system()
        return [METRIC(name=disk, value=psutil.disk_io_counters(perdisk=True)[f'{disk}'].write_bytes) for
                disk in disks]

    def _disks_io_busy_time(self):
        disks = self._get_disks_in_system()
        if platform.system() == 'Linux':
            return [METRIC(name=disk, value=psutil.disk_io_counters(perdisk=True)[f'{disk}'].busy_time) for
                    disk in disks]
        else:
            return [METRIC(name=disk, value=0) for disk in disks]

    def _network_bytes_receive(self):
        net_adapters = self._get_net_adapters()
        return [METRIC(name=adapter, value=psutil.net_io_counters(pernic=True)[f'{adapter}'].bytes_recv) for
                adapter in net_adapters]

    def _network_bytes_sent(self):
        net_adapters = self._get_net_adapters()
        return [METRIC(name=adapter, value=psutil.net_io_counters(pernic=True)[f'{adapter}'].bytes_sent) for
                adapter in net_adapters]

    def get_system_monitoring_point(self):
        monitoring_system_point = None
        try:
             monitoring_system_point = MONITORING_SYSTEM_POINT(
                host_name=gethostname(),
                boot_time=psutil.boot_time(),
                cpu_freq=self._get_cpu_freq(),
                cpu_user_time=self._cpu_user_time(),
                cpu_system_time=self._cpu_system_time(),
                cpu_idle_time=self._cpu_idle_time(),
                cpu_io_wait_time=self._cpu_io_wait_time(),
                cpu_percent_usage_user=self._cpu_percent_usage_user(),
                cpu_percent_usage_system=self._cpu_percent_usage_system(),
                cpu_percent_usage_idle=self._cpu_percent_usage_idle(),
                cpu_percent_usage_nice=self._cpu_percent_usage_nice(),
                memory_virtual_total=self._memory_virtual_total(),
                memory_virtual_used=self._memory_virtual_used(),
                memory_virtual_available=self._memory_virtual_available(),
                memory_virtual_free=self._memory_virtual_free(),
                memory_virtual_usage_percent=self._memory_virtual_usage_percent(),
                memory_swap_total=self._memory_swap_total(),
                memory_swap_usage=self._memory_swap_usage(),
                memory_swap_usage_percent=self._memory_swap_usage_percent(),
                memory_swap_free=self._memory_swap_free(),
                disk_total=self._disks_total(),
                disk_used=self._disks_used(),
                disk_free=self._disks_free(),
                disk_used_percent=self._disks_used_percent(),
                disk_io_read_bytes=self._disks_io_read_bytes(),
                disk_io_write_bytes=self._disks_io_write_bytes(),
                disks_io_busy_time=self._disks_io_busy_time(),
                network_bytes_receive=self._network_bytes_receive(),
                network_bytes_sent=self._network_bytes_sent(),
                time_stamp=time.time_ns()
            )
             self.logger.debug(f"Get system monitoring point:'{monitoring_system_point}'")
        except Exception as err:
            self.logger.exception(err)

        return monitoring_system_point


if __name__ == "__main__":
    sm = SystemMonitoring()
    with sm:
        while True:
            print(sm._get_cpu_freq())
            time.sleep(3)

    # while True:
    #     print(sm.get_system_monitoring_point())
    # time.sleep(3)
