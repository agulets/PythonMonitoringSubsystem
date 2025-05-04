from collections import namedtuple

TAG = namedtuple('TAG', ['name', 'value'])
METRIC = namedtuple('METRIC', ['name', 'value'])

REQUEST_MONITORING_POINT = namedtuple('REQUEST_MONITORING_POINT', [
    'name',
    'time_stamp',
    'response_time',
    'metrics',
    'tags'])

QUEUE_STATE_MONITORING_POINT = namedtuple('QUEUE_STATE_MONITORING_POINT', [
    'host_name',
    'name',
    'time_stamp',
    'size',
    'tags'])

SYSTEM_ERROR_MONITORING_POINT = namedtuple('SYSTEM_ERROR_MONITORING_POINT', [
    'host_name',
    'name',
    'time_stamp',
    'err_code',
    'tags'])

MONITORING_PROCESS_POINT = namedtuple('MONITORING_PROCESS_POINT', [
    'host_name',
    'process_name',
    'process_id',
    'cpu_usage_time',
    'cpu_usage_percent',
    'memory_usage_rss',
    'memory_usage_vms',
    'memory_usage_percent',
    'threads_count',
    'time_stamp'])

MONITORING_SYSTEM_POINT = namedtuple('MONITORING_SYSTEM_POINT', [
    'host_name',
    'boot_time',
    'cpu_freq',
    'cpu_user_time',
    'cpu_system_time',
    'cpu_idle_time',
    'cpu_io_wait_time',
    'cpu_percent_usage_user',
    'cpu_percent_usage_system',
    'cpu_percent_usage_idle',
    'cpu_percent_usage_nice',
    'memory_virtual_total',
    'memory_virtual_used',
    'memory_virtual_available',
    'memory_virtual_free',
    'memory_virtual_usage_percent',
    'memory_swap_total',
    'memory_swap_usage',
    'memory_swap_usage_percent',
    'memory_swap_free',
    'disk_total',
    'disk_used',
    'disk_free',
    'disk_used_percent',
    'disk_io_read_bytes',
    'disk_io_write_bytes',
    'disks_io_busy_time',
    'network_bytes_receive',
    'network_bytes_sent',
    'time_stamp'])

# Namedtuple для хранения информации о мониторируемых очередях и процессах
MONITORED_QUEUE = namedtuple('MONITORED_QUEUE', ['queue', 'name', 'tags'])

# INFLUX_FIELD = namedtuple('INFLUX_FIELD', ['field_name', 'field_value'])
INFLUX_DATA = namedtuple('INFLUX_DATA', ['measurement', 'time', 'tags', 'fields'])
