import time
import logging
import requests
from typing import List, Union
from MonitoringSubsystem.MonitoringDataClasses import VICTORIA_POINT, MONITORING_SYSTEM_POINT, TAG, MONITORING_PROCESS_POINT, \
    QUEUE_STATE_MONITORING_POINT, SYSTEM_ERROR_MONITORING_POINT, REQUEST_MONITORING_POINT, METRIC
from MonitoringSubsystem.JQueue import JQueue


def convert_timestamp(ts: Union[int, float], logger) -> int:
    try:
        ts_int = int(ts)
        if ts_int == 0:
            return 0

        ts_str = str(ts_int)
        num_digits = len(ts_str)

        if num_digits > 16:
            return ts_int // 10 ** 6
        elif num_digits > 13:
            return ts_int // 10 ** 3
        elif num_digits > 10:
            return ts_int
        elif num_digits > 0:
            return ts_int * 1000
        return 0
    except Exception as e:
        logger.error(f"Error converting timestamp {ts}: {str(e)}")
        return 0


def get_victoria_points_by_data_collector_point(
        data_point: Union[
            MONITORING_SYSTEM_POINT,
            MONITORING_PROCESS_POINT,
            REQUEST_MONITORING_POINT,
            QUEUE_STATE_MONITORING_POINT,
            SYSTEM_ERROR_MONITORING_POINT
        ],
        extra_tags: List[TAG] = None,
        logger: logging.Logger = None
) -> List[VICTORIA_POINT]:

    def add_metric_point(
            metric_name: str,
            value: Union[int, float],
            tags: List[TAG],
            timestamp: int
    ):
        try:
            if isinstance(value, (int, float)):
                points.append(VICTORIA_POINT(
                    metric_name=metric_name,
                    timestamp=timestamp,
                    tags=tags,
                    value=float(value)
                ))
            else:
                logger.warning(f"Invalid value type for {metric_name}: {type(value)}")
        except Exception as e:
            logger.error(f"Error adding metric {metric_name}: {str(e)}")

    def process_system_field(
            field: str,
            value: Union[float, int, List[METRIC]],
            base_tags: List[TAG],
            timestamp: int
    ):
        metric_groups = {
            'disk': [
                'disk_total', 'disk_used', 'disk_free', 'disk_used_percent',
                'disk_io_read_bytes', 'disk_io_write_bytes', 'disks_io_busy_time'
            ],
            'interface': [
                'network_bytes_receive', 'network_bytes_sent'
            ]
        }

        if isinstance(value, list) and all(isinstance(m, METRIC) for m in value):
            for metric in value:
                tag_name = next(
                    (grp for grp, fields in metric_groups.items() if field in fields),
                    'instance'
                )
                metric_tags = base_tags + [TAG(tag_name, metric.name)] + extra_tags
                add_metric_point(
                    metric_name=f"system_{field}",
                    value=metric.value,
                    tags=metric_tags,
                    timestamp=timestamp
                )
        else:
            add_metric_point(
                metric_name=f"system_{field}",
                value=value,
                tags=base_tags + extra_tags,
                timestamp=timestamp
            )

    points = []
    extra_tags = extra_tags or []

    try:
        timestamp = convert_timestamp(data_point.time_stamp, logger=logger)

        if isinstance(data_point, MONITORING_SYSTEM_POINT):
            base_tags = [TAG('host_name', data_point.host_name)]
            excluded_fields = {'host_name', 'time_stamp'}

            for field in data_point._fields:
                if field in excluded_fields:
                    continue

                value = getattr(data_point, field)
                process_system_field(field, value, base_tags, timestamp)

        elif isinstance(data_point, MONITORING_PROCESS_POINT):
            base_tags = [
                TAG('host_name', data_point.host_name),
                TAG('process_name', data_point.process_name),
                TAG('process_id', str(data_point.process_id))
            ]
            excluded_fields = {'host_name', 'process_name', 'process_id', 'time_stamp'}

            for field in data_point._fields:
                if field in excluded_fields:
                    continue

                add_metric_point(
                    metric_name=f"process_{field}",
                    value=getattr(data_point, field),
                    tags=base_tags + extra_tags,
                    timestamp=timestamp
                )

        elif isinstance(data_point, REQUEST_MONITORING_POINT):
            base_tags = data_point.tags + [TAG('endpoint_name', data_point.name)]

            add_metric_point(
                metric_name="request_response_time",
                value=data_point.response_time,
                tags=base_tags + extra_tags,
                timestamp=timestamp
            )

            for metric in data_point.metrics:
                add_metric_point(
                    metric_name=f"request_{metric.name}",
                    value=metric.value,
                    tags=base_tags + extra_tags,
                    timestamp=timestamp
                )

        elif isinstance(data_point, QUEUE_STATE_MONITORING_POINT):
            base_tags = [
                            TAG('host_name', data_point.host_name),
                            TAG('queue_name', data_point.name)
                        ] + data_point.tags

            add_metric_point(
                metric_name="queue_size",
                value=data_point.size,
                tags=base_tags + extra_tags,
                timestamp=timestamp
            )

        elif isinstance(data_point, SYSTEM_ERROR_MONITORING_POINT):
            base_tags = [
                            TAG('host_name', data_point.host_name),
                            TAG('error_name', data_point.name)
                        ] + data_point.tags

            add_metric_point(
                metric_name="system_error",
                value=data_point.err_code,
                tags=base_tags + extra_tags,
                timestamp=timestamp
            )

        else:
            logger.error(f"Unsupported data point type: {type(data_point)}")
            return []

    except Exception as e:
        logger.error(f"Error converting data point {data_point}: {str(e)}")
        return []

    logger.debug(f"Datapoint with type:'{type(data_point)}':\n{data_point} with type:'{type(data_point)}' "
                 f"successfully converted to next VICTORIA_POINTs:\n{points}")
    return points


class VictoriaMetricsSender:
    def __init__(self, url_prometheus_import: str = "http://localhost:8428/api/v1/import/prometheus", timeout: int = 10,
                 logger: logging.Logger = None):
        self.url = url_prometheus_import
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)

    @staticmethod
    def _convert_to_line_protocol(point: VICTORIA_POINT) -> str:
        tags = ",".join([f'{tag.name}="{tag.value}"' for tag in point.tags])
        metric_line = f"{point.metric_name}"
        if tags:
            metric_line += f"{{{tags}}}"
        return f"{metric_line} {point.value} {point.timestamp}"

    def send_points(self, points: List[VICTORIA_POINT]):
        payload = "\n".join([self._convert_to_line_protocol(p) for p in points])
        response = requests.post(self.url,
                                 data=payload,
                                 headers={'Content-Type': 'text/plain'},
                                 timeout=self.timeout)
        response.raise_for_status()


    def sender_loop(self, queue: JQueue):
        self.logger.info("VictoriaMetrics sender loop started")
        while True:
            points_for_send = []
            try:
                while not queue.empty():
                    points_for_send.append(queue.get())
                if points_for_send:
                    self.send_points(points_for_send)
                self.logger.info(f"Successfully wrote {len(points_for_send)} points to victoria")
                self.logger.debug(f"Written points to victoria: {points_for_send}")
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"Error in victoria_sender: {str(e)}")
                for point in points_for_send:
                    queue.put(point)
                time.sleep(1)