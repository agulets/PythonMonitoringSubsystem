import time
import random
import logging
import datetime
import influxdb
import contextlib
from _socket import gethostname
from multiprocessing import current_process
from abc import ABC, abstractmethod
from typing import List, Union, Generator

try:
    import influxdb_client
    from influxdb_client import Point, WritePrecision
    from influxdb_client.client.write_api import SYNCHRONOUS
except ImportError:
    influxdb_client = None

from MonitoringSubsystem.JQueue import JQueue
from MonitoringSubsystem.Commons import time_it
from MonitoringSubsystem.MonitoringDataClasses import (
    MONITORING_PROCESS_POINT, TAG, INFLUX_DATA, MONITORING_SYSTEM_POINT,
    REQUEST_MONITORING_POINT, METRIC, QUEUE_STATE_MONITORING_POINT,
    SYSTEM_ERROR_MONITORING_POINT
)


# ==================== CLIENT ABSTRACTION LAYER ====================
class InfluxClient(ABC):
    @abstractmethod
    def write_points(self, points: List[dict], database: str = None, protocol: str = 'json') -> bool:
        pass

    @abstractmethod
    def create_database(self, dbname: str) -> bool:
        pass

    @abstractmethod
    def drop_database(self, dbname: str) -> bool:
        pass

    @abstractmethod
    def get_list_database(self) -> List[dict]:
        pass

    @abstractmethod
    def switch_database(self, dbname: str) -> None:
        pass


class InfluxClientV1(InfluxClient):
    def __init__(self, host: str, port: int, username: str, password: str, database: str,
                 use_udp: bool, udp_port: int, use_ssl: bool, verify_ssl: bool, proxies: dict,
                 timeout: int, retries: int, pool_size: int, cert_path: str, use_gzip: bool,
                 session: object, headers: dict, url_postfix: str):
        self.client = influxdb.InfluxDBClient(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            use_udp=use_udp,
            udp_port=udp_port,
            ssl=use_ssl,
            verify_ssl=verify_ssl,
            proxies=proxies,
            timeout=timeout,
            retries=retries,
            pool_size=pool_size,
            cert=cert_path,
            gzip=use_gzip,
            session=session,
            headers=headers,
            path=url_postfix
        )

    def write_points(self, points: List[dict], database: str = None, protocol: str = 'json') -> bool:
        return self.client.write_points(
            points=points,
            database=database,
            protocol=protocol
        )

    def create_database(self, dbname: str) -> bool:
        return self.client.create_database(dbname)

    def drop_database(self, dbname: str) -> bool:
        return self.client.drop_database(dbname)

    def get_list_database(self) -> List[dict]:
        return self.client.get_list_database()

    def switch_database(self, dbname: str) -> None:
        self.client.switch_database(dbname)


class InfluxClientV2(InfluxClient):
    def __init__(self, url: str, token: str, org: str, bucket: str, timeout: int,
                 verify_ssl: bool, cert_path: str, proxies: dict, headers: dict):

        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.cert_path = cert_path
        self.proxies = proxies
        self.headers = headers

        self.client = influxdb_client.InfluxDBClient(
            url=self.url,
            token=self.token,
            org=self.org,
            verify_ssl=self.verify_ssl,
            ssl_ca_cert=self.cert_path,
            timeout=self.timeout,
            proxies=self.proxies,
            headers=self.headers
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def _convert_point(self, point_dict: dict) -> Point:
        point = Point(point_dict['measurement'])

        # Add tags
        for tag_key, tag_value in point_dict['tags'].items():
            point.tag(tag_key, str(tag_value))

        # Add fields
        for field_key, field_value in point_dict['fields'].items():
            point.field(field_key, field_value)

        # Convert timestamp to nanoseconds if needed
        timestamp = point_dict['time']
        if isinstance(timestamp, (int, float)) and timestamp < 1e18:
            timestamp = int(timestamp * 1e9)

        point.time(timestamp, WritePrecision.NS)
        return point

    def write_points(self, points: List[dict], database: str = None, protocol: str = 'json') -> bool:
        try:
            converted_points = [self._convert_point(p) for p in points]
            self.write_api.write(bucket=self.bucket, record=converted_points)
            return True
        except Exception as e:
            logging.error(f"Error writing points to InfluxDB v2: {str(e)}")
            return False

    def create_database(self, dbname: str) -> bool:
        try:
            buckets_api = self.client.buckets_api()
            buckets_api.create_bucket(bucket_name=dbname, org=self.org)
            return True
        except Exception as e:
            logging.error(f"Error creating bucket: {str(e)}")
            return False

    def drop_database(self, dbname: str) -> bool:
        try:
            buckets_api = self.client.buckets_api()
            bucket = buckets_api.find_bucket_by_name(dbname)
            if bucket:
                buckets_api.delete_bucket(bucket)
                return True
            return False
        except Exception as e:
            logging.error(f"Error deleting bucket: {str(e)}")
            return False

    def get_list_database(self) -> List[dict]:
        try:
            buckets_api = self.client.buckets_api()
            buckets = buckets_api.find_buckets().buckets
            return [{'name': b.name} for b in buckets]
        except Exception as e:
            logging.error(f"Error getting buckets list: {str(e)}")
            return []

    def switch_database(self, dbname: str) -> None:
        self.bucket = dbname

    def close(self) -> None:
        self.write_api.close()
        self.client.close()


# ==================== MAIN SENDER CLASS ====================
class InfluxSender:
    def __init__(self, host: str = 'localhost', port: int = 8086,
                 # for influx_1.x
                 user_name: str = 'root', user_pass: str = 'root', db_name: str = None,
                 # for influx_2.x
                 token: str = None, org: str = '-', bucket: str = None,
                 use_udp: bool = False, udp_port: int = 4444, proxies: dict = None, timeout: int = 30, retries: int = 3,
                 pool_size: int = 10, use_ssl: bool = False, verify_ssl: bool = False, cert_path: str = None, use_gzip: bool = False,
                 session: object = None, headers: dict = None, url_postfix: str = '',
                 logger: logging.Logger = None, raise_exceptions: bool = True):

        # Common parameters
        self.host = host
        self.port = port
        self.user_name = user_name
        self.user_pass = user_pass
        self.db_name = db_name
        self.use_udp = use_udp
        self.udp_port = udp_port
        self.proxies = proxies or {}
        self.timeout = timeout
        self.retries = retries
        self.pool_size = pool_size
        self.use_ssl = use_ssl
        self.verify_ssl = verify_ssl
        self.cert_path = cert_path
        self.use_gzip = use_gzip
        self.session = session
        self.headers = headers or {}
        self.url_postfix = url_postfix
        self.raise_exceptions = raise_exceptions
        self.logger = logger or logging.getLogger('root')

        # v2 specific parameters
        self.token = token
        self.org = org
        self.bucket = bucket

        # Version auto-detection
        self._detect_version()

    def _detect_version(self) -> None:
        if self.token and self.bucket:
            self.client_version = 'v2'
            self.logger.info("Using InfluxDB v2 client")
        else:
            self.client_version = 'v1'
            self.logger.info("Using InfluxDB v1 client")

    def _log_and_raise_exception(self, exception: Exception, message: str) -> None:
        error_msg = f"{message}: {str(exception)}"
        self.logger.error(error_msg)
        if self.raise_exceptions:
            raise exception

    @contextlib.contextmanager
    def get_client(self) -> Generator[InfluxClient, None, None]:
        client = None
        try:
            if self.client_version == 'v1':
                client = InfluxClientV1(
                    host=self.host,
                    port=self.port,
                    username=self.user_name,
                    password=self.user_pass,
                    database=self.db_name,
                    use_udp=self.use_udp,
                    udp_port=self.udp_port,
                    use_ssl=self.use_ssl,
                    verify_ssl=self.verify_ssl,
                    proxies=self.proxies,
                    timeout=self.timeout,
                    retries=self.retries,
                    pool_size=self.pool_size,
                    cert_path=self.cert_path,
                    use_gzip=self.use_gzip,
                    session=self.session,
                    headers=self.headers,
                    url_postfix=self.url_postfix
                )
            else:
                if not influxdb_client:
                    raise ImportError("influxdb_client package is required for InfluxDB v2 support")

                url = f"{'https' if self.use_ssl else 'http'}://{self.host}:{self.port}"
                client = InfluxClientV2(
                    url=url,
                    token=self.token,
                    org=self.org,
                    bucket=self.bucket,
                    timeout=self.timeout,
                    verify_ssl=self.verify_ssl,
                    cert_path=self.cert_path,
                    proxies=self.proxies,
                    headers=self.headers
                )
            yield client
        except Exception as e:
            self._log_and_raise_exception(e, "Failed to create InfluxDB client")
        finally:
            if client and self.client_version == 'v2':
                client.close()

    def get_db_list(self) -> List[dict]:
        with self.get_client() as client:
            return client.get_list_database()

    def check_db_existing(self, db_name: str = None) -> bool:
        target_db = db_name or self.db_name or self.bucket
        if not target_db:
            self.logger.warning("Database/bucket name not specified")
            return False

        with self.get_client() as client:
            databases = client.get_list_database()
            return any(db['name'] == target_db for db in databases)

    def create_db(self, db_name: str) -> None:
        if self.check_db_existing(db_name):
            self.logger.warning(f"Database/bucket '{db_name}' already exists")
            return

        with self.get_client() as client:
            try:
                if client.create_database(db_name):
                    self.logger.info(f"Successfully created database/bucket: {db_name}")
                else:
                    raise Exception(f"Failed to create database/bucket: {db_name}")
            except Exception as e:
                self._log_and_raise_exception(e, "Database/bucket creation failed")

    def remove_db(self, db_name: str) -> None:
        if not self.check_db_existing(db_name):
            self.logger.warning(f"Database/bucket '{db_name}' does not exist")
            return

        with self.get_client() as client:
            try:
                if client.drop_database(db_name):
                    self.logger.info(f"Successfully removed database/bucket: {db_name}")
                else:
                    raise Exception(f"Failed to remove database/bucket: {db_name}")
            except Exception as e:
                self._log_and_raise_exception(e, "Database/bucket removal failed")

    def set_bd(self, db_name: str) -> None:
        if self.client_version == 'v1':
            if not self.check_db_existing(db_name):
                raise ValueError(f"Database '{db_name}' not found")
            self.db_name = db_name
        else:
            if not self.check_db_existing(db_name):
                raise ValueError(f"Bucket '{db_name}' not found")
            self.bucket = db_name
        self.logger.info(f"Active database/bucket set to: {db_name}")

    def _datetime_nanoseconds_randomizer(self, timestamp: float) -> int:
        base_ns = int(timestamp * 1e9) if timestamp < 1e18 else int(timestamp)
        return base_ns + random.randint(0, 9999)

    def insert_points_to_db(self, points: List[INFLUX_DATA], chunk_size: int = None) -> None:
        processed_points = []
        for point in points:
            point_dict = point._asdict()
            point_dict['time'] = self._datetime_nanoseconds_randomizer(point_dict['time'])
            processed_points.append(point_dict)

        def process_chunk(chunk: List[dict]) -> None:
            with self.get_client() as client:
                try:
                    success = client.write_points(
                        points=chunk,
                        database=self.db_name if self.client_version == 'v1' else None
                    )
                    if not success:
                        raise Exception("Failed to write points to database")

                    self.logger.info(f"Successfully wrote {len(chunk)} points")
                    self.logger.debug(f"Written points: {chunk}")
                except Exception as e:
                    self._log_and_raise_exception(e, "Failed to write points chunk")

        if chunk_size is None:
            process_chunk(processed_points)
        else:
            for i in range(0, len(processed_points), chunk_size):
                chunk = processed_points[i:i + chunk_size]
                process_chunk(chunk)

    def influx_sender_thread(self, sender_queue: JQueue) -> None:
        self.logger.info("Starting InfluxDB sender thread")
        self.check_db_existing()

        while True:
            try:
                points_to_send = []
                while not sender_queue.empty():
                    points_to_send.append(sender_queue.get())

                if points_to_send:
                    self.insert_points_to_db(points_to_send, chunk_size=100)

                time.sleep(0.5)
            except Exception as e:
                # self.logger.error(f"Error in sender thread: {str(e)}")
                self.logger.exception(f"Error in sender thread: {e}")
                error_point = _create_error_point(str(e))
                sender_queue.put(error_point)
                time.sleep(1)


# ==================== DATA CONVERSION FUNCTIONS ====================
def _create_error_point(error_message: str) -> list[INFLUX_DATA]:
    tags = [
        TAG(name='process_name', value=current_process().name),
        TAG(name='action_type', value='influx_sender_thread'),
        TAG(name='error', value=error_message)
    ]

    return get_influx_points_by_data_collector_point(
                SYSTEM_ERROR_MONITORING_POINT(
                    host_name=gethostname(),
                    name='system_error',
                    time_stamp=time.time_ns(),
                    err_code=1,
                    tags=tags
                )
            )

def get_dict_tags_by_tag_list(tag_list: List[TAG]) -> List[dict]:
    return [{tag.name: tag.value} for tag in tag_list]


def get_dict_from_dicts_list(dict_list: List[dict]) -> dict:
    return {k: v for d in dict_list for k, v in d.items()}


def _get_influx_points_by_monitoring_process_point(
        process_point: MONITORING_PROCESS_POINT,
        measurement_name: str = 'process_monitoring',
        extra_tags: List[TAG] = []
) -> INFLUX_DATA:
    tag_list = [
                   TAG(name='process_name', value=process_point.process_name),
                   TAG(name='process_id', value=process_point.process_id),
                   TAG(name='host_name', value=process_point.host_name)
               ] + extra_tags

    fields = {
        field: getattr(process_point, field)
        for field in process_point._fields
        if field not in ['process_name', 'process_id', 'host_name', 'time_stamp']
    }

    return INFLUX_DATA(
        measurement=measurement_name,
        time=process_point.time_stamp,
        tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tag_list)),
        fields=fields
    )


def _get_influx_points_by_monitoring_system_point(
        system_point: MONITORING_SYSTEM_POINT,
        extra_tags: List[TAG] = []
) -> List[INFLUX_DATA]:
    base_tags = [TAG(name='host_name', value=system_point.host_name)] + extra_tags
    points = []

    for field in system_point._fields:
        if field in ['time_stamp', 'host_name']:
            continue

        value = getattr(system_point, field)
        if isinstance(value, list) and all(isinstance(m, METRIC) for m in value):
            for metric in value:
                tags = base_tags + [TAG(name=field, value=metric.name)]
                points.append(INFLUX_DATA(
                    measurement=field,
                    time=system_point.time_stamp,
                    tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tags)),
                    fields={'value': metric.value}
                ))
        else:
            points.append(INFLUX_DATA(
                measurement=field,
                time=system_point.time_stamp,
                tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(base_tags)),
                fields={'value': value}
            ))

    return points


def get_influx_points_by_data_collector_point(
        data_point: Union[
            MONITORING_PROCESS_POINT,
            MONITORING_SYSTEM_POINT,
            REQUEST_MONITORING_POINT,
            QUEUE_STATE_MONITORING_POINT,
            SYSTEM_ERROR_MONITORING_POINT
        ],
        extra_tags: List[TAG] = []
) -> List[INFLUX_DATA]:
    if isinstance(data_point, MONITORING_SYSTEM_POINT):
        return _get_influx_points_by_monitoring_system_point(data_point, extra_tags)

    if isinstance(data_point, MONITORING_PROCESS_POINT):
        return [_get_influx_points_by_monitoring_process_point(data_point, extra_tags=extra_tags)]

    if isinstance(data_point, REQUEST_MONITORING_POINT):
        tags = data_point.tags + extra_tags
        return [INFLUX_DATA(
            measurement=data_point.name,
            time=data_point.time_stamp,
            tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tags)),
            fields={'response_time': data_point.response_time} | {m.name: m.value for m in data_point.metrics}
        )]

    if isinstance(data_point, QUEUE_STATE_MONITORING_POINT):
        tags = [
                   TAG(name='host_name', value=data_point.host_name),
                   TAG(name='queue_name', value=data_point.name)
               ] + data_point.tags + extra_tags
        return [INFLUX_DATA(
            measurement="queues",
            time=data_point.time_stamp,
            tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tags)),
            fields={'queue_size': data_point.size}
        )]

    if isinstance(data_point, SYSTEM_ERROR_MONITORING_POINT):
        tags = [TAG(name='host_name', value=data_point.host_name)] + data_point.tags + extra_tags
        return [INFLUX_DATA(
            measurement=data_point.name,
            time=data_point.time_stamp,
            tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tags)),
            fields={'err_code': data_point.err_code}
        )]

    raise ValueError(f"Unsupported data point type: {type(data_point)}")