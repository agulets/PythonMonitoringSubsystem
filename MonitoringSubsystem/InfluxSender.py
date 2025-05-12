import datetime
import random
import logging
import time
from _socket import gethostname
from multiprocessing import current_process

import influxdb
import contextlib
from MonitoringSubsystem.Commons import time_it
from MonitoringSubsystem.JQueue import JQueue
from MonitoringSubsystem.MonitoringDataClasses import MONITORING_PROCESS_POINT, TAG, INFLUX_DATA, MONITORING_SYSTEM_POINT, REQUEST_MONITORING_POINT, \
    METRIC, QUEUE_STATE_MONITORING_POINT, SYSTEM_ERROR_MONITORING_POINT


class InfluxSender:
    def __init__(self, host='localhost', port=8086, user_name='root', user_pass='root', db_name=None, use_udp=False,
                 udp_port=4444, proxies=None, timeout=30, retries=3, pool_size=10, use_ssl=False, verify_ssl=False,
                 cert_path=None, use_gzip=False, session=None, headers=None, url_postfix='', logger=None,
                 raise_exceptions=True):
        self.host = host
        self.port = port
        self.proxies = proxies
        self.user_name = user_name
        self.user_pass = user_pass
        self.db_name = db_name
        self.timeout = timeout
        self.retries = retries
        self.pool_size = pool_size
        self.use_ssl = use_ssl
        self.use_udp = use_udp
        self.udp_port = udp_port
        self.cert_path = cert_path
        self.verify_ssl = verify_ssl
        self.use_gzip = use_gzip
        self.session = session
        self.headers = headers
        self.url_postfix = url_postfix
        self.raise_exceptions = raise_exceptions
        self.logger = logger if logger else logging.getLogger('root')

    def set_host(self, host):
        self.host = host

    def set_port(self, port):
        self.port = port

    def set_user_name(self, user_name):
        self.user_name = user_name

    def set_user_pass(self, user_pass):
        self.user_pass = user_pass

    def set_headers(self, headers):
        self.headers = headers

    def enable_udp(self):
        self.use_udp = True

    def disable_udp(self):
        self.use_udp = False

    def set_udp_port(self, udp_port):
        self.udp_port = udp_port

    def set_proxies(self, proxies):
        self.proxies = proxies

    def enable_cert_check(self):
        self.use_ssl = True

    def disable_cert_check(self):
        self.use_ssl = False

    def set_cert_path(self, cert_path):
        self.cert_path = cert_path

    def set_session(self, session):
        self.session = session

    def set_url_postfix(self, url_postfix):
        self.url_postfix = url_postfix

    def _log_and_raise_exception(self, exception, message):
        error = f"{message}: '{exception}'"
        self.logger.warning(error)
        if self.raise_exceptions:
            self.logger.exception(error)
            raise exception

    @contextlib.contextmanager
    def get_client(self):
        client = influxdb.InfluxDBClient(
            host=self.host,
            port=self.port,
            username=self.user_name,
            password=self.user_pass,
            use_udp=self.use_udp,
            ssl=self.use_ssl,
            verify_ssl=self.verify_ssl,
            database=self.db_name,
            timeout=self.timeout,
            retries=self.retries,
            udp_port=self.udp_port,
            proxies=self.proxies,
            pool_size=self.pool_size,
            path=self.url_postfix,
            cert=self.cert_path,
            gzip=self.use_gzip,
            session=self.session,
            headers=self.headers,
        )
        try:
            yield client
        except Exception as cli_exception:
            self._log_and_raise_exception(cli_exception, "Influx_DB get client error")

    def get_db_list(self):
        with self.get_client() as client:
            return client.get_list_database()

    def check_db_existing(self, db_name=None):
        db_name = db_name if db_name else self.db_name if self.db_name else None
        if not db_name:
            self.logger.warning("db_name not defined!")
            return False
        db_list = self.get_db_list()
        if not db_list:
            self.logger.warning("Can't get db list!")
            return False
        for element in db_list:
            if element['name'] == db_name:
                return True
        return False

    def create_db(self, db_name):
        if self.check_db_existing(db_name=db_name):
            self.logger.warning(f"DB '{db_name}' already exist!")
        with self.get_client() as client:
            try:
                timings = time_it()
                with timings:
                    client.create_database(dbname=db_name)
                self.logger.info(f"Creating db {db_name} complete by {timings.execution_time} sec.")
            except Exception as create_db_err:
                self._log_and_raise_exception(create_db_err, f"Error db creating with db_name '{db_name}'")

    def remove_db(self, db_name):
        if not self.check_db_existing(db_name=db_name):
            self.logger.warning(f"DB '{db_name}' is not exist!")
        with self.get_client() as client:
            try:
                timings = time_it()
                with timings:
                    client.drop_database(dbname=db_name)
                self.logger.info(f"Dropping db {db_name} complete by {timings.execution_time} sec.")
            except Exception as delete_db_error:
                self._log_and_raise_exception(delete_db_error, f"Error db deleting with db_name '{db_name}'")

    def set_bd(self, db_name):
        with self.get_client() as client:
            try:
                if not self.check_db_existing(db_name=db_name):
                    raise ValueError("Not found db for set")
                client.switch_database(db_name)
                self.db_name = db_name
            except Exception as set_db_error:
                self._log_and_raise_exception(set_db_error, f"Error while try to set DB: '{db_name}'")

    def insert_points_to_db(self, points, chunk_size=None):
        def _datetime_nanoseconds_randomizer(date_time):
            # Конвертируем в наносекунды с добавлением случайности
            if isinstance(date_time, (int, float)):  # Если уже timestamp
                base_ns = int(date_time * 1e9) if date_time < 1e18 else int(date_time)
            else:  # Если строковое представление
                dt = datetime.datetime.fromisoformat(date_time.replace('Z', '+00:00'))
                base_ns = int(dt.timestamp() * 1e9)

            # Добавляем случайные 3 цифры (диапазон 0-999 нс) для уникальности
            random_ns = random.randint(0, 9999)
            return base_ns + random_ns

        points_for_insert = []
        for point in points:
            point_for_insert = point._asdict()
            point_for_insert['time'] = _datetime_nanoseconds_randomizer(point_for_insert['time'])
            points_for_insert.append(point_for_insert)

        def _generate_cut_data(_data, _chunk_size):
            for i in (range(0, len(_data), _chunk_size)):
                yield _data[i:i + _chunk_size]

        def _db_insert(_points):
            with self.get_client() as client:
                try:
                    timings = time_it()
                    with timings:
                        client.write_points(points=_points, protocol='json')
                    self.logger.info(f"Inserting {len(_points)} points complete by {timings.execution_time} sec.")
                    self.logger.debug(f"Inserting points: '{_points}'")
                except Exception as insert_points_error:
                    self._log_and_raise_exception(insert_points_error, f"Error in next points inserting: '{_points}'")

        if not chunk_size:
            _db_insert(_points=points_for_insert)
        else:
            for _slice in _generate_cut_data(_data=points_for_insert, _chunk_size=chunk_size):
                _db_insert(_points=_slice)

    def influx_sender_thread(self, _sender_queue: JQueue):
        self.logger.info(f"Thread 'influx_sender_thread' started")
        self.check_db_existing()

        while True:
            try:
                points_for_send_pack = []
                for element in range(_sender_queue.qsize()):
                    if not _sender_queue.empty():
                        points_for_send_pack.append(_sender_queue.get())
                self.insert_points_to_db(points=points_for_send_pack, chunk_size=100)

            except Exception as influx_sender_thread_exception:
                self.logger.exception(f"Exception in influx_sender_thread: {influx_sender_thread_exception}")
                tags = [TAG(name='process_name', value=current_process().name),
                        TAG(name='action_type', value='influx_sender_thread')]
                system_error_point = SYSTEM_ERROR_MONITORING_POINT(
                    host_name=gethostname(),
                    name='system_error',
                    time_stamp=datetime.datetime.now(datetime.UTC).isoformat(timespec='milliseconds'),
                    err_code=1,
                    tags=tags
                )
                influx_error_points = get_influx_points_by_data_collector_point(system_error_point)
                for influx_error_point in influx_error_points:
                    _sender_queue.put(influx_error_point)
            time.sleep(0.5)


def get_dict_tags_by_tag_list(tag_list: list[TAG]):
    return [{tag.name: tag.value} for tag in tag_list]


def get_dict_from_dicts_list(_list:list[dict]):
    _dict = {}
    for _elem in _list:
        _dict.update(_elem)
    return _dict


# +
def _get_influx_points_by_monitoring_process_point(process_point: MONITORING_PROCESS_POINT,
                                                   measurement_name='process_monitoring',
                                                   extra_tags: list[TAG] = []) -> INFLUX_DATA:

    tag_list = [TAG(name='process_name', value=process_point.process_name),
                TAG(name='process_id', value=process_point.process_id),
                TAG(name='host_name', value=process_point.host_name)]
    tag_list.extend(extra_tags) if extra_tags else None

    all_fields = [_elem for _elem in process_point._fields]
    not_value_names = ['process_name', 'process_id', 'host_name', 'time_stamp']
    value_names = []
    for elem in all_fields:
        if elem not in not_value_names:
            value_names.append(elem)

    tags = get_dict_tags_by_tag_list(tag_list=tag_list)
    process_point_as_dict = process_point._asdict()

    fields = []
    for value_name in value_names:
        fields.append({value_name: process_point_as_dict[value_name]})
    influx_point = INFLUX_DATA(measurement=measurement_name,
                               time=process_point.time_stamp,
                               tags=get_dict_from_dicts_list(tags),
                               fields=get_dict_from_dicts_list(fields))
    return influx_point


def _check_for_metric_list(_elem_list):
    for element in _elem_list:
        if type(element) != METRIC:
            return False
    return True


def _check_for_tag_list(_elem_list):
    for element in _elem_list:
        if type(element) != TAG:
            return False
    return True


def check_point_for_tag_type(point):
    for tag in point.tags:
        if type(tag) != TAG:
            err = f"Type of tag is '{type(tag)}' but must be TAG"
            raise ValueError(err)


def _get_tag_by_name(_name):
    return _name.split('_')[0]


def _get_influx_points_by_monitoring_system_point(system_point: MONITORING_SYSTEM_POINT,  extra_tags: list[TAG] = []) -> list[INFLUX_DATA]:
    tag_list = [TAG(name='host_name', value=system_point.host_name), ]
    tag_list.extend(extra_tags) if _check_for_tag_list(extra_tags) else None
    tags = get_dict_tags_by_tag_list(tag_list=tag_list)

    all_data_names = [elem for elem in system_point._fields]
    exclude_value_data_names: list[str] = ['time_stamp', 'host_name']
    value_names = []
    for elem in all_data_names:
        if elem not in exclude_value_data_names:
            value_names.append(elem)

    system_point_point_as_dict = system_point._asdict()

    influx_points: list[INFLUX_DATA] = []
    one_value_names = []
    multy_valued_names = []

    for value_name in value_names:
        if type(system_point_point_as_dict[value_name]) in (int, float, str):
            one_value_names.append(value_name)

        else:
            if type(system_point_point_as_dict[value_name]) == list and _check_for_metric_list(system_point_point_as_dict[value_name]):
                multy_valued_names.append(value_name)

    for value_name in one_value_names:
        influx_point = INFLUX_DATA(measurement=value_name,
                                   time=system_point.time_stamp,
                                   tags=get_dict_from_dicts_list(tags),
                                   fields=get_dict_from_dicts_list([{value_name: system_point_point_as_dict[value_name]}, ])
                                   )
        influx_points.append(influx_point)

    for value_name in multy_valued_names:
        for metric in system_point_point_as_dict[value_name]:
            extended_tags = tags.copy()
            extended_tags.append({_get_tag_by_name(_name=value_name): metric.name})
            influx_point = INFLUX_DATA(measurement=value_name,
                                       time=system_point.time_stamp,
                                       tags=get_dict_from_dicts_list(extended_tags),
                                       fields=get_dict_from_dicts_list([{value_name: metric.value}, ])
                                       )
            influx_points.append(influx_point)

    return influx_points


def _get_influx_point_by_request_monitoring_point(request_monitoring_point: REQUEST_MONITORING_POINT, extra_tags: list[TAG] = []) -> INFLUX_DATA:
    tag_list = []
    tag_list.extend(extra_tags) if _check_for_tag_list(extra_tags) else None
    tag_list.extend(request_monitoring_point.tags) if _check_for_tag_list(request_monitoring_point.tags) else None
    fields = {'response_time': request_monitoring_point.response_time * 1.0}
    fields.update([(metric.name, metric.value) for metric in request_monitoring_point.metrics])
    return INFLUX_DATA(measurement=request_monitoring_point.name,
                       time=request_monitoring_point.time_stamp,
                       tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tag_list=tag_list)),
                       fields=fields
                       )


def _get_influx_point_by_queue_state_monitoring_point(queue_state_monitoring_point: QUEUE_STATE_MONITORING_POINT,
                                                      extra_tags: list[TAG] = []) -> INFLUX_DATA:
    tag_list = [TAG(name='host_name', value=queue_state_monitoring_point.host_name),
                TAG(name='queue_name', value=queue_state_monitoring_point.name)]
    tag_list.extend(extra_tags) if _check_for_tag_list(extra_tags) else None
    tag_list.extend(queue_state_monitoring_point.tags) if _check_for_tag_list(queue_state_monitoring_point.tags) else None
    return INFLUX_DATA(measurement="queues",
                       time=queue_state_monitoring_point.time_stamp,
                       tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tag_list=tag_list)),
                       fields={'queue_size': queue_state_monitoring_point.size})


def _get_influx_point_by_system_error_monitoring_point(system_error_monitoring_point: SYSTEM_ERROR_MONITORING_POINT,
                                                       extra_tags: list[TAG] = []) -> INFLUX_DATA:
    tag_list = [TAG(name='host_name', value=system_error_monitoring_point.host_name)]
    tag_list.extend(extra_tags) if _check_for_tag_list(extra_tags) else None
    tag_list.extend(system_error_monitoring_point.tags)
    return INFLUX_DATA(measurement=system_error_monitoring_point.name,
                       time=system_error_monitoring_point.time_stamp,
                       tags=get_dict_from_dicts_list(get_dict_tags_by_tag_list(tag_list=tag_list)),
                       fields={'err_code': system_error_monitoring_point.err_code})


def get_influx_points_by_data_collector_point(data_collector_point: (MONITORING_PROCESS_POINT,
                                                                     MONITORING_SYSTEM_POINT,
                                                                     REQUEST_MONITORING_POINT,
                                                                     QUEUE_STATE_MONITORING_POINT,
                                                                     SYSTEM_ERROR_MONITORING_POINT),
                                              extra_tags: list[TAG] = []) -> list[INFLUX_DATA]:
    if type(data_collector_point) is MONITORING_SYSTEM_POINT:
        return _get_influx_points_by_monitoring_system_point(data_collector_point, extra_tags=extra_tags)
    elif type(data_collector_point) is MONITORING_PROCESS_POINT:
        return [_get_influx_points_by_monitoring_process_point(data_collector_point, extra_tags=extra_tags)]
    elif type(data_collector_point) is REQUEST_MONITORING_POINT:
        return [_get_influx_point_by_request_monitoring_point(data_collector_point, extra_tags=extra_tags)]
    elif type(data_collector_point) is QUEUE_STATE_MONITORING_POINT:
        return [_get_influx_point_by_queue_state_monitoring_point(data_collector_point, extra_tags=extra_tags)]
    elif type(data_collector_point) is SYSTEM_ERROR_MONITORING_POINT:
        return [_get_influx_point_by_system_error_monitoring_point(data_collector_point, extra_tags=extra_tags)]
    else:
        raise ValueError(f"Data point not in collection for convert:'{data_collector_point}'")


if __name__ == "__main__":
    iflux = InfluxSender()
