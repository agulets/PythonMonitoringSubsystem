import time
import random
from MonitoringSubsystem.DataCollector import DataCollector
from MonitoringSubsystem.JQueue import JQueue
from MonitoringSubsystem.MonitoringDataClasses import TAG


def worker_process(data_collector_queue: JQueue):
    # Create worker queue
    worker_queue = JQueue()

    # Create data collector in worker process with shared metrics queue
    worker_data_collector = DataCollector(data_collector_queue=data_collector_queue)
    # Start process monitoring
    worker_data_collector.start_thread_for_monitoring_process(process_name="worker_process")
    # Add worker queue to monitoring
    worker_data_collector.add_queue_to_monitoring(queue=worker_queue,name="worker_queue", tags=[TAG(name="process", value="worker")])

    # Simulate some work
    while True:
        for i in range(random.randint(1,3)):
            worker_queue.put(f"{time.time()}")
            print("worker - put some stuff to worker_queue")
            time.sleep(random.randint(1,3))

        # Keep alive
        print("worker tick")
        time.sleep(random.randint(1,3))


if __name__ == "__main__":
    # Example usage
    import time
    from multiprocessing import Process

    # Main process

    # Datacollector test InfluxV1.8
    main_data_collector = DataCollector(max_queue_size=20, log_level=30,
                                        influx_sender_enable=True, influx_db_name='test', influx_user_name='test',
                                        influx_user_pass='test', influx_host='localhost', influx_port=8085)
    # Start system monitoring
    main_data_collector.start_system_monitoring_process(scrape_interval=5)
    # # Start main process monitoring
    main_data_collector.start_thread_for_monitoring_process(process_name="Main")
    #
    # Create and start worker process
    worker = Process(name='TEST_worker_process_1', target=worker_process, args=(main_data_collector.get_data_collector_queue(),))
    worker.start()

    while True:
        time.sleep(3)
        print("Main tick")
