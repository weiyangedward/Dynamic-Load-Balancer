import time
import logging


def worker(job_queue, adaptor, completed_queue):
    logging.info("Worker thread running ...")

    while True:
        job = job_queue.get(True)

        start_time = time.time()
        job.compute()
        completed_queue.put(job)
        # task_done() serves as the barrier between processing phase and aggregation phase
        job_queue.task_done()

        logging.debug("Finished job [%s, %s)" % (job.start, job.end))

        # sleep after finishing each job to simulate cpu throttling
        processing_time = time.time() - start_time
        cpu_throttling_value = adaptor.get_cpu_throttling()
        sleep_time = processing_time * (1 - cpu_throttling_value) / cpu_throttling_value
        # call sleep with "0" seconds will cause a context switch?
        if sleep_time > 0:
            time.sleep(sleep_time)
