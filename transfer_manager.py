import Queue
import threading
import xmlrpclib
import logging
import cPickle as pickle
from SimpleXMLRPCServer import SimpleXMLRPCServer
from constant import *


class TransferManager:
    def __init__(self, remote_uri, job_queue, completed_queue):
        self.job_queue = job_queue
        self.completed_queue = completed_queue
        self.proxy = xmlrpclib.ServerProxy(remote_uri)
        self._set_up_rpc()
        self.bootstrap_finished = threading.Event()

    def _set_up_rpc(self):
        # for simplicity, use RPC for transferring load
        # In our case, it will cause one HTTP connection one RPC
        server = SimpleXMLRPCServer(("", TRANSFER_MANAGER_PORT), allow_none=True, logRequests=False)
        logging.info("Transfer manager listening on port %s..." % TRANSFER_MANAGER_PORT)

        server.register_function(self.give_task, "give_task")
        server.register_function(self.fetch_job, "fetch_job")
        server.register_function(self.give_job, "give_job")
        server.register_function(self.fetch_results, "fetch_results")

        rpc_server_thread = threading.Thread(target=lambda: server.serve_forever())
        rpc_server_thread.daemon = True
        rpc_server_thread.start()

    def transfer_load(self):
        """
        Transfer a job to peer node
        """
        try:
            job = self.job_queue.get(False)
            logging.info("Transfer job [%s, %s), queue size: %s" % (job.start, job.end, self.job_queue.qsize()))
            self.proxy.give_job(xmlrpclib.Binary(pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL)))
            self.job_queue.task_done()
        except Queue.Empty as e:
            logging.debug("Error during load transfer: job queue is empty")

    def request_load(self):
        """
        request a job from peer node
        """
        job = self.proxy.fetch_job()
        if job is not None:
            job = pickle.loads(job.data)
            self.job_queue.put(job)
            logging.info("Receive job [%s, %s), queue size: %s" % (job.start, job.end, self.job_queue.qsize()))

    def transfer_workload(self, workload):
        """
        transfer workload to peer node. the function is called by local node in bootstrap phase
        """
        task = xmlrpclib.Binary(pickle.dumps(workload, protocol=pickle.HIGHEST_PROTOCOL))
        self.proxy.give_task(task)

    def collect_results(self):
        """
        collect results from peer node. the function is called by local node in aggregation phase
        """
        return pickle.loads(self.proxy.fetch_results().data)

    def get_jobqueue_size(self):
        return self.job_queue.qsize()

    def fetch_job(self):
        try:
            job = self.job_queue.get(False)
            self.job_queue.task_done()
            logging.info("Transfer job [%s, %s), queue size: %s" % (job.start, job.end, self.job_queue.qsize()))
            return xmlrpclib.Binary(pickle.dumps(job, protocol=pickle.HIGHEST_PROTOCOL))
        except Queue.Empty as e:
            logging.debug("Error during load transfer: job queue is empty")

    def give_job(self, job):
        job = pickle.loads(job.data)
        logging.info("Receive job [%s, %s), queue size: %s" % (job.start, job.end, self.job_queue.qsize()))
        self.job_queue.put(job)

    def give_task(self, task):
        task = pickle.loads(task.data)
        logging.info("Receive workload, start: %s, size: %s" % (task.start, task.length))
        for job in task.split_into_jobs(NUM_CHUNK):
            self.job_queue.put(job)
        self.bootstrap_finished.set()

    def fetch_results(self):
        self.job_queue.join()
        logging.info("Transferring results ...")
        # The function should only be called after processing phase finishes
        # So don't worry about more than one thread access completed_queue
        results = []
        while not self.completed_queue.empty():
            results.append(self.completed_queue.get_nowait())
        return xmlrpclib.Binary(pickle.dumps(results, protocol=pickle.HIGHEST_PROTOCOL))
