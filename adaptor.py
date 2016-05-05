import time
import threading
from constant import *


class Adaptor:
    def __init__(self, state_manager, hardware_monitor, transfer_manager, transfer_policy):
        self.sm = state_manager
        self.hm = hardware_monitor
        self.tm = transfer_manager
        self.transfer_policy = transfer_policy
        # an Event indicating whether processing phase has finished
        self.processing_finished = threading.Event()

    def adapt(self):
        state_sender = threading.Thread(target=self.send_state)
        state_sender.daemon = True
        state_sender.start()
        load_balancer = threading.Thread(target=self.load_balance)
        load_balancer.daemon = True
        load_balancer.start()

    def load_balance(self):
        # periodically check whether a load transfer should occur
        while True:
            remote_state = self.sm.get_remote_system_state()
            my_queue_size = self.tm.get_jobqueue_size()

            if remote_state is not None:
                if remote_state["pending_job"] == 0 and my_queue_size == 0:
                    # join() is used to prevent the case where the last job has been removed from the job queue,
                    # but not been finished yet
                    self.tm.job_queue.join()
                    self.processing_finished.set()
                    return
                else:
                    transfer_decision, transfer_size = self.transfer_policy(
                        remote_state, self.hm.get_hardware_info(), my_queue_size, self.get_cpu_throttling())
                    if transfer_decision is "None":
                        pass
                    elif transfer_decision is "Transfer":
                        for _ in xrange(transfer_size):
                            self.tm.transfer_load()
                    elif transfer_decision is "Request":
                        for _ in xrange(transfer_size):
                            self.tm.request_load()

            time.sleep(ADAPTOR_PERIOD)

    def send_state(self):
        # periodically send local state to the peer node
        while True:
            state = {"pending_job": self.tm.get_jobqueue_size(),
                     "cpu_throttling": self.hm.get_cpu_throttling(),
                     "hardware_info": self.hm.get_hardware_info()}
            self.sm.send_state(state)
            time.sleep(ADAPTOR_PERIOD)

    def get_cpu_throttling(self):
        return self.hm.get_cpu_throttling()
