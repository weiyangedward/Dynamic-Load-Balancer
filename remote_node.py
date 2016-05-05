import Queue
import threading
import time
import logging
import sys
from worker_thread import worker
from state_manager import StateManager
from hardware_monitor import HardwareMonitor
from transfer_manager import TransferManager
from adaptor import Adaptor
from constant import *


class RemoteNode:
    def __init__(self):
        self.state_manager = None
        self.hardware_monitor = None
        self.transfer_manager = None
        self.adaptor = None
        self.job_queue = Queue.Queue()
        self.completed_queue = Queue.Queue()

    def run(self):
        self.state_manager = StateManager((LOCAL_HOST, STATE_MANAGER_PORT))
        self.hardware_monitor = HardwareMonitor()
        self.transfer_manager = TransferManager("HTTP://%s:%s/" % (LOCAL_HOST, TRANSFER_MANAGER_PORT),
                                                self.job_queue, self.completed_queue)
        self.adaptor = Adaptor(self.state_manager, self.hardware_monitor,
                               self.transfer_manager, TRANSFER_POLICY)

        # remote node won't start processing phase until bootstrap phase finishes
        self.transfer_manager.bootstrap_finished.wait()

        worker_thread = threading.Thread(target=worker,
                                         args=(self.job_queue, self.adaptor, self.completed_queue))
        worker_thread.daemon = True
        worker_thread.start()

        self.adaptor.adapt()

        while True:
            time.sleep(1)


if __name__ == "__main__":
    # logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO,
    #                    datefmt="%a, %d %b %Y %H:%M:%S")
    logging.basicConfig(stream=sys.stdout, format="%(message)s", level=logging.INFO)
    RemoteNode().run()