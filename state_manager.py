import threading
import socket
import logging
import cPickle as pickle
from constant import *


class StateManager:
    def __init__(self, dest):
        self.dest = dest
        self.state_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.state_socket.bind(('', STATE_MANAGER_PORT))
        # lock for getting and setting peer system state
        self.lock = threading.Lock()
        self.remote_state = None

        # daemon thread for updating peer system state
        receiver_thread = threading.Thread(target=self._receive_state)
        receiver_thread.daemon = True
        receiver_thread.start()

    def get_remote_system_state(self):
        with self.lock:
            return self.remote_state

    def update_remote_system_state(self, state):
        with self.lock:
            self.remote_state = state

    def send_state(self, state):
        self.state_socket.sendto(pickle.dumps(state), self.dest)

    def _receive_state(self):
        while True:
            state = self.state_socket.recv(MAX_MSG_LENGTH)
            state = pickle.loads(state)
            logging.debug("Receive remote state: %s" % state)
            self.update_remote_system_state(state)