from transfer_policy import *

# the maximum UDP packet length, only used for transferring system state
MAX_MSG_LENGTH = 2048

# the host name of remote node
REMOTE_HOST = "sp16-cs423-g14.cs.illinois.edu"

# the host name of local node
LOCAL_HOST = "sp16-cs423-s-g14.cs.illinois.edu"

# the port number state manager listens on (UDP)
STATE_MANAGER_PORT = 60002

# the period of state manager exchanging state information and adaptor balancing load (measured in seconds)
ADAPTOR_PERIOD = 0.6

# the port number transfer manager listens on (RPC)
TRANSFER_MANAGER_PORT = 60001

# how many chunks should the workload be split into?
NUM_CHUNK = 512

# where should the final aggregation result be placed?
RESULT_OUTPUT_FILE = "vector.json"

# what transfer policy should the adaptor use?
TRANSFER_POLICY = sender_initiated_transfer_policy
