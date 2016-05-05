from math import ceil, floor

# the following parameters are relevant to stability
# initiate transfer only if the difference between estimated completion time exceeds the threshold
DIFF_THRES = 10
# sender will initiate transfer only if its job queue length exceeds the threshold
SENDER_QUEUE_THRES = 20
# receiver will initiate transfer only if peer's job queue length exceeds the threshold
RECEIVER_QUEUE_THRES = 30


def vanilla_transfer_policy(remote_state, hw_info, queue_len, cpu_throttling):
    # return one of ["None", "Request", "Transfer"]
    return "None", 0


def sender_initiated_transfer_policy(remote_state, hw_info, queue_len, cpu_throttling):
    remote_queue_len = remote_state["pending_job"]
    remote_cpu_throttling = remote_state["cpu_throttling"]
    if queue_len > SENDER_QUEUE_THRES:
        local_estimated_time = queue_len / cpu_throttling
        remote_estimated_time = remote_queue_len / remote_cpu_throttling
        if local_estimated_time > remote_estimated_time + DIFF_THRES:
            transfer_size = int(ceil((queue_len * remote_cpu_throttling - remote_queue_len * cpu_throttling) \
                                     / (remote_cpu_throttling + cpu_throttling)))
            return "Transfer", transfer_size

    return "None", 0


def receiver_initiated_transfer_policy(remote_state, hw_info, queue_len, cpu_throttling):
    remote_queue_len = remote_state["pending_job"]
    remote_cpu_throttling = remote_state["cpu_throttling"]
    if remote_queue_len > RECEIVER_QUEUE_THRES:
        local_estimated_time = queue_len / cpu_throttling
        remote_estimated_time = remote_queue_len / remote_cpu_throttling
        if local_estimated_time < remote_estimated_time - DIFF_THRES:
            transfer_size = int(floor(-(queue_len * remote_cpu_throttling - remote_queue_len * cpu_throttling) \
                                      / (remote_cpu_throttling + cpu_throttling)))
            return "Request", transfer_size

    return "None", 0


def symmetric_initiated_transfer_policy(remote_state, hw_info, queue_len, cpu_throttling):
    decision, size = sender_initiated_transfer_policy(remote_state, hw_info, queue_len, cpu_throttling)
    if decision != "None":
        # return size/2 because we know the other side will request the other half
        return "Transfer", size / 2
    else:
        decision, size = receiver_initiated_transfer_policy(remote_state, hw_info, queue_len, cpu_throttling)
        if decision != "None":
            # return size/2 because we know the other side will transfer the other half
            return "Request", size / 2
        else:
            return "None", 0

