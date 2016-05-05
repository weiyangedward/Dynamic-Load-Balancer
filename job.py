from math import ceil, floor
import struct


# pickle protocol 2 requires new-style class.
# use protocol 2 because the space saving is huge in our case (around 50%)
class VectorAdditionTask(object):
    def __init__(self, length=1024*1024*4, start=0):
        self.length = length
        self.start = start
        self.vector = [1.111111] * length

    def halve(self):
        left, right = int(ceil(self.length / 2.0)), int(floor(self.length / 2.0))
        return VectorAdditionTask(left, self.start), VectorAdditionTask(right, self.start + left)

    def split_into_jobs(self, num_job):
        job_sizes = [self.length / num_job] * num_job
        for i in xrange(self.length % num_job):
            job_sizes[i] += 1

        jobs, start = [], 0
        for job_size in job_sizes:
            end = start + job_size
            jobs.append(VectorAdditionJob(start + self.start, end + self.start, self.vector[start:end]))
            start = end
        return jobs

    def fill_in_result(self, job):
        self.vector[job.start - self.start:job.end - self.start] = job.vector

    # def serialize(self):
    #     length = struct.pack("!I", self.length * 8 + 8)
    #     start = struct.pack("!I", self.start)
    #     vector = struct.pack("!%sd" % self.length, self.vector)
    #
    #     return "".join([length, start, vector])
    #
    # @staticmethod
    # def deserialize(msg):
    #     length = struct.unpack("!I", msg[:4])[0] - 8
    #     start = struct.unpack("!I", msg[4:8])[0]
    #     vector = list(struct.unpack("!%sd" % length, msg[8:]))
    #     task = VectorAdditionTask(length, start, False)
    #     task.vector = vector
    #
    #     return task


class VectorAdditionJob(object):
    def __init__(self, start, end, vector):
        # [start, end)
        self.start = start
        self.end = end
        self.vector = vector

    def compute(self):
        for i in xrange(len(self.vector)):
            for _ in xrange(200):
                self.vector[i] += 1.111111

    # def serialize(self):
    #     length = struct.pack("!I", self.length * 8 + 12)
    #     start = struct.pack("!I", self.start)
    #     end = struct.pack("!I", self.end)
    #     vector = struct.pack("!%sd" % len(self.vector), self.vector)
    #
    #     return "".join([length, start, end, vector])
    #
    # @staticmethod
    # def deserialize(msg):
    #     length = struct.unpack("!I", msg[:4])[0] - 12
    #     start = struct.unpack("!I", msg[4:8])[0]
    #     end = struct.unpack("!I", msg[8:12])[0]
    #     vector = list(struct.unpack("!%sd" % length, msg[12:]))
    #     job = VectorAdditionTask(start, end, vector)
    #
    #     return job
