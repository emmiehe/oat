from datetime import datetime, timedelta
from collections import deque
import math


class Job:
    def __init__(
        self,
        name,
        deadline,
        duration,
        work_unit=timedelta(hours=1),
        priority=0,
        daily_repeat=2,
        dependencies=[],
        progress=timedelta(0),
    ):
        self.name = name
        self.create_time = datetime.now()
        self.deadline = deadline
        self.duration = duration
        self.work_unit = work_unit
        self.priority = priority
        self.daily_repeat = daily_repeat
        self.dependencies = dependencies
        self.progress = progress
        self.is_valid_job()

    def __repr__(self):
        return (
            "Job: {}\n"
            "Create Time: {}\n"
            "Deadline: {}\n"
            "Total Time Left: {}\n"
            "Estimated Duration: {}\n"
            "Work Unit: {}\n"
            "Progress: {}/{}".format(
                self.name,
                self.create_time,
                self.deadline,
                self.deadline - datetime.now(),
                self.duration,
                self.work_unit,
                self.progress,
                self.duration,
            )
        )

    def is_valid_job(self):
        assert (
            self.deadline > self.create_time
            and self.deadline > datetime.now()
            and self.duration - self.progress < self.deadline - self.create_time
        )

    def get_remaining(self):
        return self.duration - self.progress

    def update(self, new_progress):
        self.progress += new_progress


class Chunk:
    def __init__(self, job, duration, index, start=None, end=None):
        self.job = job
        self.duration = duration
        self.index = index
        self.start = start
        self.end = end

    def __repr__(self):
        return "Work Chunk for Job [{}][{}]: {} ({} - {})".format(
            self.job.name, self.index, self.duration, self.start, self.end
        )


now = datetime.now()


class Scheduler:
    # todo: pause, complete, jobs update
    def __init__(
        self,
        jobs,
        break_unit=timedelta(minutes=20),
        daily_start_hour=10,
        daily_end_hour=20,
    ):
        self.jobs = [j for j in jobs if j.get_remaining() > timedelta(0)]
        self.break_unit = break_unit
        self.daily_start_hour = daily_start_hour
        self.daily_end_hour = daily_end_hour

    def calc_bin_index(self, bins, end_datetime, target_datetime):
        return len(bins) - (end_datetime - target_datetime).days - 1

    def make_chunks(self):
        chunks = []
        # sort jobs first
        for job in sorted(
            self.jobs, key=lambda j: (j.priority, j.deadline, j.duration)
        ):

            remaining = job.get_remaining()

            job_chunks = [
                Chunk(
                    job,
                    min(job.work_unit, remaining - i * job.work_unit),
                    i // job.daily_repeat,
                )
                for i in range(math.ceil(remaining / job.work_unit))
            ]
            if job_chunks:
                reversed(job_chunks)
                chunks.append(job_chunks)

        if not chunks:
            return chunks
        # the latest deadline
        deadline_eve = max([job.deadline for job in self.jobs]) - timedelta(minutes=1)
        end = min(
            deadline_eve,
            datetime(
                deadline_eve.year,
                deadline_eve.month,
                deadline_eve.day,
                self.daily_end_hour,
            ),
        )
        bins = [[] for i in range((end - datetime.now()).days + 2)]
        for job_chunks in chunks:
            job_deadline = min(job_chunks[0].job.deadline, end)
            index = job_chunks[0].index
            bin_index = self.calc_bin_index(bins, end, job_deadline)

            for chunk in job_chunks:
                if chunk.index != index:
                    bin_index -= 1
                    index = chunk.index
                chunk.end = end - timedelta(days=(len(bins) - 1 - bin_index))
                bins[bin_index].append(chunk)

        print(bins)
        chunks = []
        for i in range(len(bins) - 1, -1, -1):
            if not bins[i]:
                continue
            bin_chunks = bins[i]
            curr_end = bin_chunks[0].end
            for chunk in bin_chunks:
                if (curr_end - chunk.duration).hour < self.daily_start_hour:
                    if i == 0:
                        print("Oh no this is not schedulable!")
                        return []
                    else:
                        bins[i - 1].append(chunk)  # cp this chunk to next bin
                else:
                    chunk.end = curr_end
                    chunk.start = curr_end - chunk.duration
                    curr_end -= chunk.duration + self.break_unit
                    chunks.append(chunk)

        chunks = sorted(chunks, key=lambda c: c.end)
        if chunks[0].start < datetime.now():
            print(
                "WARNING: starting late by {}, you might not be able "
                "to finish.".format((datetime.now() - chunks[0].start))
            )
        # assert chunks[0].start >= datetime.now()
        return chunks

    def add_job(self, job):
        self.jobs.append(job)
        self.schedule()

    def schedule(self):
        chunks = self.make_chunks()
        print("### Chunks: ### \n")
        for chunk in chunks:
            print(chunk)
        return chunks


if __name__ == "__main__":
    app = Job("Write Scheduler prototype", datetime(2021, 12, 24), timedelta(hours=4))
    scheduler = Scheduler([app])
    scheduler.schedule()
