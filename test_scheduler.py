import unittest, math
from main import Job, Chunk, Scheduler
from datetime import datetime, timedelta


class TestScheduler(unittest.TestCase):
    now = datetime.now()
    next_day = now + timedelta(days=1)
    next_week = now + timedelta(weeks=1)
    next_month = now + timedelta(weeks=4)
    next_year = now + timedelta(weeks=52)
    job_small = Job("One paragraph email", next_day, timedelta(minutes=30))
    job_medium = Job("Written assignment", next_week, timedelta(hours=4))
    job_big = Job(
        "Coding project", next_month, timedelta(hours=40), work_unit=timedelta(hours=2)
    )
    job_habit = Job("Daily puzzle", next_year, timedelta(minutes=30), daily_repeat=1)

    def test_empty(self):
        scheduler = Scheduler([])
        chunks = scheduler.schedule()
        self.assertEqual(chunks, [])

    def schedule_single_job(self, job):
        scheduler = Scheduler([job])
        chunks = scheduler.schedule()
        # verify we do this job in one chunk
        self.assertTrue(len(chunks) == math.ceil(job.duration / job.work_unit))
        # verify we finish before deadline
        self.assertTrue(chunks[-1].end <= job.deadline)

    def test_single_job_small(self):
        self.schedule_single_job(self.job_small)

    def test_single_job_medium(self):
        self.schedule_single_job(self.job_medium)

    def test_single_job_big(self):
        self.schedule_single_job(self.job_big)

    def test_single_job_habit(self):
        self.schedule_single_job(self.job_habit)


if __name__ == "__main__":
    unittest.main()
