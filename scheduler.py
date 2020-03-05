import plotly.figure_factory as fig_f
from operator import attrgetter

from base import SingletonMetaClass
from schedule import ResourceSchedule, JobSchedule


class Scheduler(metaclass=SingletonMetaClass):
    """
    Scheduler class
    """

    def __init__(self):
        self.resources_schedule = {}  # dictionary of resource schedule
        self.job_schedule = {}  # dictionary of job schedule

    def _get_schedule(self, job, requirements):
        """
        Creates a schedule for the job
        :param job: job contains priority, duration
        :param requirements: tuple of resource requirements
        :return: Job Schedule
        """
        job_s = None
        requirements.sort(key=lambda resource_list: len(resource_list))
        potential_resource_slots = []
        for potential_resources in requirements:
            slots = self._get_slots(potential_resources, job.duration)
            if not slots:
                break
            potential_resource_slots.append(slots)

        chosen_slots = self._choose_slots(potential_resource_slots, job.duration)
        if chosen_slots:
            # print(f"Scheduled Job:{job.id} on slots{chosen_slots}")
            job_s = JobSchedule(job, chosen_slots)
        else:
            print("Unable to schedule as resources not available")

        return job_s

    def schedule_job(self, job, requirements):
        """
        Creates a schedule for the job
        :param job: job contains priority, duration
        :param requirements: tuple of resource requirements
        :return: True if scheduled False if can't be scheduled
        """
        # sort based on the length of potential resources
        scheduled_status = False
        job_s = self._get_schedule(job, requirements)
        if job_s:
            self._update_schedule(job_s)
            scheduled_status = True
        return scheduled_status

    def _choose_slots(self, potential_resource_slots, duration):
        """
        :param potential_resource_slots: list of list of resource slots
        :return: chosen slots or None if can't be scheduled
        """
        out = []
        max_slots = len(max(potential_resource_slots, key=lambda slots_list: len(slots_list)))
        for l in potential_resource_slots:
            diff = max_slots - len(l)
            if diff:
                last = l[-1]
                for _ in range(diff):
                    l.append(last)
        for slots_r in zip(*potential_resource_slots):
            start_time_slot = max(slots_r, key=lambda x: x.start_time)
            out = [start_time_slot]
            chosen_resources = [start_time_slot.resource_id]
            slot_req_index = slots_r.index(start_time_slot)
            start_time = start_time_slot.start_time
            end_time = start_time + duration
            outcome = True
            for res_req_index, res_list in enumerate(potential_resource_slots):
                if slot_req_index == res_req_index or res_list[-1].resource_id == start_time_slot.resource_id:
                    continue
                found = False
                for s in res_list:
                    if s.start_time <= start_time and s.end_time >= end_time and s.resource_id not in chosen_resources:
                        found = True
                        out.append(s)
                        chosen_resources.append(s.resource_id)
                        break
                if not found:
                    outcome = False
                    break  # break from for inner_res in res_availability:
            if not outcome:
                continue  # get next slot
            if outcome:
                for slots in out:
                    slots.end_time = end_time
                    slots.start_time = start_time
                break

        return out

    def _get_slots(self, potential_resources, duration):
        """
        get available slots for potential resources
        :param potential_resources: tuple of potential resources
        :param test/job duration
        :return: list of Slots
        """
        slots = []
        for resource in potential_resources:
            resource_schedule = self.resources_schedule.get(resource.id, ResourceSchedule(resource))
            self.resources_schedule[resource.id] = resource_schedule
            slots.extend(resource_schedule.get_slots(duration))

        slots.sort(key=attrgetter('start_time'))
        return slots

    def _update_schedule(self, job_s):
        self.job_schedule[job_s.job.id] = job_s
        for slot in job_s.slots:
            self.resources_schedule[slot.resource_id].add_job(job_s, slot)
        return job_s

    def get_next(self):
        """
        generator yielding next job that can be run
        :return: job which can be run along with the resources
        """

    def cancel_job(self, job, reschedule=True):
        """
        Cancel a job and return resouces if allocated
        :param job:
        :return: None
        """
        # print(f"Cancelling Job {job}")
        job_schedule = self.job_schedule.get(job, None)
        rescheduled_job = set()

        # print(f"current schedule:{job_schedule}")

        if job_schedule:
            for assigned_resource in job_schedule.assigned_resources:
                self.resources_schedule.get(assigned_resource).cancel_job(job)

            for assigned_resource in job_schedule.assigned_resources:
                for job_s in list(self.resources_schedule.get(assigned_resource).schedule.values()):
                    if job_s.job.id not in rescheduled_job and job_s.job.id != job and job_s.start_time > job_schedule.start_time \
                            and job_s.last_resource in job_schedule.assigned_resources:
                        if job_s.job.id not in self.job_schedule:
                            self.resources_schedule.get(assigned_resource).cancel_job(job_s.job.id)
                            raise Exception(f"job {job_s.job.id} not in job schedule resource {assigned_resource}")
                        rescheduled_job.add(job_s.job.id)
        if rescheduled_job and reschedule:
            self._reschedule_jobs(rescheduled_job)
            # print(f"Rescheduled_jobs: {rescheduled_job}")
        del self.job_schedule[job]
        return rescheduled_job

    def cancel_jobs(self, jobs):
        print(f"Cancelling jobs:{len(jobs)}")
        rescheduled_jobs = set()
        cancelled_jobs = set(jobs)
        for job in jobs:
            rescheduled_jobs.update(self.cancel_job(job, reschedule=False))

        rescheduled_jobs = rescheduled_jobs - cancelled_jobs
        self._reschedule_jobs(rescheduled_jobs)

        return rescheduled_jobs

    def job_finished(self, job):
        """
        Call when a job is finished
        :param job:
        :return: None
        """

    def _reschedule_jobs(self, rescheduled_jobs):
        print(f"rescheduling jobs:{rescheduled_jobs}")

        for job in rescheduled_jobs:
            old_s = self.job_schedule.get(job, None)
            if old_s is None:
                raise Exception(f"job {job} not present")
            # print(f"Current Schedule:{old_s}")
            for assigned_resource in old_s.assigned_resources:
                self.resources_schedule[assigned_resource].cancel_job(job)

        for job in rescheduled_jobs:
            # print(f"rescheduling jobs: {job}")
            old_s = self.job_schedule.get(job, None)

            # del self.job_schedule[job]
            new_s = self._get_schedule(old_s.job, old_s.job.potential_resources)
            if new_s.start_time < old_s.start_time:
                self._update_schedule(new_s)
            else:
                self._update_schedule(old_s)
            if not self.job_schedule.get(job, None):
                raise Exception(f"Unable to reschedule job: {job}")

    def last_finishing_job(self):
        js = sorted(list(self.job_schedule.values()), key=attrgetter('end_time'))
        if not js:
            return []

        return js[-1]

    def plot_schedule(self, max_duration=None, group_by_resource=None):
        from datetime import datetime, timedelta
        import random
        df = []
        if not group_by_resource:
            for job_s in self.job_schedule.values():
                if (not max_duration) or job_s.start_time <= max_duration:
                    df.append(dict(Task=f"{job_s.job.id}->{job_s.assigned_resources}",
                                   Start=datetime.utcnow() + timedelta(seconds=job_s.start_time),
                                   Finish=datetime.utcnow() + timedelta(seconds=job_s.end_time)
                                   ))

            fig = fig_f.create_gantt(df, showgrid_x=True, showgrid_y=True)
            fig.show()
        else:
            color = 444444
            colors = {}
            for resource, res_schedule in self.resources_schedule.items():
                for job_s in res_schedule.schedule.values():
                    if (not max_duration) or job_s.start_time <= max_duration:
                        df.append(dict(
                            Task=resource,
                            Start=datetime.utcnow() + timedelta(seconds=job_s.start_time),
                            Finish=datetime.utcnow() + timedelta(seconds=job_s.end_time),
                            Job=job_s.job.id,
                            Description=f"{job_s.job.id} {job_s.job.test_id} resources:{job_s.assigned_resources}, duration{job_s.job.duration}"
                        ))
                    if job_s.job.id not in colors:
                        colors[job_s.job.id] = f"#{color}"
                        color += random.randint(1000, 10000)

            fig = fig_f.create_gantt(df, colors=colors, index_col='Job', showgrid_x=True, showgrid_y=True, group_tasks=True, width=10000, height=10000)
            fig.show()


