from operator import attrgetter

from base import SingletonMetaClass, Job
from schedule import ResourceSchedule, JobSchedule


class Scheduler(metaclass=SingletonMetaClass):
    """
    Scheduler class
    """

    def __init__(self):
        self.resources_schedule = {}  # dictionary of resource schedule
        self.job_schedule = {}  # dictionary of job schedule

    def schedule_job(self, job, requirements):
        """
        Creates a schedule for the job
        :param job: job contains priority, duration
        :param requirements: tuple of resource requirements
        :return: True if scheduled False if can't be scheduled
        """
        # sort based on the length of potential resources
        scheduled_status = True
        requirements.sort(key=lambda resource_list: len(resource_list))
        potential_resource_slots = []
        for potential_resources in requirements:
            slots = self._get_slots(potential_resources, job.duration)
            if not slots:
                scheduled_status = False
                break
            potential_resource_slots.append(slots)

        chosen_slots = self._choose_slots(potential_resource_slots, job.duration)
        if chosen_slots:
            self._update_schedule(job, chosen_slots)
        else:
            print("Unable to schedule as resources not available")
            scheduled_status = False

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
            out.append(start_time_slot)
            slot_req_index = slots_r.index(start_time_slot)
            start_time = start_time_slot.start_time
            end_time = start_time + duration
            outcome = True
            for res_req_index, res_list in enumerate(potential_resource_slots):
                if slot_req_index == res_req_index or res_list[-1].resource_id == start_time_slot.resource_id:
                    continue
                found = False
                for s in res_list:
                    if s.start_time <= start_time and s.end_time >= end_time and s.resource_id != start_time_slot.resource_id:
                        found = True
                        out.append(s)
                        break
                if not found:
                    outcome = False
                    break  # break from for inner_res in res_availability:
            if not outcome:
                out = []
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

    def _update_schedule(self, job, slots):
        print(f"Scheduled Job:{job.id} on slots{slots}")
        self.job_schedule[job.id] = JobSchedule(job, slots)
        for slot in slots:
            self.resources_schedule[slot.resource_id].add_job(job, slot)

    def get_next(self):
        """
        generator yielding next job that can be run
        :return: job which can be run along with the resources
        """

    def cancel_job(self, job):
        """
        Cancel a job and return resouces if allocated
        :param job:
        :return: None
        """
        print(f"Cancelling Job{job.id}")
        job_schedule = self.job_schedule.get(job.id, None)

        if job_schedule:
            for assigned_resource in job_schedule.assigned_resources:
                self.resources_schedule.get(assigned_resource).cancel_job(job)

            for assigned_resource in job_schedule.assigned_resources:
                for job_id in list(self.resources_schedule.get(assigned_resource).schedule.keys()):
                    if job_id != job.id:
                        self._reschedule(self.job_schedule[job_id].job)


    def job_finished(self, job):
        """
        Call when a job is finished
        :param job:
        :return: None
        """


    def _reschedule(self, job):
        print(f"rescheduling job: {job.id}")
        job_schedule = self.job_schedule[job.id]
        print(f"Current start time:{job_schedule.start_time}")
        for assigned_resource in job_schedule.assigned_resources:
            self.resources_schedule[assigned_resource].cancel_job(job)

        del self.job_schedule[job.id]
        self.schedule_job(job, job.potential_resources)



