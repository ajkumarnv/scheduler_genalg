from collections import OrderedDict
from operator import attrgetter

from base import Slot


class ResourceSchedule:
    def __init__(self, resource):
        self.resource = resource.id
        self.schedule = OrderedDict()
        self.slots = []

    def get_slots(self, duration, reference_time=None):
        """
        get you the next available slot close
        :param duration in seconds
        :param reference_time_slot if passed the slot shall be closest to the reference time
        :
        :return: list of Slots available at resource
        """
        slots_available = []
        start_time = 0
        for slot in self.slots:
            if slot.start_time - start_time >= duration:
                slots_available.append(Slot(start_time, slot.start_time, self.resource))
            start_time = slot.end_time
        slots_available.append(Slot(start_time, 1000000, self.resource))

        return slots_available

    def add_job(self, job_s, slot):
        """
        :param job_s: JobSchedule to be added to schedule
        :param slot: slot where job is added
        :return:
        """
        self.schedule[job_s.job.id] = job_s
        slot.job_id = job_s.job.id
        self.assign_slot(slot)

    def assign_slot(self, slot):
        """
        Assigns a slot and updates the available slots
        :param slot: slot to be assigned
        :return: None
        """
        self.slots.append(slot)
        self.slots.sort(key=attrgetter('start_time'))
        # if not self.verify_schedule():
        #     raise Exception(f"Invalid Schedule for resource:{self.resource} slots:{self.slots}")

    def cancel_job(self, job_id):
        """
        Cancels the job, releases the slots and updates available slots
        :param job_id:
        :return:
        """
        #print(f"cancelling job {job_id} on resource {self.resource}")
        job_s = self.schedule.get(job_id, None)
        if not job_s:
            print(f"Job {job_id} not in resource: {self.resource} schedule:{self.schedule}")
        for slot in self.slots:
            if slot.job_id == job_id:
                found = True
                break
        if found:
            self.slots.remove(slot)

        del self.schedule[job_id]
        if self.schedule.get(job_id, None):
            raise Exception(f"Unable to delete job :{job_id}")

    def release_slot(self, slot):
        """
        Release a slot
        :param slot: slot to be released
        :return: None
        """
        self.slots.remove(slot)

    def finished_job(self, job):
        """
        Updates the schedule when a job is finished
        :param job:
        :return: None
        """
        pass

    def verify_schedule(self):
        if len(self.slots) <= 1:
            return True
        for i in range(1, len(self.slots)):
            if not (self.slots[i].start_time >= self.slots[i-1].end_time):
                print(f"{self.slots[i]} not >= {self.slots[i-1]}")
                return False
        return True

    def __repr__(self):
        return f"""{self.resource}
{self.schedule}
{self.slots}

"""


class JobSchedule:
    def __init__(self, job, slots=None):
        self.job = job
        self.assigned_resources = []
        self.end_time = None
        self.slots = None
        if slots:
            for slot in slots:
                self.assigned_resources.append(slot.resource_id)
                slot.job_id = job.id
            self.end_time = slot.end_time
            self.slots = slots

    @property
    def start_time(self):
        return max(self.slots, key=attrgetter('start_time')).start_time

    @property
    def last_resource(self):
        return max(self.slots, key=attrgetter('start_time')).resource_id

    def __repr__(self):
        return f"{self.job} start_time:{self.start_time} end_time:{self.end_time} slots:{self.slots}"



