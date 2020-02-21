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
                slots_available.append(Slot(start_time, start_time + duration, self.resource))
            start_time = slot.end_time
        slots_available.append(Slot(start_time, 1000000, self.resource))

        return slots_available

    def add_job(self, job, slot):
        """
        :param job: job to be added to schedule
        :param slot: slot where job is added
        :return:
        """
        self.schedule[job] = slot
        self.assign_slot(slot)

    def assign_slot(self, slot):
        """
        Assigns a slot and updates the available slots
        :param slot: slot to be assigned
        :return: None
        """
        self.slots.append(slot)
        self.slots.sort(key=attrgetter('start_time'))
        pass

    def cancel_job(self, job):
        """
        Cancels the job, releases the slots and updates available slots
        :param job:
        :return:
        """
        slot = self.schedule[job]
        self.release_slot(slot)
        del self.schedule[job]

    def release_slot(self, slot):
        """
        Release a slot
        :param slot: slot to be released
        :return: None
        """
        passs

    def finished_job(self, job):
        """
        Updates the schedule when a job is finished
        :param job:
        :return: None
        """
        pass


class JobSchedule:
    def __init__(self, job, slots=None):
        self.job_id = job.id
        self.potential_resources = job.potential_resources
        self.assigned_resources = []
        self.start_time = None
        self.end_time = None
        if slots:
            for slot in slots:
                self.assigned_resources.append(slot.resource_id)
            self.start_time = slot.start_time
            self.end_time = slot.end_time
