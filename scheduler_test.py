from base import Job, Resource
from scheduler import Scheduler
from random import randint
from operator import attrgetter
import ast

input_file = r"/home/ajay/workspace/PycharmProjects/scheduler_genalg/test/ts1.txt"
def test(ftest):
    test_content = ftest.readlines()
    number_of_machines = int(test_content[2].split(' ')[-1].replace('\n', ''))
    number_of_resources = int(test_content[3].split(' ')[-1].replace('\n', ''))
    machines = {f'm{i}': Resource(f'm{i}') for i in range(1, number_of_machines+1)}
    resources = {f'r{i}': Resource(f'r{i}') for i in range(1, number_of_resources + 1)}


    jobs = []
    job_id = 1
    for test_id, line in enumerate(test_content[5:]):
        if line.startswith('test'):
            resource_requirements = []
            test_id = ast.literal_eval(line.split(',')[0].replace(' ', '').replace('test(', ''))
            job_length = int(line.split(',')[1].replace(' ', ''))
            machines_string = line[line.find('['):line.find(']') + 1]
            resources_string = ((line[::-1])[line[::-1].find(']'):line[::-1].find('[') + 1])[::-1]
            job_priority = randint(1,50)

            if machines_string == '[]':
                job_machines = machines.values()
            else:
                job_machines = [machines[m] for m in ast.literal_eval(machines_string)]
            resource_requirements.append(job_machines)
            if resources_string != '[]':
                job_resources = [[resources[r]] for r in ast.literal_eval(resources_string)]
                resource_requirements.extend(job_resources)

            jobs.append(Job(f"{job_id}", test_id, job_length, resource_requirements, job_priority))
            job_id += 1
            #jobs.sort(key=attrgetter('priority'), reverse=True)

    t = []
    i = 501
    size = 1000
    while len(t) < size:
        for job in jobs:
            new_j = Job(f"{i}",  job.test_id, job.duration, job.potential_resources, job.duration)
            t.append(new_j)
            i += 1
    jobs.extend(t)
    print(len(jobs))

    scheduler = Scheduler()
    import time
    start = time.time()
    for job in jobs:
        status = scheduler.schedule_job(job, job.potential_resources)
        print(f"Scheduling Job {job.id}->{status}")
        # print(f"current schedule: {scheduler.resources_schedule}")
    scheduler.plot_schedule(max_duration=10000, group_by_resource=True)

    max = 0
    jss = list(scheduler.job_schedule.values())
    jss.sort(key=lambda x: x.start_time)


    print(jss[-1].job.id, jss[-1].start_time, jss[-1].end_time, jss[-1].assigned_resources)

    #print(scheduler.last_finishing_job())
    #scheduler.cancel_job('t11')
    #print(scheduler.last_finishing_job())
    rescheduled_job = []
    cancelled_job = []
    step = 300
    for i in range(0, len(jobs), step):
        rescheduled_job.extend(scheduler.cancel_jobs([job.id for job in jobs[i:i+step]]))
        #print(rescheduled_job)
    # rescheduled_job.extend(scheduler.cancel_jobs([job.id for job in jobs[300:500]]))
    # print(rescheduled_job)
    # rescheduled_job.extend(scheduler.cancel_jobs([job.id for job in jobs[500:600]]))
    # print(rescheduled_job)
    # rescheduled_job.extend(scheduler.cancel_jobs([job.id for job in jobs[600:900]]))
    # print(rescheduled_job)
    # rescheduled_job.extend(scheduler.cancel_jobs([job.id for job in jobs[900:]]))
    # print(rescheduled_job)
    # cancelled_job.append(job.id)
    # #print(f"cancelled jobs:{cancelled_job}")
        #print(scheduler.last_finishing_job())
    #print(rescheduled_job)

    # for job in jobs:
    #     print(f"Scheduling Job {job.id}->{scheduler.schedule_job(job, job.potential_resources)}")
    #     #print(scheduler.last_finishing_job())
    #
    # for job in jobs:
    #     rescheduled_job.extend(scheduler.cancel_job(job.id))
    #     #print(scheduler.last_finishing_job())

    # for job in jobs:
    #     print(f"Scheduling Job {job.id}->{scheduler.schedule_job(job, job.potential_resources)}")
    # print(scheduler.last_finishing_job())
    # for job in jobs:
    #     scheduler.cancel_job(job.id)
    #     #print(scheduler.last_finishing_job())
    end = time.time()
    print(f"time taken: {end - start} seconds jobs scheduled: {len(jobs)} cancelled_jobs: {len(jobs)} rescheduled_jobs; {len(rescheduled_job)}")




with open(input_file, 'r') as ftest:
    test(ftest)
