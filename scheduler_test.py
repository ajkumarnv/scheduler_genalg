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
    for job_id, line in enumerate(test_content[5:]):
        if line.startswith('test'):
            resource_requirements = []
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

            jobs.append(Job(job_id, job_length, resource_requirements, job_priority))
            jobs.sort(key=attrgetter('priority'), reverse=True)
    scheduler = Scheduler()
    for job in jobs:
        print(f"Scheduling Job {job.id}->{scheduler.schedule_job(job, job.potential_resources)}")

with open(input_file, 'r') as ftest:
    test(ftest)
