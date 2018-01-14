import ast
from random import randint


fout_template = 'out/sol{}.txt'

npopulation = 20
iters = 10000


def calc_fitness(schedule):
    # Fitness of a solution is the total time to execute all jobs,
    # which is the end_time of last job on particular machine
    last_end_time = 0
    for m in schedule:
        if len(m) > 0 and m[-1][2] > last_end_time:
            last_end_time = m[-1][2]
    return last_end_time


def sort_population(p):
    return sorted(p, key=lambda x: (-x[1]))     # descending order, sorting by second tuple element - fitness


def init_population(jobs, nmachines, nresources):
    population = []     # population = [(sol1, sol_fitness1), ..., (sol_n, sol_fitness_n)]
    jobs_using_resources = [j for j in jobs if len(j[3]) > 0]
    other_jobs = [j for j in jobs if len(j[3]) == 0]
    for _ in range(npopulation):
        # schedule = sol = [(job1, start_time1, end_time1), ..., (job_n, start_time_n, end_time_n]
        schedule = [[] for _ in range(nmachines)]
        resource_usage = [[] for _ in range(nresources)]    # easier to check resources being used at some point in time

        # First step is to place all jobs holding some global resources as they are more critical than the rest
        for jur in jobs_using_resources:
            rnd_idx = jur[2][randint(0, len(jur[2]) - 1)] if len(jur[2]) > 0 else randint(0, nmachines - 1)
            # place after last one (on rnd_idx machine) or place first if none is already assigned
            job_start_time = schedule[rnd_idx][-1][2] if len(schedule[rnd_idx]) > 0 else 0
            # check resources availability
            for r in jur[3]:
                for ru in resource_usage[r]:
                    if ru[0] <= job_start_time <= ru[1]:
                        job_start_time = ru[1]      # start only when all jobs holding each needed resource finish
            job_end_time = job_start_time + jur[1]
            schedule[rnd_idx].append((jur, job_start_time, job_end_time))  # end_time = start_time + job_length
            for r in jur[3]:
                resource_usage[r].append((job_start_time, job_end_time))

        # Second step is to place all the other jobs on remaining empty places where they can fit (random placement)
        for j in other_jobs:
            rnd_idx = j[2][randint(0, len(j[2]) - 1)] if len(j[2]) > 0 else randint(0, nmachines - 1)
            start_time = schedule[rnd_idx][-1][2] if len(schedule[rnd_idx]) > 0 else 0
            end_time = start_time + j[1]
            schedule[rnd_idx].append((j, start_time, end_time))
        population.append((schedule, calc_fitness(schedule)))

    return population


def cross(parent1, parent2):
    return None


def mutate(child):
    return None


def solve(jobs, nmachines, nresources, gen_alg=False):
    population = sort_population(init_population(jobs, nmachines, nresources))
    if not gen_alg:
        return population[0]

    # Run elimination genetic algorithm for iters iterations
    for _ in range(iters):
        rand_idx = randint(2, len(population) - 1)      # index of the one that we evaluate against created child
        child = cross(population[0], population[1])     # simple elitistic selection
        child = mutate(child)
        if child[1] < population[rand_idx][1]:
            population[rand_idx] = child                # replace chosen solution if child has better fitness score
        sort_population(population)
    return population[0]


def wout(sol, test_idx, njobs):
    with open(fout_template.format(test_idx), 'w') as fout:
        sout = ''
        for i in range(njobs):
            for m_idx, m in enumerate(sol[0]):
                for job in m:
                    if job[0][0] == i:
                        sout += '\'t{}\',{},\'m{}\'.\n'.format(i + 1, job[1], m_idx)
        fout.write(sout)


def test(ftest, test_idx):
    test_content = ftest.readlines()

    number_of_machines = int(test_content[2].split(' ')[-1].replace('\n', ''))
    number_of_resources = int(test_content[3].split(' ')[-1].replace('\n', ''))
    jobs = []
    for job_id, line in enumerate(test_content[5:]):
        if line.startswith('test'):
            job_length = int(line.split(',')[1].replace(' ', ''))
            machines_string = line[line.find('['):line.find(']') + 1]
            resources_string = ((line[::-1])[line[::-1].find(']'):line[::-1].find('[') + 1])[::-1]

            if machines_string == '[]':
                job_machines = []
            else:
                job_machines = sorted([int(m[1:]) - 1 for m in ast.literal_eval(machines_string)])
            if resources_string == '[]':
                job_resources = []
            else:
                job_resources = sorted([int(r[1:]) - 1 for r in ast.literal_eval(resources_string)])
            jobs.append((job_id, job_length, job_machines, job_resources))
    wout(solve(jobs, number_of_machines, number_of_resources), test_idx, len(jobs))


if __name__ == '__main__':
    for i in range(1, 10):
        ftest = open('test/ts{}.txt'.format(i))
        test(ftest, i)
