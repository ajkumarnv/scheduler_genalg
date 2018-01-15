import ast
from random import randint, uniform
from copy import deepcopy
import sys
import time


test_template = 'test/ts{}.txt'
fout_template = 'out/res-{}-ts{}.txt'

npopulation = 20
iters = 100000
cross_prob = 0.2
mut_prob = 0.2

# Time measurement consts
m1 = 60
m1_str = '1m'
m5 = 5 * 60
m5_str = '5m'
ne = 15 * 60
ne_str = 'ne'


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
        population.append((schedule, calc_fitness(schedule), resource_usage))

    return population


def cross(parent1, parent2):
    # Take material from parent1, apply to parent2 and return it
    # Specifically, place job2 on the same machine in parent2 on which job1 is placed in parent1
    # => Parent1 dictates location of specific job with probability cross_prob
    child = deepcopy(parent2)
    for m_id, m in enumerate(parent1[0]):
        for j in m:
            # Choose job from parent1 only if it is not the job that holds global resources,
            # otherwise, you most likely end up with solution that is infeasible.
            # Note: you do not need to explicitly check if job can be put on machine m_id
            # as the same machine execution restriction exists for job in parent1 :)
            if len(j[0][3]) == 0 and uniform(0, 1) < cross_prob:
                job_id = j[0][0]
                job_length = j[0][1]
                # Find job with job_id in parent2 and remove it from that machine
                # Then place the same job on new machine (which is possibly the same one)
                removed = False
                placed = False
                for m2_id, m2 in enumerate(child[0]):
                    if not removed:
                        for j2 in m2:
                            # If job_id is found, remove the job from that machine and quit looping
                            if j2[0][0] == job_id:
                                m2.remove(j2)
                                removed = True
                                break
                    if not placed and m2_id == m_id:
                        # Add job on specific machine
                        # Fill out gaps if possible to fit
                        # Note: in case both jobs are on the same machine - you will first remove and then add it again
                        for j2_id, j2 in enumerate(m2[:-1]):
                            # start_time[job_id + 1] - end_time[job_id] (possible gap)
                            if job_length <= m2[j2_id+1][2] - j2[2]:
                                m2.insert(j2_id + 1, j)
                                placed = True
                                break
                        # If it doesn't fit between or if machine is empty, put it on the back
                        if not placed:
                            m2.append(j)
    return child


def mutate(child, nmachines):
    # The mutation mechanism places job on less busy machine, i.e., the machine with smaller end_time of last job
    for m in child[0]:
        for j in m:
            if uniform(0, 1) < mut_prob:
                possible_machines = j[0][2]
                if len(possible_machines) == 0:
                    possible_machines = [i for i in range(nmachines)]
                # Find least busy machine
                least_end_time = sys.maxsize
                lbm_id = 0
                for m_id, m_ in enumerate(child[0]):
                    if m_id in possible_machines and (len(m_) == 0 or (len(m_) > 0 and m_[-1][2] < least_end_time)):
                        least_end_time = m_[-1][2] if len(m_) > 0 else 0
                        lbm_id = m_id
                # Check job's resources availability
                for r in j[0][3]:
                    for ru in child[2][r]:
                        if ru[0] <= least_end_time <= ru[1]:
                            return child    # in case global resource is used at chosen moment, no mutation is applied
                # Add job to least busy machine
                child[0][lbm_id].append(j)
                # Remove job from original machine
                m.remove(j)
    return child


def solve(jobs, nmachines, nresources, gen_alg=False, t=m1):
    start = time.time()
    population = sort_population(init_population(jobs, nmachines, nresources))
    if not gen_alg:
        return population[0]

    # Run elimination genetic algorithm for iters iterations
    for i in range(iters):
        if i % 100 == 0:
            print('Iteration #{} | Fitness: {}'.format(i, population[0][1]))
        rand_idx = randint(2, len(population) - 1)      # index of the one that we evaluate against created child
        child = cross(population[0], population[1])     # simple selection with implicit elitism
        child = mutate(child, nmachines)
        if child[1] < population[rand_idx][1]:
            population[rand_idx] = child                # replace chosen solution if child has better fitness score
        sort_population(population)

        if time.time() - start > t:                    # stop improving if time limit exceeded
            break
    return population[0]


def wout(sol, t_str, test_idx, njobs):
    with open(fout_template.format(t_str, test_idx), 'w') as fout:
        sout = ''
        for i in range(njobs):
            for m_idx, m in enumerate(sol[0]):
                for job in m:
                    if job[0][0] == i:
                        sout += '\'t{}\',{},\'m{}\'.\n'.format(i + 1, job[1], m_idx)
        fout.write(sout)


def test(ftest, test_idx, ntests, gen_alg=False):
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
    for t, t_str in zip([m1, m5, ne], [m1_str, m5_str, ne_str]):
        print('Solving in {} time...'.format(t_str))
        wout(solve(jobs, number_of_machines, number_of_resources, gen_alg, t / ntests), t_str, test_idx, len(jobs))


if __name__ == '__main__':
    for i in range(1, 11):
        ftest = open(test_template.format(i))
        test(ftest, i, 10, gen_alg=True)
