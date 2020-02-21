import ast
from random import randint, uniform
from copy import deepcopy
import sys
import time

test_template = 'test/ts{}.txt'
fout_template = 'out_{}/res-{}-ts{}.txt'

npopulation = 100
nelite = 3
nselect = 50
iters = 10000
no_impr_limit = 1000
cross_prob = 0.3
mut_prob = 0.6

# Time measurement consts
m1 = 60
m1_str = '1m'
m5 = 5 * 60
m5_str = '5m'
ne = sys.maxsize  # time is not the stopping condition in this case
ne_str = 'ne'

def verify_schedule(schedule):
    ret = True
    for m_id, m in enumerate(schedule):
        if m and ret:
            index = len(m) - 1
            if m[index][2] < m[index][1]:
                ret = False
                break
            while index > 0:
                if (m[index][2] < m[index][1] or m[index][1] < m[index -1][2]):
                    ret = False
                    break
                index -= 1
        if not ret:
            break
    return ret


def calc_fitness(schedule):
    # Fitness of a solution is the total time to execute all jobs,
    # which is the end_time of last job on particular machine
    last_end_time = 0
    if verify_schedule(schedule):
        for m in schedule:
            if len(m) > 0 and m[-1][2] > last_end_time:
                last_end_time = m[-1][2]
    else:
        last_end_time = 999999999999
    return last_end_time


def sort_population(p):
    return sorted(p, key=lambda x: (x[1]))  # descending order, sorting by second tuple element - fitness


def init_population(jobs, nmachines, nresources):
    population = []  # population = [(sol1, sol_fitness1), ..., (sol_n, sol_fitness_n)]
    jobs_using_resources = [j for j in jobs if len(j[3]) > 0]
    other_jobs = [j for j in jobs if len(j[3]) == 0]
    population = serial_allotment(population,jobs_using_resources, other_jobs, nmachines, nresources)
    for _ in range(npopulation):
        # schedule = sol = [(job1, start_time1, end_time1), ..., (job_n, start_time_n, end_time_n]
        schedule = [[] for _ in range(nmachines)]
        resource_usage = [[] for _ in range(nresources)]  # easier to check resources being used at some point in time

        # First step is to place all jobs holding some global resources as they are more critical than the rest
        for jur in jobs_using_resources:
            rnd_idx = jur[2][randint(0, len(jur[2]) - 1)] if len(jur[2]) > 0 else randint(0, nmachines - 1)
            # place after last one (on rnd_idx machine) or place first if none is yet assigned
            job_start_time = schedule[rnd_idx][-1][2] if len(schedule[rnd_idx]) > 0 else 0
            # check resources availability
            # start only when all jobs holding each needed resource finish
            for r in jur[3]:
                if len(resource_usage[r]) > 0 and resource_usage[r][-1][2] > job_start_time:
                    job_start_time = resource_usage[r][-1][2]
            job_end_time = job_start_time + jur[1]
            schedule[rnd_idx].append([jur, job_start_time, job_end_time])  # end_time = start_time + job_length
            for r in jur[3]:
                resource_usage[r].append((jur, job_start_time, job_end_time))
                resource_usage[r].sort(key=lambda x: x[-1])

            # Second step is to place all the other jobs on remaining empty places where they can fit (random placement)
        for j in other_jobs:
            rnd_idx = j[2][randint(0, len(j[2]) - 1)] if len(j[2]) > 0 else randint(0, nmachines - 1)
            start_time = schedule[rnd_idx][-1][2] if len(schedule[rnd_idx]) > 0 else 0
            end_time = start_time + j[1]
            schedule[rnd_idx].append([j, start_time, end_time])
        population.append([schedule, calc_fitness(schedule), resource_usage])

    return population

def create_available_slots(schedule, requirements, duration, pad_equal=True):
    res_availability = [[(0, 100000000000000, m)] for m in requirements]
    max_length = 1
    for r_index, r in enumerate(requirements):
        l = res_availability[r_index]
        if len(schedule[r]) > 0:
            l.append((schedule[r][-1][2], 1000000, r))
            l.pop(0)
            if schedule[r][0][1] >= duration:
                l.append((0, schedule[r][0][1], r))

        for index, r_s in enumerate(schedule[r][:-1]):
            if schedule[r][index + 1][1] - r_s[2] > duration:
                l.append((r_s[2], schedule[r][index + 1][1], r))
        l.sort(key=lambda x: x[0])

        if len(l) > max_length:
            max_length = len(l)
    if pad_equal:
        for l in res_availability:
            slots_length = len(l)
            if slots_length < max_length:
                last = l[-1]
                for i in range(max_length - slots_length):
                    l.append(last)
    return res_availability


def serial_allotment(population, jobs_using_resources, other_jobs, nmachines, nresources):
    # First step is to place all jobs holding some global resources as they are more critical than the rest
    # schedule = sol = [(job1, start_time1, end_time1), ..., (job_n, start_time_n, end_time_n]
    schedule = [[] for _ in range(nmachines)]
    resource_usage = [[] for _ in range(nresources)]  # easier to check resources being used at some point in time
    for jur in jobs_using_resources:
        job_start_time = 0
        min_start_time = 0
        res_availability = create_available_slots(resource_usage, jur[3], jur[1])
        #     res_availability
        #     start_time = 0
        #     for s in resource_usage[r]:
        #
        #
        #     if len(resource_usage[r]) > 0 and resource_usage[r][-1][1] > min_start_time:
        #         min_start_time = resource_usage[r][-1][1]


        # if len(resource_usage[r]) > 0 and resource_usage[r][-1][2] > job_start_time:
        #     job_start_time = resource_usage[r][-1][2]


        if len(jur[2]) > 0:
            machines = jur[2]
        else:
            machines = range(len(schedule))


        res_availability_m = create_available_slots(schedule, machines, jur[1], pad_equal=True)
        rnd_idx = 0
        job_start_time = 0


        for slots_r in zip(*res_availability):
            start_time_slot = max(slots_r, key=lambda x: x[0])
            start_time = start_time_slot[0]
            end_time = start_time_slot[0] + jur[1]
            outcome = True
            for inner_res in res_availability:
                if inner_res[-1][-1] == start_time_slot[-1]:
                    continue
                found = False
                for s in inner_res:
                    if s[0] <= start_time and s[1] >= end_time:
                        found = True
                        break
                if not found:
                    outcome = False
                    break # break from for inner_res in res_availability:
            if not outcome:
                continue  # get next slot
            if outcome:
                job_start_time = start_time
                outcome = False
                for slots_m in res_availability_m:
                    for s in slots_m:
                        if s[0] <= job_start_time and s[1] >= end_time:
                            outcome = True
                            rnd_idx = s[-1]
                            break
                    if outcome:
                        break
            if outcome:
                break


        # for m in machines:
        #     if len(schedule[m]) == 0:
        #         m_start_time = 0
        #         else:
        #             m_start_time = schedule[m][-1][-1]
        #         if abs(m_start_time - min_start_time[0]) < mindiff:
        #             mindiff = abs(m_start_time - min_start_time[0])
        #             rnd_idx = m
        #     if len(schedule[rnd_idx]) > 0:
        #         job_start_time = max(schedule[rnd_idx][-1][-1], min_start_time[0])
        # else:
        #     for m, s in enumerate(schedule):
        #         if len(schedule[m]) == 0:
        #             m_start_time = 0
        #         else:
        #             m_start_time = schedule[m][-1][-1] + 1
        #
        #         delta = job_start_time - m_start_time
        #         if  delta >= 0 and delta < mindiff:
        #             mindiff = delta
        #             rnd_idx = m
        #     if len(schedule[rnd_idx]) > 0:
        #         job_start_time = max(schedule[rnd_idx][-1][-1] + 1, job_start_time)

        job_end_time = job_start_time + jur[1]
        schedule[rnd_idx].append([jur, job_start_time, job_end_time])  # end_time = start_time + job_length
        schedule[rnd_idx].sort(key=lambda x: x[1])
        for r in jur[3]:
            resource_usage[r].append((jur, job_start_time, job_end_time))
            resource_usage[r].sort(key=lambda x: x[1])

    # Second step is to place all the other jobs on remaining empty places where they can fit (random placement)
    machines = range(nmachines)
    for j in other_jobs:
        job_start_time = 0
        min_start_time = 0
        res_availability_m = [[(0, 100000000000000, m)] for m in machines]
        found = False
        while not found:
            for index, r in enumerate(machines):
                l = res_availability_m[index]
                if len(schedule[r]) > 0:
                    l.append((schedule[r][-1][2], 1000000, r))
                    if schedule[r][0][1] < j[1]:
                        l.pop(0)
                    for index, r_s in enumerate(schedule[r][:-1]):
                        if schedule[r][index + 1][1] - r_s[2] > j[1]:
                            l.append((r_s[2], schedule[r][index + 1][1], r))
                    l.sort(key=lambda x: x[0])
                else:
                    rnd_idx = r
                    start_time = 0
                    found = True
                    break
            if not found:
                for slots_m in zip(*res_availability_m):
                    min_start_slot = min(slots_m, key=lambda x: x[0])
                    min_end_time = min_start_slot[1]
                    if (min_end_time - min_start_slot[0]) >= j[1]:
                        found = True
                        start_time = min_start_slot[0]
                        rnd_idx = min_start_slot[-1]
                        break

        end_time = start_time + j[1]
        schedule[rnd_idx].append([j, start_time, end_time])
        schedule[rnd_idx].sort(key=lambda x: x[1])
    population.append([schedule, calc_fitness(schedule), resource_usage])
    print(calc_fitness(resource_usage))
    return population


## DEBUG
def check_feasibility(child):
    for m in child[0]:
        for j in m:
            for ji in m:
                if ji[1] < j[1] < ji[2]:
                    return False
    return True


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
                same_machine = False
                for m2_id, m2 in enumerate(child[0]):
                    if not removed:
                        for j2 in m2:
                            # If job_id is found, remove the job from that machine and quit looping
                            if j2[0][0] == job_id:
                                if m2_id == m_id:
                                    same_machine = True
                                    break
                                m2.remove(j2)
                                removed = True
                                break
                    if same_machine:
                        break
                    if not placed and m2_id == m_id:
                        # Add job on specific machine
                        # Fill out gaps if possible to fit
                        # Note: in case both jobs are on the same machine - you will first remove and then add it again
                        for j2_id, j2 in enumerate(m2[:-1]):
                            # start_time[job_id + 1] - end_time[job_id] (possible gap)
                            if job_length <= m2[j2_id + 1][2] - j2[2]:
                                new_j = deepcopy(j)
                                new_j[1] = j2[2]
                                new_j[2] = new_j[1] + job_length
                                m2.insert(j2_id + 1, new_j)
                                placed = True
                                break
                        # If it doesn't fit between or if machine is empty, put it on the back
                        if not placed:
                            m2.append(j)

    child[1] = calc_fitness(child[0])  # update fitness
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
                resource_in_use = False
                for r in j[0][3]:
                    if resource_in_use:
                        break
                    for ru in child[2][r]:
                        try:
                            if ru[1] <= least_end_time <= ru[2]:
                                resource_in_use = True
                                break
                        except Exception as e:
                            print(ru)
                            raise e
                if resource_in_use:  # in case global resource is used at chosen moment, no mutation is applied
                    continue
                # Add job to least busy machine
                child[0][lbm_id].append(j)
                # Remove job from original machine
                m.remove(j)

    child[1] = calc_fitness(child[0])  # update fitness
    return child


def solve(jobs, nmachines, nresources, gen_alg=False, t=m1):
    start = time.time()
    population = sort_population(init_population(jobs, nmachines, nresources))
    print(f"before optimizations runtime: {population[0][1]}")
    # return population[0]
    if not gen_alg:
        return population[0]

    better_cnt = 0
    last_improvement = -1
    # Run elimination genetic algorithm for iters iterations
    for i in range(iters):
        if i % 100 == 0:
            print('Iteration #{} | Fitness: {} | Improvements: {}'.format(i, population[0][1], better_cnt))
            better_cnt = 0
            print(f"remaining time {time.time() - start} {t}")
        rand_parent_id1 = randint(0, nselect)  # simple uniform selection among best nselect best solutions
        rand_parent_id2 = randint(0, nselect)
        child = cross(population[rand_parent_id1], population[rand_parent_id2])
        child = mutate(child, nmachines)

        rand_idx = randint(nelite, len(population) - 1)  # index of the one that we evaluate against created child
        if child[1] < population[rand_idx][1]:
            better_cnt += 1
            last_improvement = i
            population[rand_idx] = deepcopy(child)  # replace chosen solution if child has better fitness score
            population = sort_population(population)

        # stop improving if time limit exceeded or no improvements in population for no_impr_limit iterations
        if i - last_improvement > no_impr_limit or time.time() - start > t:
            break
    print('Solution found after {} iterations: {}\n========================'.format(i, population[0][1]))
    return population[0]


def wout(sol, t_str, test_idx, njobs, feasibility):
    with open(fout_template.format(feasibility, t_str, test_idx), 'w') as fout:
        l = []
        sout = ''
        for i in range(njobs):
            for m_idx, m in enumerate(sol[0]):
                for job in m:
                    if job[0][0] == i:
                        ress = []
                        for r_index, r in enumerate(sol[2]):
                            for s in r:
                                if s[0][0] == job[0][0]:
                                    ress.append((r_index, s))
                        sout += '\'t{}\',{},\'m{}\' {} {}. {}\n'.format(i + 1, job[1], m_idx + 1, job[0][-2],
                                                                        job[0][-1], ress)
                        l.append((i + 1, job[1], job[2], m_idx + 1, job[0][-3], job[0][-2], job[0][-1], ress, job[0][1]))
        from itertools import groupby
        # groupby(sol[0])
        # sol

        sout += "#" * 100 + "\n"
        sout += "#" * 100 + "\n"
        sout += "#" * 100 + "\n"

        l = sorted(l, key=lambda a: a[1])

        for i in l:
            sout += f"t{i[0]}, start:{i[1]}, end:{i[2]} duration:{i[8]} m{i[3]} res_req:{i[5]} priority:{i[6]} resources:{i[7]}\n"
        fout.write(sout)
        for m_index, s in enumerate(sol[0]):
            print(f"m{m_index + 1}->{s}")


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
            job_priority = 1

            if machines_string == '[]':
                job_machines = []
            else:
                job_machines = sorted([int(m[1:]) - 1 for m in ast.literal_eval(machines_string)])
            if resources_string == '[]':
                job_resources = []
            else:
                job_resources = sorted([int(r[1:]) - 1 for r in ast.literal_eval(resources_string)])
            jobs.append((job_id, job_length, job_machines, job_resources, job_priority))
    # jobs = sorted(jobs, key=lambda job: job[-1], reverse=True)
    for job in jobs:
        pass
        # print(job)
    for t, t_str in zip([m1, m5, ne], [m1_str, m5_str, ne_str]):
        print('Solving in {} time...'.format(t_str))
        wout(solve(deepcopy(jobs), number_of_machines, number_of_resources, gen_alg, t / ntests),
             t_str + "optimized", test_idx, len(jobs), 'infeasible' if gen_alg is True else 'feasible')


if __name__ == '__main__':
    for i in range(1, 2):
        test_input_file = test_template.format(i)
        print(f"running test for file:{test_input_file}")
        with open(test_input_file) as ftest:
            test(ftest, i, 0.1, gen_alg=False)
