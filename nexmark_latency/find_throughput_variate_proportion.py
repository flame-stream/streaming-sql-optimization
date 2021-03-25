import os
import json
import shutil
from os.path import join, isfile
from statistics import mean
from datetime import datetime

'''
how to run: python3 find_max_rate.py 2>/dev/null
'''

queries = {1: "16", 2: "17"}
rate_min = 10000
rate_max = 25000
rate_step = 500
rate_delta = 1000
num_events = 500_000

expCount = 3

window_size = 10

summary_proportion = 100
const_person_proportion = 5
auction_proportions = [5 * i for i in range(1, 19)]

code_template = './gradlew -Dorg.gradle.java.home=/Library/Java/JavaVirtualMachines/jdk-11.0.2.jdk/Contents/Home' \
                '    :run -Pnexmark.runner=":runners:flink:1.09" -Pnexmark.args="' \
                '        --runner=FlinkRunner ' \
                '        --query={} ' \
                '        --queryLanguage=sql ' \
                '        --streaming=true ' \
                '        --manageResources=false ' \
                '        --monitorJobs=true ' \
                '        --flinkMaster=[local] ' \
                '        --latencyLogDirectory=./log/log ' \
                '        --parallelism=1 ' \
                '        --maxParallelism=1 ' \
                '        --windowSizeSec={} ' \
                '        --numEvents={} ' \
                '        --firstEventRate={} ' \
                '        --nextEventRate={} ' \
                '        --personProportion={} ' \
                '        --auctionProportion={} ' \
                '        --bidProportion={}"'


def run(query, rate, p_proportion, a_proportion, b_proportion):
    query_num = queries[query]
    code = code_template.format(query_num, window_size, num_events, rate, rate,
                                p_proportion, a_proportion, b_proportion)
    stream = os.popen(code)
    output = stream.read()
    max_rate = parse_output(output)
    latency = get_result()
    return latency, max_rate


def get_result():
    results = []
    log_dir = "log"
    files_and_dirs = os.listdir(log_dir)
    only_files = [f for f in files_and_dirs if isfile(join(log_dir, f))]
    for file in only_files:
        with open(f"./{log_dir}/{file}", "r") as my_file:
            data = my_file.readlines()
            json_obj = json.loads(data[0])
            results.append(json_obj["latency"])

    for elem in files_and_dirs:
        if isfile(join(log_dir, elem)):
            os.unlink(os.path.join(log_dir, elem))
        else:
            shutil.rmtree(os.path.join(log_dir, elem))

    return mean(results)


def parse_output(output):
    split = output.split()
    baseline_index = split.index("(Baseline)")
    return float(split[baseline_index + 7])


def calc_max_rate(query, person_prop, auction_prop, bid_prop):
    curr_rate = rate_min
    max_rate = 0
    while curr_rate <= rate_max:
        print(f"RUN with rate = {curr_rate}")
        latency, rate = run(query, curr_rate,
                            person_prop, auction_prop, bid_prop)
        print(f"rate -> {rate}")
        max_rate = max(max_rate, rate)
        if rate + rate_delta < curr_rate:
            print(f"max_rate -> {max_rate}")
            break
        curr_rate += rate_step
    return max_rate


def run_n_times(query, p_prop, a_prop, b_prop):
    max_rates = []
    for i in range(expCount):
        print(f" > RUN № {i}")
        max_rate = calc_max_rate(query, p_prop, a_prop, b_prop)
        max_rates.append(max_rate)
        print(f" > RATE IS {max_rate}")
    return mean(max_rates)


if __name__ == "__main__":
    with open("results.txt", "a") as f:
        kv_res1 = {}
        kv_res2 = {}
        for auction_proportion in auction_proportions:
            bid_proportion = summary_proportion - auction_proportion - const_person_proportion

            print(f"PROPORTIONS: {const_person_proportion}:{auction_proportion}:{bid_proportion}")
            f.write(f"PROPORTIONS: {const_person_proportion}:{auction_proportion}:{bid_proportion}\n")

            f.write("QUERY_1: \n")
            print("QUERY_1:")
            mean_throughput = run_n_times(1, const_person_proportion, auction_proportion, bid_proportion)

            kv_res1[f"{const_person_proportion}:{auction_proportion}:{bid_proportion}"] = mean_throughput

            f.write("mean_throughput: " + str(mean_throughput) + "\n")
            print("mean_throughput: " + str(mean_throughput) + "\n")

            f.write("QUERY_2: \n")
            print("QUERY_2:")
            mean_throughput = run_n_times(2, const_person_proportion, auction_proportion, bid_proportion)

            kv_res2[f"{const_person_proportion}:{auction_proportion}:{bid_proportion}"] = mean_throughput

            print("mean: " + str(mean_throughput) + "\n")
            f.write("mean: " + str(mean_throughput) + "\n")

            print("FINISHED FOR CURRENT PROPORTION")
            print("RESULTS FOR NOW")
            print("Q1:", kv_res1)
            print("Q2:", kv_res2)

        print("FINISHED")
        print("RESULTS:")
        print("Q1:", kv_res1)
        print("Q2:", kv_res2)
