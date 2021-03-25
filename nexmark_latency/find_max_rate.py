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
win_sizes = [15, 20, 25]  # windows sizes to run
# win_sizes = [2, 5, 10, 15, 20, 25]  # windows sizes to run
rate_min = 6500
rate_max = 20000
rate_step = 500
rate_delta = 1000
num_events = 500_000

expCount = 5

personProportion = 5
auctionProportion = 90
bidProportion = 5

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


def run(window_size, query, num_events, rate):
    query_num = queries[query]
    code = code_template.format(query_num, window_size, num_events, rate, rate,
        personProportion, auctionProportion, bidProportion)
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


def calc_max_rate(window_size, query):
    curr_rate = rate_min
    max_rate = 0
    while curr_rate <= rate_max:
        print(f"RUN with rate = {curr_rate}")
        latency, rate = run(window_size, query, num_events, curr_rate)
        print(f"rate -> {rate}")
        max_rate = max(max_rate, rate)
        if rate + rate_delta < curr_rate:
            print(f"max_rate -> {max_rate}")
            break
        curr_rate += rate_step
    return max_rate


if __name__ == "__main__":
    with open("results.txt", "a") as f:
        f.write(str(datetime.now()) + "\n")
        for win_size in win_sizes:

            print(f"WIN_SIZE: {win_size}, QUERY_1")
            max_rates = []
            for i in range(expCount):
                print(f" > QUERY_1, RUN № {i}")
                max_rate = calc_max_rate(win_size, 1)
                max_rates.append(max_rate)
                print(f" > QUERY_1, RATE IS {max_rate}")

            print(f" >>> WIN_SIZE: {win_size}, QUERY_1, MEAN_RATE = {mean(max_rates)}")



            # print(f"WIN_SIZE: {win_size}, QUERY_2")
            # max_rates = []
            # for i in range(expCount):
            #     print(f" > QUERY_2, RUN № {i}")
            #     max_rate = calc_max_rate(win_size, 2)
            #     max_rates.append(max_rate)
            #     print(f" > QUERY_2, RATE IS {max_rate}")

            # print(f" >>> WIN_SIZE: {win_size}, QUERY_2, MEAN_RATE = {mean(max_rates)}")

        print("FINISHED")
