import os
import json
import shutil
from os.path import join, isfile
from statistics import mean
from datetime import datetime

'''
how to run: python3 variate_proportion.py 2>/dev/null
'''

queries = {1: "16", 2: "17"}
win_sizes = [10]  # windows sizes to run
rate = 10000
num_events = 1_000_000

runs_count = 10

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


def run(window_size, query, pers_prop, auc_prop, bid_prop):
    query_num = queries[query]
    code = code_template.format(query_num, window_size, num_events, rate, rate, pers_prop, auc_prop, bid_prop)
    stream = os.popen(code)
    output = stream.read()
    # time = parse_output(output)
    latency = get_result()
    return latency


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
    print(split)
    return float(split[baseline_index + 6])


def run_n_times(window_size, n, query, pers, auc, bid):
    mean_latency_list = []
    for i in range(n):
        print(f"RUN № {i}")
        latency = run(window_size, query, pers, auc, bid)
        print(f"  {latency}")
        mean_latency_list.append(latency)
    return mean_latency_list, mean(mean_latency_list)


if __name__ == "__main__":
    with open("results.txt", "a") as f:
        f.write(str(datetime.now()) + "\n")
        for win_size in win_sizes:
            f.write(f"WIN_SIZE: {win_size} \n")
            print(f"WIN_SIZE: {win_size}")

            for auction_proportion in auction_proportions:
                bid_proportion = summary_proportion - auction_proportion - const_person_proportion

                print(f"PROPORTIONS: {const_person_proportion}:{auction_proportion}:{bid_proportion}")
                f.write(f"PROPORTIONS: {const_person_proportion}:{auction_proportion}:{bid_proportion}")

                f.write("QUERY_1: \n")
                print("QUERY_1:")
                latency_list, mean_latency = run_n_times(win_size, runs_count, 1,
                                                         const_person_proportion, auction_proportion, bid_proportion)
                for elem in latency_list:
                    f.write(str(elem) + "\n")
                f.write("mean: " + str(mean_latency) + "\n")
                print("mean: " + str(mean_latency) + "\n")

                f.write("QUERY_2: \n")
                print("QUERY_2:")
                latency_list, mean_latency = run_n_times(win_size, runs_count, 2,
                                                         const_person_proportion, auction_proportion, bid_proportion)
                for elem in latency_list:
                    f.write(str(elem) + "\n")
                print("mean: " + str(mean_latency) + "\n")
                f.write("mean: " + str(mean_latency) + "\n")

                print("FINISHED")
