import os
import json
import shutil
from os.path import join, isfile
from statistics import mean
from datetime import datetime

queries = {1: "16", 2: "17"}
win_size = 10
runs_count = 10

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
        '        --windowSizeSec={}' \
        '        --personProportion={} ' \
        '        --auctionProportion={} ' \
        '        --bidProportion={}'

code_template_with_count = code_template + '  --counting"'


def run(window_size, query, counting):
    query_num = queries[query]
    if counting:
        template = code_template_with_count
    else:
        template = code_template + '"'
    code = template.format(query_num, window_size,
        personProportion, auctionProportion, bidProportion)
    stream = os.popen(code)
    output = stream.read()
    # time = pvarse_output(output)
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
    print(f"   ->   {results}")
    return mean(results)


def parse_output(output):
    split = output.split()
    baseline_index = split.index("(Baseline)")
    print(split)
    return float(split[baseline_index + 6])


def run_n_times(window_size, n, query, counting):
    mean_latency_list = []
    for i in range(n):
        latency = run(window_size, query, counting)
        mean_latency_list.append(latency)
        k = i + 1
        print(f"run №{k} of {n}, latency is {latency}")
    return mean_latency_list, mean(mean_latency_list)


if __name__ == "__main__":
    with open("results.txt", "a") as f:
        f.write(str(datetime.now()) + " WITH_VS_WITHOUT_COUNTING" + "\n")
        f.write(f"WIN_SIZE = : {win_size} \n")
        print(f"WIN_SIZE = {win_size}")

        f.write("QUERY_1 WITH COUNTING:\n")
        print("QUERY_1 WITH COUNTING:")
        latency_list, mean_latency = run_n_times(win_size, runs_count, 1, True)
        for elem in latency_list:
            f.write(str(elem) + "\n")
            print(str(elem))
        f.write("MEAN LATENCY = " + str(mean_latency) + "\n")
        print("MEAN LATENCY = " + str(mean_latency))

        f.write("QUERY_1 WITHOUT COUNTING: \n")
        print("QUERY_1 WITHOUT COUNTING:")
        latency_list, mean_latency = run_n_times(win_size, runs_count, 1, False)
        for elem in latency_list:
            f.write(str(elem) + "\n")
            print(str(elem))
        f.write("MEAN LATENCY = " + str(mean_latency) + "\n")
        print("MEAN LATENCY = " + str(mean_latency))

        print("FINISHED")
