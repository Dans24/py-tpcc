import sys
import importlib
from datetime import datetime, timedelta
import multiprocessing as mp
from configParser import parseFile

def compute(driver, method, time, counter = None):
    driver.connect()
    now = datetime.now()
    end_time = now + timedelta(0, time)
    if counter is not None:
        counter[method] = 0
    while now < end_time:
        try:
            getattr(driver, method)()
            if counter is not None:
                counter[method] += 1
        except:
            driver.rollback()
        finally:
            now = datetime.now()
    driver.close()

# Configuration
if len(sys.argv) < 2:
    print("Driver is missing")
    exit()
driver_name = sys.argv[1]

default = {
    "warmup": 10,
    "run": 60,
    "update": 1,
    "query": 1,
    "clear": 1,
    "load": 1,
    "host": "localhost",
    "database": "test"
}
config = parseFile(sys.argv[2]) if len(sys.argv) > 2 else {}
default.update(config)
config = default

# Driver import
module_name = driver_name + "Driver"
driver = getattr(importlib.import_module(module_name), module_name)
main_driver = driver(config)

# Before benchmark
print("Connecting...")
main_driver.connect()
if config["clear"]:
    print("Clearing...")
    main_driver.clear()
if config["load"]:
    print("Loading...")
    main_driver.load()
if config["warmup"]:
    print("Warming up for %ds" % config["warmup"])
    compute(main_driver, "query", config["warmup"])
main_driver.close()

# Benchmark
if config["run"]:
    counter = mp.Manager().dict()
    pu = [mp.Process(target=compute, args=(driver(), "update", config["run"], counter)) for _ in range(config["update"])]
    pq = [mp.Process(target=compute, args=(driver(), "query", config["run"], counter)) for _ in range(config["query"])]
    print("Benchmarking for %ds" % config["run"])
    for p in pu:
        p.start()
    for p in pq:
        p.start()
    try:
        for p in pu:
            p.join()
        for p in pq:
            p.join()
    finally:
        # After benchmark
        for k, v in counter.items():
            avg_time = int((float(config["run"]) / v) * 1000000)
            print(u"%s\t%s txn\t%s \u03BCs" % (k, v, avg_time))