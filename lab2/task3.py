import hazelcast
import threading
from time import time

lock = threading.Lock()

def increment(map, key):
    for _ in range(10000):
        with lock:
            count = map.get(key).result()
            map.put(key, count +1)


if name == 'main':
    hz = hazelcast.HazelcastClient()
    map = hz.get_map("my-dist-map")
    key = "task_1"
    map.put(key,0)
    start=time()
    threads =[threading.Thread(target=increment,
                               args=[map, key]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time() - start))
    print(map.get("task_1").result())
