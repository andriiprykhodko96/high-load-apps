import hazelcast
import threading
from time import time

lock = threading.Lock()

def increment(map, key):
    for _ in range(1000):
        map.lock(key)
        try:
            count = map.get(key)
            map.put(key, count + 1)
        finally:
            map.unlock(key)


if name == 'main':
    hz = hazelcast.HazelcastClient()
    map = hz.get_map("my-dist-map").blocking()
    key = "task_3"
    map.put(key,0)
    start=time()
    threads =[threading.Thread(target=increment,
                               args=[map, key]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time() - start))
    print(map.get("task_3"))
