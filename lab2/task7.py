import hazelcast
import threading
from time import time

lock = threading.Lock()

def increment(map, key):
    for _ in range(10000):
        value = count.add_and_get(1)
        map.put(key, value)


if name == 'main':
    hz = hazelcast.HazelcastClient()
    map = hz.get_map("my-dist-map").blocking()
    key = "task5"
    count = hz.cp_subsystem.get_atomic_long(key).blocking()
    map.put(key,0)
    start=time()
    threads =[threading.Thread(target=increment,
                               args=[map, key]) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print("Time: %s seconds" % (time() - start))
    print(map.get("task5"))
