import random

size = 1000000
worker_name = 'performance_test'

print("insert into queueworkers values('performance_test');")
print("COPY queue (bibliographicrecordid, agencyid, worker, queued) FROM stdin;")
for _ in range(size):
    print("{}\t{}\t{}\t{}".format(random.randint(100000000, 999999999), '123456', worker_name, 'now()'))
print('\\.')
print(' ')
