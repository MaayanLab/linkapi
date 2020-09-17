


import h5py
import numpy as np
import pandas as pd
import time
import random
import math

rr = np.random.randint(100000, size=(10000, 1000))

f = h5py.File("test_1.h5", "w")
dset = f.create_dataset("c1", (10000,1000), data=rr, chunks=(10000, 1), dtype='i')
f.close()


f = h5py.File("test_2.h5", "w")
dset = f.create_dataset("c1", (10000,1000), data=rr, chunks=(10000, 2), dtype='i')
f.close()


f = h5py.File("test_10.h5", "w")
dset = f.create_dataset("c1", (10000,1000), data=rr, chunks=(10000, 10), dtype='i')
f.close()


rr = np.random.randint(100000, size=(30000, 20000))
f = h5py.File("test_x.h5", "w")
dset = f.create_dataset("c1", (30000,20000), data=rr, dtype='i')
f.close()



start = time.time()
f = h5py.File('test_1.h5', 'r')
res = pd.DataFrame(f['c1'])
f.close()
print(time.time()-start)

start = time.time()
f = h5py.File('test_2.h5', 'r')
res = pd.DataFrame(f['c1'])
f.close()
print(time.time()-start)

start = time.time()
f = h5py.File('test_10.h5', 'r')
res = pd.DataFrame(f['c1'])
f.close()
print(time.time()-start)


sa = sorted(random.sample(range(0,20000), 1000))

start = time.time()
f = h5py.File('test_x.h5', 'r')
res = pd.DataFrame(f['c1'][:,sa])
f.close()
print(time.time()-start)
print(res.shape)


start = time.time()
f = h5py.File('test_x.h5', 'r')
res = pd.DataFrame(f['c1'][:,sa])
f.close()
print(time.time()-start)
print(res.shape)



sa = sorted(random.sample(range(0,200000), 1000))

start = time.time()
f = h5py.File('/Users/maayanlab/OneDrive/singlecell/mouse_matrix.h5', 'r')
res = pd.DataFrame(f['data/expression'][sa,:])
f.close()
print(time.time()-start)
print(res.shape)





f = h5py.File('/Users/maayanlab/OneDrive/singlecell/mouse_matrix.h5', 'r')
res = f["data/expression"]
f2 = h5py.File("data/test_real.h5", "w")
dset = f2.create_dataset("c1", res.shape, dtype='i', chunks=(1, 200), compression="gzip")
sp = dset.shape
f2.close()
f.close()

chunk = int(math.floor(sp[0]/50))
f = h5py.File('/Users/maayanlab/OneDrive/singlecell/mouse_matrix.h5', 'r')
f2 = h5py.File("data/test_real.h5", "a")
dset = f2["c1"]
for i in range(0,51):
    print(i)
    start = i*chunk
    to = min((i+1)*chunk, sp[0])
    dset[start:to, :] = f["data/expression"][start:to, :]

f2.close()
f.close()



sa = sorted(random.sample(range(0,200000), 1000))

start = time.time()
f = h5py.File('data/test_real.h5', 'r')
res = pd.DataFrame(f['c1'][sa,:])
f.close()
print(time.time()-start)
print(res.shape)






f = h5py.File('/Users/maayanlab/OneDrive/singlecell/mouse_matrix.h5', 'r')
res = f["data/expression"]
f2 = h5py.File("data/test_real.h5", "w")
dset = f2.create_dataset("c1", res.shape, dtype='i', chunks=(1, 200), compression="gzip")
sp = dset.shape
f2.close()
f.close()

chunk = int(math.floor(sp[0]/50))
f = h5py.File('/Users/maayanlab/OneDrive/singlecell/mouse_matrix.h5', 'r')
f2 = h5py.File("data/test_real.h5", "a")
dset = f2["c1"]
for i in range(0,51):
    print(i)
    start = i*chunk
    to = min((i+1)*chunk, sp[0])
    dset[start:to, :] = f["data/expression"][start:to, :]

f2.close()
f.close()



