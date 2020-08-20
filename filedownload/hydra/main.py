from datetime import date
import tornado.escape
import tornado.ioloop
import tornado.web
import h5py
import numpy as np
import os
import zipfile
import io
import csv
import pandas as pd
import string
import random
import urllib
import requests
import StringIO
from threading import Thread

root = os.path.dirname(__file__)

print(root)

print("Downloading expression")
testfile1 = urllib.URLopener()
testfile1.retrieve("http://s3.amazonaws.com/mssm-seq-matrix/mouse_matrix.h5", "data/mouse_matrix.h5")

testfile2 = urllib.URLopener()
testfile2.retrieve("http://s3.amazonaws.com/mssm-seq-matrix/human_matrix.h5", "data/human_matrix.h5")
print("Download complete")

def extract_zip(input_zip):
    input_zip = zipfile.ZipFile(input_zip)
    return input_zip.read("gene_count.tsv")

def getData(index, species, searchterm, search_samples, results):
    zip_file_url = 'http://amp.pharm.mssm.edu/hydrahead/data'
    
    jdata = {
        'species': species,
        'searchterm': searchterm,
        'search_samples': search_samples
    }
    
    headers = {'Content-type': 'application/json'}
    r = requests.post(zip_file_url, json=jdata, headers=headers)
    
    df = pd.read_csv(StringIO.StringIO(r.content), sep="\t")
    df.drop(df.columns[[0]], axis=1, inplace=True)
    results[index] = df
    return True

def chunkIt(seq, num):
    if len(seq) < num:
        return([seq])
    else:
        avg = len(seq) / float(num)
        out = []
        last = 0.0
        
        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg
        
        return out

def sendZip(species, searchterm, search_samples, self):
    
    randomString = ''.join([random.choice(string.ascii_letters + string.digits) for n in xrange(32)])
    
    file = species+"_matrix.h5"
    f = h5py.File("data/"+file, 'r+')
    
    genes = f['meta']['genes'][()]
    genes = [ x.decode("utf-8") for x in genes ]
    samples = f['meta']['Sample_geo_accession'][()]
    samples = [ x.decode("utf-8") for x in samples ]
    
    ind_dict = dict((k,i) for i,k in enumerate(samples))
    inter = set(ind_dict).intersection(search_samples)
    idx = sorted([ ind_dict[x] for x in inter ])
    
    if len(idx) > 3000:
        idx = idx[1:3000]
    
    sorted_samples = [ samples[i] for i in idx ]
    sample_chunks = chunkIt(sorted_samples, 4)
    
    threads = []
    result = {}
    
    for ii in range(0,len(sample_chunks)):
        process = Thread(target=getData, args=[ii, species, searchterm, sample_chunks[ii], result])
        process.start()
        threads.append(process)
    
    for process in threads:
        process.join()
    
    expr = pd.concat(result, axis=1)
    sampleheader = [el[1] for el in list(expr)]
    
    expr.columns = sampleheader
    expr.index = genes
    
    file_name = species+"_"+searchterm+".tsv"
    with zipfile.ZipFile(file_name, 'w') as zf:
        fout = StringIO.StringIO()
        expr.to_csv(fout, sep="\t", index=True, header=True)
        zf.writestr("gene_count.tsv", fout.getvalue(), compress_type=zipfile.ZIP_DEFLATED)
    
    # zf = zipfile.ZipFile(file_name, "w")
    # zf.write(randomString+"_pandaout.tsv", "gene_count.tsv", compress_type=zipfile.ZIP_DEFLATED)
    # zf.close()
    
    buf_size = 4096
    self.set_header('Content-Type', 'application/octet-stream')
    self.set_header('Content-Disposition', 'attachment; filename=' + file_name)
    with open(file_name, 'rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            self.write(data)
    self.finish()
    os.remove(file_name)
    os.remove(randomString+"_pandaout.tsv")

class VersionHandler(tornado.web.RequestHandler):
    def get(self):
        response = { 'version': '1',
                     'last_build':  date.today().isoformat() }
        self.write(response)

class DataHandler(tornado.web.RequestHandler):
    def post(self):
        data = tornado.escape.json_decode(self.request.body)
        sendZip(data["species"], data["searchterm"], data["search_samples"], self)
    
    def get(self):
        search_samples = ["GSM678413","GSM678414","GSM741170","GSM741171","GSM741172","GSM742937","GSM742938"]
        sendZip("human", "brain", search_samples, self)


application = tornado.web.Application([
    (r"/hydra/version", VersionHandler),
    (r"/hydra/data", DataHandler),
    (r"/hydra/(.*)", tornado.web.StaticFileHandler, dict(path=root))
])

if __name__ == "__main__":
    application.listen(5000)
    tornado.ioloop.IOLoop.instance().start()




