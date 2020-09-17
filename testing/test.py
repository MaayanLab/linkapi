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



root = os.path.dirname(__file__)

print(root)

print("Downloading expression")
testfile1 = urllib.URLopener()
testfile1.retrieve("https://s3.amazonaws.com/mssm-seq-matrix/mouse_matrix.h5", "data/mouse_matrix.h5")

testfile2 = urllib.URLopener()
testfile2.retrieve("https://s3.amazonaws.com/mssm-seq-matrix/human_matrix.h5", "data/human_matrix.h5")
print("Download complete")

def sendZip(species, search_term, search_samples, self):
    
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
    
    if len(idx) > 5000:
        idx = idx[1:5000]
    
    refreg = f['data']['expression'].regionref[idx, :]
    expr = np.transpose(f['data']['expression'][refreg])
    sub_samples = [samples[i] for i in idx]
    
    df = pd.DataFrame(expr, index=genes, columns=sub_samples)
    df.to_csv(randomString+"_pandaout.tsv", sep="\t", index=True, header=True)
    
    file_name = species+"_"+search_term+".zip"
    file_name = randomString+"_pandaout.tsv"
    
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
        
        #search_samples = ["GSM678413","GSM678414","GSM741170","GSM741171","GSM741172","GSM742937","GSM742938"]
        sendZip(data["species"], data["searchterm"], data["search_samples"], self)
    
    def get(self):
        search_samples = ["GSM678413","GSM678414","GSM741170","GSM741171","GSM741172","GSM742937","GSM742938"]
        sendZip("human", "brain", search_samples, self)


application = tornado.web.Application([
    (r"/hydrahead/version", VersionHandler),
    (r"/hydrahead/data", DataHandler),
    (r"/hydrahead/(.*)", tornado.web.StaticFileHandler, dict(path=root))
])

if __name__ == "__main__":
    application.listen(5000)
    tornado.ioloop.IOLoop.instance().start()


