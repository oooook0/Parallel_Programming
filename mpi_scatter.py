#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 12:59:11 2017

@author: sunyitao
"""

import sys

import os

from mpi4py import MPI

import pandas as pd

import tempfile


os.chdir('/Users/sunyitao/Desktop/shool file/STA 9794')


#  Global variables for MPI  
#  
   
# instance for invoking MPI relatedfunctions  
comm = MPI.COMM_WORLD  
# the node rank in the whole community  
comm_rank = comm.Get_rank()  
# the size of the whole community, i.e.,the total number of working nodes in the MPI cluster  
comm_size = comm.Get_size()  
   
wt = MPI.Wtime()

mode = MPI.MODE_RDONLY

if comm_rank == 0:
    work = ['data-small.txt']
else:
    work = None
    
unit = comm.scatter(work, comm_size)

f= MPI.File.Open(comm,unit,mode)

ba = bytearray(f.Get_size())

f.Iread(ba)

f.Close()

descriptor, path = tempfile.mkstemp(suffix='mpi.txt')

print(path)

tf = os.fdopen(descriptor, 'w')

tf.write(ba)

tf.close()

contents = open(path, 'rU').read() + str(comm.Get_rank())

os.remove(path)

local_lines=contents.readlines()

sampledata=[]
error=[]
cnt = 0  

for line in local_lines:  
    
    row=line.strip().split(",")

    datapoint={}
    datapoint['timestamp']=pd.datetime.strptime(row[0], '%Y%m%d:%H:%M:%S.%f')
    datapoint['price']=float(row[1])
    datapoint['units traded']=int(row[2])

   #clean up abnormal datapoints
    if float(row[1]) <= 0 or int(row[2]) < 0:
        error.append(datapoint)
    elif float(row[1]) >= 5000:
        error.append(datapoint)
    else:   
        sampledata.append(datapoint)

    cnt += 1  

sys.stderr.write("processor %d has processed %d/%d lines \n" %(comm_rank, cnt, len(local_lines)))

if comm_rank ==0:
    final_data=comm.gather(sampledata,roots=0)
    final_error=comm.gather(error,roots=0)

    
   