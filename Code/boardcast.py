#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 27 18:44:28 2017

@author: sunyitao
"""

import sys

import os

from mpi4py import MPI

import numpy as np

import pandas as pd


os.chdir('/Users/sunyitao/Desktop/shool file/STA 9794')
f=open('data-big.txt')

f.tell()
#  
#  Global variables for MPI  
#  
   
# instance for invoking MPI relatedfunctions  
comm = MPI.COMM_WORLD  
# the node rank in the whole community  
comm_rank = comm.Get_rank()  
# the size of the whole community, i.e.,the total number of working nodes in the MPI cluster  
comm_size = comm.Get_size()  
   
wt = MPI.Wtime()

if __name__ == '__main__':
    
   if comm_rank == 0:  
       sys.stderr.write("processor root starts reading data...\n")
       
       all_lines = f.readlines()
       
   all_lines = comm.bcast(all_lines if comm_rank == 0 else None, root = 0)  
   
   num_lines = len(all_lines)  
   
   local_lines_offset = np.linspace(0, num_lines, comm_size +1).astype('int')  
   
   local_lines = all_lines[local_lines_offset[comm_rank] :local_lines_offset[comm_rank + 1]]  
   
   sys.stderr.write("%d/%d processor gets %d/%d data \n" %(comm_rank, comm_size, len(local_lines), num_lines))
   
   sampledata=[]
   error=[]
   cnt = 0  
   for line in local_lines:  
       row=line.strip().split(",")
        
       datapoint={}
       try:
           datapoint['timestamp']=pd.datetime.strptime(row[0], '%Y%m%d:%H:%M:%S.%f')
           datapoint['price']=float(row[1])
           datapoint['units traded']=int(row[2])
       except:
           error.append(datapoint)

       #clean up abnormal datapoints
       if float(row[1]) <= 0 or int(row[2]) < 0:
            error.append(datapoint)
       elif float(row[1]) >= 5000:
            error.append(datapoint)
       else:   
            sampledata.append(datapoint)
        
       cnt += 1  

   sys.stderr.write("processor %d has processed %d/%d lines \n" %(comm_rank, cnt, len(local_lines)))


total = MPI.Wtime() - wt


sys.stderr.write("run time is %d \n"%(total))

