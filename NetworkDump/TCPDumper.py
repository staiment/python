#!/usr/bin/python
'''
Created on 4 ago. 2018

This script is shared under the Creative Commons BY license.

@author: David de Juan Calvo, alias Staiment 

Ensure 
ip link set eth0 promisc on
'''

import argparse
import threading
import subprocess as sub
import time
import signal
import gzip
import shutil
import os
import sys
import glob
from collections import deque


MODE_FILES = "files"
MODE_TIME = "days"

verbose_level = None
args = None
file=None
process=None
counter=0
queue = deque()
ex=False
stop=False
stoped=False
current=None

def verboseprint(level, *args2):         
    # Print each argument separately so caller doesn't need to
    # stuff everything to be printed into a single string
    if(level <= verbose_level):
        for val in args2:
            print (val);
            print()

def compressFile():
    global stop
    global stoped
    global current
    global file
    global args
    global counter
    stop=True
    while(not stoped):
        time.sleep(.2)
    file.close
    old=current
    if (counter==9999999999):
        counter=0
        verboseprint(1, "Maxint reached, setting to counter=0")
    else:
        counter+=1
    current=args.output+"/"+args.prefix+ format(counter,'09d')
    file = open(current, 'w')
    stop=False
    with open(old, 'rb') as f_in:
        with gzip.open(old+".gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(old)

def processSignal(signum, frame):
    verboseprint(1, "Processing gentle kill")
    compressFile()
    process.kill()
    

def timeWatcher():
    verboseprint(1, "Files days life watcher function selected ")
    global args
    while(True):
        current_time = time.time()
        files = filter(os.path.isfile,glob.glob(args.output+"/"+args.prefix+"*.gz"))
        if(len(files)>0):
            maxfiles=len(files)
            verboseprint(2, "Len files to remove: "+str(maxfiles))
            for x in range  (0, len(files)):
                modification_time=os.path.getmtime(files[x])
                if (current_time - modification_time) // (24*3600) >= args.days:
                #if (current_time - modification_time) // (60) >= args.days:
                    toDelete=files[x]
                    verboseprint(1, "Deleting file: "+toDelete)
                    os.remove(toDelete)
        time.sleep(2)

def filesWatcher():
    global args
    verboseprint(1, "Number of files watcher function selected ")
    while(True):
        files = filter(os.path.isfile,glob.glob(args.output+"/"+args.prefix+"*.gz"))
        files.sort(key=lambda x: os.path.getmtime(x))
        if(len(files)>args.count):
            numBorrar=len(files)>args.count
            verboseprint(1, "Deleting file: ")
            for x in (0, numBorrar):
                toDelete=files[x]
                verboseprint(1, "Deleting file: "+toDelete)
                os.remove(toDelete)
        time.sleep(2)
        

def nullFunction():
    verboseprint(1, "Null watcher function selected ")
    
def sizeWatcher():
    global current
    global args
    verboseprint(1, "Starting size watcher ")
    while(True):
        size=os.stat(current).st_size
        size/=(1024.0*1024.0)
        if(size>=args.size):
            verboseprint(3, "Size reached: compress ")
            compressFile()
        else:
            time.sleep(.3)

def initFileWatcher():
    verboseprint(3, "Choosing mode")
    if(args.mode == MODE_FILES):
        verboseprint(1, "Mode files")
        worker=filesWatcher;
    elif (args.mode == MODE_TIME):
        verboseprint(1, "Mode time")
        worker=timeWatcher;
    else:
        worker=nullFunction;
    t = threading.Thread(target=worker)
    t.setDaemon(True)
    t.start()
    
def initTcpDump():
    verboseprint(3,"Init tcpdump for "+args.adapter)
    global queue  
    global process
    process = sub.Popen(('tcpdump', '-i',args.adapter,'-nn', '-s0' ,'-v','-A','-XX','-t','-l'), stdout=sub.PIPE)
    for row in iter(process.stdout.readline, b''):
        pack=row.rstrip()
        verboseprint(3,"Read: "+pack)
        queue.append(pack )

def parseParams():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a","--adapter", default="eth0", help="Network adapter to listen to")
    parser.add_argument("-o", "--output", default=".", help="Output directory")
    parser.add_argument("-p", "--prefix", default="dump_", help="Files prefix")
    parser.add_argument("-s", "--size", type=int, default="1024", help="Output file size maximum")
    parser.add_argument("-m", "--mode", default="files", help="Mode limit files or days")
    parser.add_argument("-d", "--days", type=int, default="30", help="Days to keep log")
    parser.add_argument("-c", "--count", type=int, default="10", help="Number of files")
    parser.add_argument("-v", "--verbose", action="count", help="Verbose")
    
    global args
    args = parser.parse_args()
    global verbose_level
    verbose_level = args.verbose;
    return args

def initWriter():
    global file
    global args
    global counter
    global queue
    global ex
    global stop
    global stoped
    global current
    current=args.output+"/"+args.prefix+ format(counter,'09d')
    file = open(current, 'w')
    lines=1
    sizeDaemon=threading.Thread(target=sizeWatcher)
    sizeDaemon.setDaemon(True)
    sizeDaemon.start()
    while(not ex):
        if(stop):
            stoped=True
            while(stop):
                time.sleep(0.3)
            stoped=False
        try:
            str=queue.popleft()
            file.write(str+'\n') 
            if(lines % 100==0):
                lines=1
                file.flush()
        except IndexError:
            verboseprint(3,"Queue empty, waiting 300ms")
            time.sleep(0.3)

    file.close() 
    
def initWriterTh():
    t = threading.Thread(target=initWriter)
    t.setDaemon(True)
    t.start()

def start():
    signal.signal(signal.SIGTERM, processSignal)
    parseParams();
    initFileWatcher();
    initWriterTh();
    initTcpDump();


if __name__ == '__main__':
    start()
