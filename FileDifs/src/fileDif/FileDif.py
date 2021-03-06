'''
Created on 29 jul. 2017

@author: David de Juan Calvo
email: david.dejuancalvo@gmail.com
'''
import sys
import os.path
import hashlib
from pathlib import Path
from shutil import copyfile

BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

IN="IN"
OUT="OUT"
BASE_DIR="BASE_DIR"
FILE_BASE="FILE_BASE"
FILE_OUT="FILE_OUT"
COPY_DIST="COPY_DIST"

alreadyFiles= dict()

params = {
    IN:"input",
    OUT:"out",
    BASE_DIR:"base",
    FILE_BASE:"list.source",
    FILE_OUT:"out.list",
    COPY_DIST:"true"
} 

def readParams():
    for param in sys.argv:
        par=param.split("=")
        if(len(par)==2):
            params[par[0].upper()]=par[1]
    return
            
def sanitizeParams():
    ret=True
    inp=Path(params[IN])
    if inp.is_dir():
        out=Path(params[OUT])
        if out.is_dir():
            file=Path(params[FILE_BASE])
            if(not file.is_file()):
                base=Path(params[BASE_DIR])
                if not base.is_dir():
                    print ("Either \"base_dir\" or \"file_base\" must be provided\n")
                    ret=False
        else:
            print ("\"out\" var must be a folder\n ")
            ret=False
    else:
        print ("\"in\" var must be a folder\n ")
        ret=False
    
    return ret

def caculateHash(file):  
    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
            sha1.update(data)
    return md5.hexdigest(),sha1.hexdigest()

def saveListPath(fw, path):
    for file in path.iterdir():
        if file.is_dir():
            saveListPath(fw, file)
        else:
            hashes=caculateHash(str(file))
            print("Writing...."+str(file)+"\n")
            fw.write(hashes[0]+"\t"+hashes[1]+"\t"+str(file.absolute())+"\n")
    return

def loadListPath(listPath):
    fr=open(listPath,"r")
    fr.read
    for line in fr:
        hashes=line.split("\t")
        alreadyFiles[hashes[0]]=hashes[1]
    return

def loadFileList():
    listPath= Path(params[FILE_BASE])
    pathBase=Path(params[BASE_DIR])
    if not listPath.is_file():
        fw=open(listPath,"w")
        fw.write("SHA1\tMD5\tPATH\n")
        saveListPath(fw, pathBase)
        fw.close()
    loadListPath(listPath)
    return

def copyFile(origin,pathOut,sha1,md5,fw):
    fileIn=str(origin.absolute())
    fw.write(sha1+"\t"+md5+"\t"+fileIn+"\n")
    fileOut=pathOut+"/"+sha1+str(origin.suffix)
    print ("Copying... "+fileIn+"\n")
    copyfile(fileIn, fileOut)
    return

def recursiveInput(fw,pathFile,pathOut):
    for file in pathFile.iterdir():
        if file.is_dir():
            recursiveInput(fw, file,pathOut)
        else:
            hashes=caculateHash(str(file))
            print("Comparing..."+str(file)+"\n")
            exists=hashes[0] in alreadyFiles and hashes[1]==alreadyFiles[hashes[0]]
            copyDist=params[COPY_DIST]=="true"
            if ((exists and not copyDist) or (not exists and copyDist )):
                copyFile(file,pathOut,hashes[0],hashes[1],fw)
    return

def compareInputs():
    pathIN = Path(params[IN])
    fwResume=open(params[FILE_OUT],"w")
    fwResume.write("SHA1\tMD5\tPATH\n")
    folderOut=params[OUT]
    recursiveInput(fwResume, pathIN, folderOut)
    fwResume.close()
    return

def main(argv):
    readParams()
    if sanitizeParams():
        loadFileList()
        compareInputs()
        print ("Done...\n")
    else:
        print ("Compare two folders recursively\n")
        print ("Params:\n")
        print ("\"in=\" Input folder")
        print ("\"base_dir=\" Base folder to compare to")
        print ("\"file_base=\" File with base hashes to compare to, if it exists base_dir will not be used")
        print ("\"out=\" Base folder to save files to")
        print ("\"file_out=\" File to summarize the copied files")
        print ("\"copy_dist=\" true if the copy files are equals false otherwise")
    return

if __name__ == '__main__':
    main(sys.argv)