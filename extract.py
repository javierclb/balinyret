import sys, subprocess, os
from pyspark import SparkFiles
from pyspark import SparkContext
from EspProcess import run_cmd
from os import remove, getcwd, listdir

sc = SparkContext.getOrCreate()

def load(path,filename,lista):

    pdata=sc.addFile(path+'/'+filename)	
    data=SparkFiles.get(filename)
#    subprocess.call(["mdb-schema", data, "mysql"])
    table_names = subprocess.Popen(["mdb-tables", "-1", data], stdout=subprocess.PIPE).communicate()[0]
    tables = table_names.splitlines()
    print(tables_names)
    for table in tables:
        if table in lista:
           filename = table.replace(" ","_") + ".csv"
           file = open(filename, 'w')
  	   print("Tabla Creada " + table)
           contents = subprocess.Popen(["mdb-export", data, table],
                      stdout=subprocess.PIPE).communicate()[0]
           file.write(contents)
           file.close()
           (ret,out,err) = run_cmd(['hdfs','dfs','-put','-f',table+".csv",path ])
           if ret != 0:
                print("problemas to move BASE.csv to hdfs")
           remove(table+".csv") 
    return table_names
