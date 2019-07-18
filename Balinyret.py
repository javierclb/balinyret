#!/usr/bin/env python
# coding: utf-8
#!/usr/bin/env python

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkFiles
from extract import load
from datetime import datetime, timedelta
from sys import argv
from pyspark.sql import SparkSession
from argparse import ArgumentParser
from PLP_Output import saveToHive

def cargar(anio,mes,hdfs_path):
    lista1=["MEDIDAS","BASE","BASE_LT"]
    lista2=["CMG"]
    load(hdfs_path, "Balance.accdb",lista1)
    load(hdfs_path,"cmg"+anio[-2:]+mes+".accdb",lista2)

def main(spark,anio,mes,path,sep="/",hivemode="new"):
    """Realizamos la carga de datos, como inputs se tienen 4 csv's que se sacan del archivo access otorgado por coordinador,
    se extraen de manera automática(como se indica más arriba) y se almacenan directamente en machicura, en este mismo directorio se debe poseer
    los archivos zonas(que principalmente no cambiará mucho de periodo a otro), prorratas(que se deben calcular para cada periodo)
    fechas(en la cual debe ir el bloque, hp, horadia, tipodia, etc) y por último el archivo CLIENTES_ldx que se actualizan según
    se realice un cambio""" 
    hdfs_path=path+"/"+anio+mes	
    
    BASEPRE = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/BASE.csv')
    BASELTPRE = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/BASE_LT.csv')
    ZONAS = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/Zonas.csv')
    PRORRATAS = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/Prorratas.csv')
    FECHAS = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/Fechas.csv')
    CLIENTESLDX = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/Baseclientesldx.csv')
    CMGPRE = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/CMG.csv')
    MEDIDASPRE = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/MEDIDAS.csv')
    CMGDOLPRE = spark.read.options(header=True,ignoreLeadingWhiteSpace=True,inferSchema=True,).csv(hdfs_path+'/cmgdol.csv')

    #Manejamos los datos 
    #Creamos keepbasepre para las tablas que necesitamos de BASEPRE, en total esta tabla cuenta con 20 columnas, sacamos solamente
    #las necesarias, luego de realizar la selección, cambiamos nombre, determinado por la data histórica subida con 
    #anterioridad.
    KEEPBASEPRE = [BASEPRE.clave, BASEPRE.propietario, BASEPRE.descripcion, BASEPRE.nombre_barra, BASEPRE.tipo1, BASEPRE.clave_LT, BASEPRE.subsistema]
    BASEPRE = BASEPRE.select(*KEEPBASEPRE)
    #Renombramos los tablas según corresponda, para mantener la estructura de siempre, por ejemplo, en la base el campo 'barra' se encuentra
    #como 'nombre_barra'.
    BASEPRE = BASEPRE.withColumnRenamed("nombre_barra","barra").withColumnRenamed("tipo1","tipo").withColumnRenamed("clave_LT","linea")
    
    #Realizamos manejo de datos en ZONAS, seleccionando campos requeridos.
    KEEPZONAS = [ZONAS.barra_fact, ZONAS.zona, ZONAS.tension, ZONAS.macrozona, ZONAS.region, ZONAS.macrozona2]
    ZONAS = ZONAS.select(*KEEPZONAS)
    ZONAS = ZONAS.withColumnRenamed("BARRA_FACT", "barra").withColumnRenamed("zona","barra_troncal").withColumnRenamed("macrozona","sistema").withColumnRenamed("macrozona2","macrozona")
    #ES MOMENTO DE CHEQUEAR SI HAY BARRAS SIN ASIGNACIÓN DE INFORMACIÓN EN ZONAS.CSV

    #Muestra las barras que no tienen especificación en el .csv Zonas, de manera tal de que antes de agregar info, se tenga presente\
    #inmediatamente cuales son las barras a las cuales les falta especificación de Sistema, región, barra troncal, etc.
    BARRASVACIAS = BASEPRE.join(ZONAS, BASEPRE.barra == ZONAS.barra, "left").drop(ZONAS.barra)

    
    #Seleccionamos los campos necesarios de la tabla de costos marginales.
    KEEPCMGPRE = [CMGPRE.nombre_barra, CMGPRE.CMG_PESO_KWH, CMGPRE.Hora_Mensual]
    CMGPRE = CMGPRE.select(*KEEPCMGPRE)
    CMGPRE = CMGPRE.withColumnRenamed("nombre_barra","barra").withColumnRenamed("CMG_PESO_KWH","cmg_pesos")                   .withColumnRenamed("Hora_Mensual","hora")

    #Seleccionamos las columnas necesarias de la tabla MEDIDAS, asignando nombre determinado por la data subida con anterioridad.
    KEEPMEDIDAS = [MEDIDASPRE.clave, MEDIDASPRE.Hora_Mensual, MEDIDASPRE.MedidaHoraria2, MEDIDASPRE.MedidaHoraria]
    MEDIDASPRE = MEDIDASPRE.select(*KEEPMEDIDAS)
    MEDIDASPRE = MEDIDASPRE.withColumnRenamed("Hora_Mensual","hora").withColumnRenamed("MedidaHoraria2","medida1").             withColumnRenamed("MedidaHoraria","medida2")


    #Realizamos el primer join, teniendo como campo en común clave, con esto, cada clave queda con sus medidas horarias asociadas.
    BASE = BASEPRE.join(MEDIDASPRE, BASEPRE.clave == MEDIDASPRE.clave, "inner").drop(MEDIDASPRE.clave)

    #Realizamos el join correspondiente, teniendo el costo marginal para cada barra de manera horaria.
    BASE = BASE.join(CMGPRE,(BASE.barra==CMGPRE.barra)&(BASE.hora==CMGPRE.hora),"inner").drop(CMGPRE.barra).drop(CMGPRE.hora)
    #Armamos la tabla valorizado con el costo marginal y la medida2
    BASE = BASE.withColumn("valorizado",col("cmg_pesos")*col("medida2"))


    #Realizamos manejo de datos en BASE_LT, de manera que sacamos lo que necesitamos.
    KEEPBASELTPRE = [BASELTPRE.nombre_barra_ini, BASELTPRE.nombre_barra_fin, BASELTPRE.clave_LT]
    BASELTPRE = BASELTPRE.select(*KEEPBASELTPRE)
    BASELTPRE = BASELTPRE.withColumnRenamed("clave_LT", "linea").withColumnRenamed("nombre_barra_ini","barra_ini").withColumnRenamed("nombre_barra_fin","barra_fin")
    
    #Realizamos el join pertinente entre la BASE que ya contiene cierta cantidad de campos y BASELTPRE, teniendo como campo
    #en común "linea".
    BASE = BASE.join(BASELTPRE,BASE.linea==BASELTPRE.linea,"left").drop(BASELTPRE.linea)
    #El campo linea viene con un prefijo en donde se asigna la zona, en la data de los años 2011-2018 este campo es un 'int',
    #por ende, se extrae del string el prefijo de la zona y se cambia a un numero entero el campo línea.
    BASE = BASE.withColumn("linea", regexp_extract("linea", "(\d+)" , 1 ))
    
    #Realizamos el join de las ZONAS con la base, de manera de dejar con más completitud la información obtenida.
    BASE=BASE.join(ZONAS,BASE.barra==ZONAS.barra,"left").drop(ZONAS.barra)

    #Agregamos las descripciones a las fechas que se poseen.
    BASE=BASE.join(FECHAS,BASE.hora==FECHAS.HORA,"left").drop(FECHAS.HORA)

    BASE=BASE.join(CMGDOLPRE,(BASE.DIA==CMGDOLPRE.dia)&(BASE.HORADIA==CMGDOLPRE.horadia)&(BASE.barra==CMGDOLPRE.barra),"left").        drop(CMGDOLPRE.horadia).drop(CMGDOLPRE.dia).drop(CMGDOLPRE.barra)

    #Agregamos clientesldx a la tabla también.
    BASE=BASE.join(CLIENTESLDX,BASE.clave==CLIENTESLDX.clave,"left").drop(CLIENTESLDX.clave)
    #Realizamos manejo de datos en PRORRATAS
    KEEPPRORRATAS = [PRORRATAS.SUMINISTRADOR, PRORRATAS.DISTRIBUIDORA, PRORRATAS.PRORRATA]
    PRORRATAS = PRORRATAS.select(*KEEPPRORRATAS)
    #Se realiza el cambio suministrador empresa, dejando ya fijo el campo empresa para los suministradores que se encuentren en el
    #csv correspondiente
    PRORRATAS = PRORRATAS.withColumnRenamed("SUMINISTRADOR", "empresa")

    #Se trabaja el csv de prorratas, de manera de sacar el signo '%' que se encuentra en este, de manera de dejarlo como un double
    #y realizar la multiplicación correspondiente después.
    PRORRATAS = PRORRATAS.withColumn("PRORRATA", split(PRORRATAS['PRORRATA'], "\%" )[0])
    PRORRATAS = PRORRATAS.withColumn("PRORRATA", PRORRATAS["PRORRATA"].cast(DoubleType()))


    #Se saca el formato porcentaje
    PRORRATAS = PRORRATAS.withColumn("PRORRATA", col('PRORRATA')*0.01)


    #Se realiza el join correspondiente, dejando las prorratas ya asociadas a su clave
    BASE=BASE.join(PRORRATAS,BASE.propietario==PRORRATAS.DISTRIBUIDORA,"leftouter").drop(PRORRATAS.DISTRIBUIDORA)


    #Dejamos como empresa al propietario en cuestión para cada celda que no tenga empresa definida desde antes.
    BASE =BASE.withColumn('empresa',when(BASE.empresa.isNull(),BASE.propietario).otherwise(BASE.empresa))

    #Es necesario rellenar todos los campos Null en el campo 'prorrata', estos se rellenan con el valor de 1.0.
    BASE=BASE.fillna({'PRORRATA':'1'})


    #Agregamos 2 campos nuevos, FISICO2 y VALORIZADO2, que son las multiplicaciones pertinentes por la prorrata y valorizado/medida2
    BASE=BASE.withColumn("fisico2",col("medida2")*col("PRORRATA"))
    BASE=BASE.withColumn("valorizado2",col("valorizado")*col("PRORRATA"))

    #Identificamos como campo null a todas esas filas que no tienen empresa
    BASE =BASE.withColumn('empresa',when((BASE.fisico2==0)&(BASE.valorizado2==0)&(BASE.medida1!=0),None).otherwise(BASE.empresa))

    #Cambiamos el tipo de linea, ya que estaba con objeto string con anterioridad, se necesita como tipo int ya que es el tipo
    #que se tiene en la base total.
    BASE = BASE.withColumn("linea", BASE["linea"].cast(IntegerType()))

    #Cambiamos el tipo fecha que se encontraba como tipo str a timestamp, adjuntamos formato.
    #
    formato="dd/MM/yyyy"

    BASE = BASE.withColumn("fecha", (unix_timestamp("fecha",formato)+(col("horadia")-4)*3600).cast("timestamp"))
    #BASE1 = BASE1.withColumn("fecha1", unix_timestamp("fecha",formato).cast("timestamp"))

    #Sacamos de la base a las claves que no tengan empresa (esto producido para filas que no posean valorizado o fisicos)
    BASEFINAL = BASE.filter(BASE.empresa.isNotNull())

    #COMANDO REFERIDO A BORRAR UNA TABLA EN HIVE
    str_mes1 = 'drop table '+'balinyret.balance'+anio+mes

    #STRING RELACIONADO A AGREGAR UNA TABLA EN HIVE
    str_mes2 = 'balance'+anio[-2:]+mes
    #BASEFINAL.write.format("parquet").mode('append').partitionBy("ANIO","MES").saveAsTable(str_mes2)
    saveToHive(BASEFINAL,str_mes2,"balinyret",spark,hivemode=hivemode)

if __name__=='__main__':
    """ejecucion via spark submit de la forma spark2-submit balinyret.py --mes=mm --anio=aaaa path=directorio_hdfs"""  
    hive_path="hdfs:///user/hive/warehouse"
    parser=ArgumentParser()
    parser.add_argument('--mes',help="Ingresar mes balance",type=str,required=True)
    parser.add_argument('--anio',help="Ingresar anio balance",type=str,required=True)
    parser.add_argument('--path',help="Ingresar directorio archivos",type=str,required=True)
    parser.add_argument('--path',help="Ingresar directorio archivos",type=str,required=True)
    parser.add_argument('--hivemode', type=str,
                        default='datasets',
                        help='Indica si la tabla es nueva("new"), se sobrescribre de existente ("overwrite") o se agregan ("append"). (default: %(default)s)')
    args=parser.parse_args()
    spark=SparkSession.builder.appName('Balinyret').master('yarn')\
                      .enableHiveSupport().config("hive.metastore.warehouse.dir", hive_path)\
		      .config("hive.exec.dynamic.partition", "true")\
		      .getOrCreate()

    anio=args.anio    
    mes=args.mes
    path=args.path
    hivemode=args.hivemode
    spark.sparkContext.setLogLevel("ERROR")
    main(spark,anio,mes,path,hivemode)
    spark.stop()

