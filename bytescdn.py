#!/usr/bin/env python
#_*_ codig: utf8 _*_
import sys, traceback, gzip, time, datetime, psycopg2, json, os, shutil, re, urllib.parse
from Modules.functions import *
from Modules.constants import *

if __name__ == '__main__':
    date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")) #Se recoge el dato de fecha y hora en el instante en que se ejecuta el codigo.
    if Flag_Status('r'): #Se consulta el estado de la bandera "FLAG" en el archivo json.    
    #Si el estado es true se ejecuta el proceso.
        try:
            beginning=time.time() #Captura del dato de hora y fecha
            logs_List=os.listdir(src_Path) #Se recoge la lista de los archivos logs descargados que entrega la funcion Download_Logs.
            postgresql=psycopg2.connect(data_base_connect) #Se establece la conexión a la base de datos PostgreSQL utilizando la información proporcionada en la variable data_base_connect. 
            curpsql=postgresql.cursor() #Se crea un cursor curpsql para ejecutar consultas en la base de datos.
            if logs_List !=[]:
                for file_Name in logs_List: #Se itera dentro de la lista de logs selecionando uno por uno.
                    sum_bytes=0
                    file_Name_Split=file_Name.split('.')
                    cdn_Id=file_Name_Split[0]
                    date_Log=f"{file_Name_Split[1]}.{file_Name_Split[2]}"
                    with gzip.open(f'{src_Path}{file_Name}', 'rt') as file: #Se abre el archivo para acceder a los datos.
                        for line in file: #Se itera sobre el archivo recogiendo los datos linea a linea.
                            quantity+=1 #Contador de lineas procesadas.
                            if not('#' in line): #Si en la linea selecionada se encuentra el caracter '#' se ignora.
                                columns=line.split('\t') #Se crea una lista con cada dato en la linea separado por tabulacion.
                                sum_bytes+=int(columns[3]) #Los datos de la lista columns en la posicion 8 se separan por el caracter '/' y se crea una lista con los datos separados.
                            else:
                                pass
                    sql_command="INSERT INTO cdnbytes VALUES(%s,%s,%s,%s,%s);"
                    sql_Data=(
                        file_Name,
                        str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")),
                        cdn_Id,
                        date_Log,
                        sum_bytes/1000000000
                    )                    
                    curpsql.execute(sql_command,sql_Data)
                    postgresql.commit()
                    finish=time.time() #Captura del tiempo en el instante que termina de procesar un log.
                    dict_log['Lines_processed']=str(quantity) #Se agrega al resumen la cantidad total de lineas procesadas.
                    dict_log['Total_Bytes']=sum_bytes #Se agrega al resumen la cantidad total de manifests registrados.
                    process_duration=round((finish-beginning),3)
                    list_durations.append(process_duration)
                    dict_log['Process_duration']=str(process_duration) #Se agrega al resumen el calculo de la duracion total de la ejecucion del codeigo.
                    os.remove(src_Path+file_Name) #Se elimnina el log procesado del folde local.
                    quantity=0
                    dict_summary[file_Name]=dict_log #Se agrega al resumen el nombre del log procesado.
                    dict_log={} #se limpia el diccionario.
                postgresql.close()
                dict_summary["Total_process_duration"]=str(round(sum(list(list_durations)),2))
                dict_summary_srt=json.dumps(dict_summary, sort_keys=True, indent=4) #Se transforma el diccionario a formato texto.
                print(dict_summary_srt)
                print_log(dict_summary_srt, date_log) #Se registra en el log de eventos el resumen.
            else:
                pass
        except:
            postgresql.close()
            finish=time.time() #Captura del tiempo en el instante que termina de procesar un log.
            Flag_Status("w") #Se cambia el estado de la bandera "FLAG" a false.
            error=sys.exc_info()[2] #Captura del error generado por el sistema.
            errorinfo=traceback.format_tb(error)[0] #Cartura del detalle del error.
            dict_log['Lines_processed']=str(quantity) #Se agrega al resumen la cantidad total de lineas procesadas.
            dict_log['Process_duration']=str(round((finish-beginning),2)) #Se agrega al resumen el calculo de la duracion total de la ejecucion del codigo.
            #Se agrega al resumen detalle del error ocurrido.
            dict_log['Log_Error']={
                'Error': str(sys.exc_info()[1]),
                'error_info': errorinfo
            }
            dict_summary[file_Name]=dict_log #Se agrega al resumen el nombre del log procesado.
            dict_summary["Total_process_duration"]=str(round(sum(list(list_durations)),2))
            dict_summary_srt=json.dumps(dict_summary, sort_keys=True, indent=4) #Se transforma el diccionario a formato texto.
            print(dict_summary_srt)
            print_log(dict_summary_srt, date_log) #Se registra en el log de eventos el resumen.
            mail_subject='FAIL bytescdn PROD execution error' #Se establece el asunto del correo.
            SendMail(dict_summary_srt, mail_subject) #Se envia correo electronico.
    else:
        text_print="bytescdn application failure not recognized\n" #Texto a imprimir o registrar.
        print_log(text_print, date_log) #Se registra el texto anterior en el log de eventos.
        quit() #Termina la ejecucion del codigo.
    