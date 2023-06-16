#!/usr/bin/env python
#_*_ codig: utf8 _*_
import datetime, json, boto3, datetime, sys, traceback
from boto3.s3.transfer import S3Transfer, TransferConfig
from Modules.constants import * #Se importan las constantes definidas en el archivo constants.py
from Modules.functions import *

if __name__ == '__main__':
    try:
        date_log=str(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
        objects={'Objects':[]} #Se inicializa la variable como un diccionario vacio.
        list_objects=[] #Se inicializa la variable como una lista vacia.
        aws_session=boto3.Session(profile_name=aws_profile) #Se crea una sesion de AWS bajo el perfil definido en la constante aws_profile.
        s3_client=aws_session.client('s3') #Se establece conexion con el servicio S3.
        logs=s3_client.list_objects_v2(Bucket=Bucket_logs, MaxKeys=10000) #se listan todos los objetos (archivos) en el Bucket definido en la constante Bucket_logs de S3.
        if 'Contents' in logs: #Se comprueba si hay objetos en el bucket.   
            for i in range(len(logs['Contents'])): #Si hay objetos se itera sobre cada objeto.
                log_Key=logs['Contents'][i]['Key'] #Captura de la ruta del objeto.
                S3Transfer(s3_client, TransferConfig(max_bandwidth=5000000)).download_file(Bucket_logs,log_Key,f'{src_Path}{log_Key}')
                objects['Objects'].append({'Key': log_Key,}) #Se agrega al dicionario el nombre del objeto descargado.
                list_objects.append(f"{src_Path}{log_Key}") #Se agrega la ruta completa del objeto descargado en el folder local.
                #Se realiza un a copia del objeto en el bucket definido en la constante Bucket_logs_old.
                s3_client.copy_object( 
                    Bucket=Bucket_logs_old, #Se establece el bucket de destino.
                    CopySource=f'{Bucket_logs}/{log_Key}', #Se establece la ruta del objeto en el bucket de origen.
                    Key=f'{log_Key}' #Se establece el nombre del objeto en el destino.
                    )
            #Se eliminan los objetos en el bucket de origen.    
            s3_client.delete_objects(
                    Bucket=Bucket_logs, #Se establce el bucket.
                    Delete=objects #Se establce la lista de objetos a eliminar.
                )
            objects_str=json.dumps(objects, sort_keys=True, indent=4) #Se transforma el diccionario a formato texto.
            print(objects)
            print_log(objects, date_log) #Se registra en el log de eventos el resumen.
        else:
            text_print=f"Logs not found" #Se define el texto a registrar en el log de eventos.
            print_log(text_print,date_log) #Se registra el texto en el log de eventos.
    except:
        error=sys.exc_info()[2] #Captura del error generado por el sistema.
        errorinfo=traceback.format_tb(error)[0] #Cartura del detalle del error.
        dict_summary={
            'Error': str(sys.exc_info()[1]),
            'error_info': errorinfo
        }
        dict_summary_srt=json.dumps(dict_summary, sort_keys=True, indent=4) #Se transforma el diccionario a formato texto.
        print(dict_summary_srt)
        print_log(dict_summary_srt, date_log) #Se registra en el log de eventos el resumen.
        mail_subject='FAIL bytescdn PROD execution error' #Se establece el asunto del correo.
        SendMail(dict_summary_srt, mail_subject) #Se envia correo electronico.