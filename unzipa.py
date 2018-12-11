# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 15:30:58 2018

@author: l.ikeda
"""

import json

import zipfile
import boto3
s3 = boto3.resource('s3')
lam = boto3.client('lambda')

def lambda_handler(event, context):
    for rec in event['Records']:
        bucket = rec['s3']['bucket']['name']
        item = rec['s3']['object']['key']
    
    name = item.split('/')[-1]
    
    # tmp tem 512 mb, tem que controlar tamanho do .zip e arquivos descompactados.
    s3.Bucket(bucket).download_file(item, '/tmp/'+name)

    with zipfile.ZipFile('/tmp/'+name,"r") as zip_ref:
        lst = zip_ref.infolist()
        num = len(lst)
        
        # procura numero bom de funcs lambda
        last = num*5 if num*5 < 600 else 600
        for i in range(2,100):
            # verificar se não vai dar timeout, 5s por imagem max 800s
            aux = num*5/i
            if aux < 600:
                if (1-(aux/last)) < 0.5:
                    break
                last = aux
        n_funcs = i-1
        num_imgs = num/n_funcs
        print('invocando {} funcoes lambda com {} imagens cada.'.format(n_funcs, int(num_imgs)))
        '''
        import random, string
        # talvez não pegue o ultimo elemento, ficar de olho...
        rnd_names = [(''.join(random.choices(string.ascii_letters + string.digits, k=8))+'/', lst[round(i*num_imgs):round((i+1)*num_imgs)]) for i in range(n_funcs)]
        
        print(len(rnd_names))
        
        for x in rnd_names:
            for img in x[1]:
                s3.Bucket(bucket).put_object(Bucket=bucket, Key=x[0]+img.filename, Body=zip_ref.read(img))
            payload = {'img_dir': x[0]}
            print('invocando...', payload['img_dir'])
            lam.invoke(FunctionName='Classifica_imagem_velsis', InvocationType='Event', Payload=json.dumps(payload))
        '''

    
