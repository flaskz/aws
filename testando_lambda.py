# -*- coding: utf-8 -*-
"""
Created on Tue Dec 11 15:16:56 2018

@author: l.ikeda
"""

import sys
from numbers import Number
from collections import Set, Mapping, deque    
zero_depth_bases = (str, bytes, Number, range, bytearray)
iteritems = 'items'
    
def getsize(obj_0):
    """Recursively iterate to sum size of object & members."""
    _seen_ids = set()
    def inner(obj):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0
        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)
        if isinstance(obj, zero_depth_bases):
            pass # bypass remaining control flow and return
        elif isinstance(obj, (tuple, list, Set, deque)):
            size += sum(inner(i) for i in obj)
        elif isinstance(obj, Mapping) or hasattr(obj, iteritems):
            size += sum(inner(k) + inner(v) for k, v in getattr(obj, iteritems)())
        # Check for custom object instances - may subclass above too
        if hasattr(obj, '__dict__'):
            size += inner(vars(obj))
        if hasattr(obj, '__slots__'): # can have __slots__ with __dict__
            size += sum(inner(getattr(obj, s)) for s in obj.__slots__ if hasattr(obj, s))
        return size
    return inner(obj_0)

import zipfile

with zipfile.ZipFile('test2.zip',"r") as zip_ref:
    lst = zip_ref.infolist()
    num = len(lst)
    
    '''
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
    '''
    
    # setar cada funcao pra max 100 imagens, da pra aumentar depois
    n_funcs = round(num/100)
    num_imgs = num/n_funcs
    print('invocando {} funcoes lambda com {} imagens cada.'.format(n_funcs, int(num_imgs)))
    
    lst_imgs = [lst[round(i*num_imgs):round((i+1)*num_imgs)] for i in range(n_funcs)]
    
    if sum([len(x) for x in lst_imgs]) != len(lst):
        print('algo errado aqui...')
        
    for x in lst_imgs:
        payload = {'lista_imagens': x}
        # print(getsize(payload))
        lam.invoke(FunctionName='Classifica_imagem_velsis', InvocationType='Event', Payload=json.dumps(payload))


    '''
    # desse jeito ele cria um arquivo novo pra cada arquivo do zip, e depois chama a função de
    # classificação, que vai ler o arquivo e depois excluir ele. Gasto desnecessário no s3...
    
    import random
    import string
    # talvez não pegue o ultimo elemento, ficar de olho...
    
    # python 3.5-
    rnd_names = [(''.join([random.choice(string.ascii_letters + string.digits) for x in range(8)])+'/', lst[round(i*num_imgs):round((i+1)*num_imgs)]) for i in range(n_funcs)]
    
    # python 3.6+
    rnd_names = [(''.join(random.choices(string.ascii_letters + string.digits, k=8))+'/', lst[round(i*num_imgs):round((i+1)*num_imgs)]) for i in range(n_funcs)]
    # print(len(rnd_names))
    
    import sys
    
    for x in rnd_names:
        print(sys.getsizeof(x[1]))
        for img in x[1]:
            s3.Bucket(bucket).put_object(Bucket=bucket, Key=x[0]+img.filename, Body=zip_ref.read(img))
        payload = {'img_dir': x[0]}
        print('invocando...', payload['img_dir'])
        lam.invoke(FunctionName='Classifica_imagem_velsis', InvocationType='Event', Payload=json.dumps(payload))
    '''

