import numpy as np
import tensorflow as tf
import boto3

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')

input_height = 299
input_width = 299
input_mean = 0
input_std = 255
input_layer = "Placeholder"
output_layer = "final_result"

def load_graph(model_file):
  graph = tf.Graph()
  graph_def = tf.GraphDef()

  with open(model_file, "rb") as f:
    graph_def.ParseFromString(f.read())
  with graph.as_default():
    tf.import_graph_def(graph_def)

  return graph

def read_tensor_from_image_file(file_name,
                                input_height=299,
                                input_width=299,
                                input_mean=0,
                                input_std=255):
  input_name = "file_reader"
  output_name = "normalized"
  file_reader = tf.read_file(file_name, input_name)
  if file_name.endswith(".png"):
    image_reader = tf.image.decode_png(
        file_reader, channels=3, name="png_reader")
  elif file_name.endswith(".gif"):
    image_reader = tf.squeeze(
        tf.image.decode_gif(file_reader, name="gif_reader"))
  elif file_name.endswith(".bmp"):
    image_reader = tf.image.decode_bmp(file_reader, name="bmp_reader")
  else:
    image_reader = tf.image.decode_jpeg(
        file_reader, channels=3, name="jpeg_reader")
  float_caster = tf.cast(image_reader, tf.float32)
  dims_expander = tf.expand_dims(float_caster, 0)
  resized = tf.image.resize_bilinear(dims_expander, [input_height, input_width])
  normalized = tf.divide(tf.subtract(resized, [input_mean]), [input_std])
  sess = tf.Session()
  result = sess.run(normalized)

  return result

def load_labels(label_file):
  label = []
  proto_as_ascii_lines = tf.gfile.GFile(label_file).readlines()
  for l in proto_as_ascii_lines:
    label.append(l.rstrip())
  return label

def prediz(image_list, model_file, label_file):
  graph = load_graph(model_file)
  print('comecando predicao...')
  input_name = "import/" + input_layer
  output_name = "import/" + output_layer
  input_operation = graph.get_operation_by_name(input_name)
  output_operation = graph.get_operation_by_name(output_name)
  
  predicoes = []
  
  for k, v in image_list.items():
      with tf.Session(graph=graph) as sess:
        results = sess.run(output_operation.outputs[0], {
            input_operation.outputs[0]: v
        })
      results = np.squeeze(results)
    
      top_k = results.argsort()[-5:][::-1]
      labels = load_labels(label_file)
      '''
      for i in top_k:
        print(labels[i], results[i])
      '''
      print(k+': ', labels[top_k[0]], ', ', results[top_k[0]])
      predicoes.append((k, labels, results, top_k))
    
  return predicoes

def lambda_handler(event, context):
    import zipfile
    
    img_list = event['lista_imagens']
    zip_path = event['zip_name']
    zip_name = zip_path.split('/')[-1]
    bucket = 'lucasmodelcinq'
      
    s3.Bucket(bucket).download_file('model1_graph.pb', '/tmp/'+'model1_graph.pb')
    print('baixou modelo...')
    s3.Bucket(bucket).download_file('model1_labels.txt', '/tmp/'+'model1_labels.txt')
    print('baixou labels...')
    
    try:
        print('numero de files: ', len(img_list))
    except:
        print('lista vazia...')
        exit(0)
    
    try:
        s3.Bucket(bucket).download_file(zip_path, '/tmp/'+zip_name)
    except:
        print('nao conseguiu baixar zip.')
        exit(0)
    
    lst_imgs = {}
    
    try:        
        with zipfile.ZipFile('/tmp/'+zip_name, 'r') as zip_ref:
            for arq in img_list:
                name = arq.filename
                data = dynamodb_client.get_item(TableName='predicoes', Key={'nome_imagem': {'S': name}})
                if 'Item' not in data.keys():
                    lst_imgs[name] = (read_tensor_from_image_file(
                        zip_ref.read(arq),
                        input_height=input_height,
                        input_width=input_width,
                        input_mean=input_mean,
                        input_std=input_std))  
                else:
                    print('imagem ja processada')
    except:
        print('problema na leitura do zip')
        exit(0)
            
    if len(lst_imgs) > 0 :
        lst_preds = prediz(lst_imgs, '/tmp/'+'model1_graph.pb', '/tmp/'+'model1_labels.txt')
        print('terminou predicao...')
        for pred in lst_preds:
            aux = [{'M': {pred[1][i]: {'N': str(pred[2][i])}}} for i in pred[3]]
            item = {'nome_imagem': {'S': pred[0]}, 'percents': {'L': aux}}
            dynamodb_client.put_item(TableName='predicoes', Item=item)
            print('Terminou de gravar no DynamoDB...')
    print('Terminou Lambda...')
