import weaviate
from weaviate.classes.config import Property, DataType
import os

client = weaviate.Client('http://localhost:8080')

schema = {
   "classes": [
       {
           "class": "X",
           "description": "Images of frames of Analyticx",
           "moduleConfig": {
               "img2vec-neural": {
                   "imageFields": [
                       "image"
                   ]
               }
           },
           "vectorIndexType": "hnsw", # https://www.pinecone.io/learn/series/faiss/hnsw/
           "vectorizer": "img2vec-neural", # the img2vec-neural Weaviate vectorizer
           "properties": [
               {
                   "name": "image",
                   "dataType": ["blob"],
                   "description": "image",
               },
               {
                   "name": "filepath",
                   "dataType":["string"],
                   "description": "Minio filepath of the images", 
               },
           ]
       }
   ]
}

client.schema.create(schema)
