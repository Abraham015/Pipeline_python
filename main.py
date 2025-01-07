import argparse
import os.path
from typing import Tuple

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

def sanitizar_palabra(palabra)->str:
    quitar=[',', '.', '-', ':', ' ', "'", '"']
    for simbolo in quitar:
        palabra=palabra.replace(simbolo, "")

    palabra=palabra.lower()
    palabra=palabra.replace("á","a")
    palabra=palabra.replace("é","e")
    palabra=palabra.replace("í","i")
    palabra=palabra.replace("ó","o")
    palabra=palabra.replace("ú","u")
    return palabra

def main():
    parser=argparse.ArgumentParser(description="Nuestro primer pipeline")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")
    parser.add_argument("--n-palabras", type=int, help="Número de palabras en la salida")
    our_args, beam_args=parser.parse_known_args()
    run_pipeline(our_args, beam_args)

def run_pipeline(custom_args, beam_args):
    entrada=custom_args.entrada
    salida=custom_args.salida
    n_palabras=custom_args.n_palabras
    opts=PipelineOptions(beam_args)
    with beam.Pipeline(options=opts) as p:
        #Se va a leer todo el archivo a procesar
        lineas: PCollection[str]= p | beam.io.ReadFromText(entrada)
        #"En un lugar de la mancha"->["En", "un",..]-> "En", "un"...
        #Si se usa beam.Map se crean arrays de las palabras
        palabras=lineas | beam.FlatMap(lambda l: l.split())
        limpiadas=palabras | beam.Map(sanitizar_palabra)
        #Esta función nos va a regresar una tupla con el contador y la palabra
        contadas: PCollection[Tuple[str, int]]= limpiadas | beam.combiners.Count.PerElement()
        palabras_top_lista=contadas | beam.combiners.Top.Of(n_palabras, key=lambda kv:kv[1])
        palabras_top=palabras_top_lista | beam.FlatMap(lambda x: x)
        formateado: PCollection[str]=palabras_top | beam.Map(lambda kv: "%s, %d" % (kv[0], kv[1]))
        #formateado | beam.Map(print)
        if os.path.exists(salida):
            os.remove(salida)
        formateado | beam.io.WriteToText(salida)

if __name__=='__main__':
    main()