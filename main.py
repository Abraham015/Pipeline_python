import argparse
from typing import Tuple

import apache_beam as beam
from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

def main():
    parser=argparse.ArgumentParser(description="Nuestro primer pipeline")
    parser.add_argument("--entrada", help="Fichero de entrada")
    parser.add_argument("--salida", help="Fichero de salida")
    our_args, beam_args=parser.parse_known_args()
    run_pipeline(our_args, beam_args)

def run_pipeline(custom_args, beam_args):
    entrada=custom_args.entrada
    salida=custom_args.salida
    opts=PipelineOptions(beam_args)
    with beam.Pipeline(opts) as p:
        #Se va a leer todo el archivo a procesar
        lineas: PCollection[str]= p | beam.io.ReadFromText(entrada)
        #"En un lugar de la mancha"->["En", "un",..]-> "En", "un"...
        #Si se usa beam.Map se crean arrays de las palabras
        palabras=lineas | beam.FlatMap(lambda l: l.split())
        #Esta función nos va a regresar una tupla con el contador y la palabra
        contadas: PCollection[Tuple[str, int]]= palabras | beam.combiners.Count.PerElement()
        palabras_top=contadas | beam.combiners.Top.Of(5, keys=lambda kv:kv[1])
        palabras_top | beam.Map(print)

if __name__=='__main__':
    main()