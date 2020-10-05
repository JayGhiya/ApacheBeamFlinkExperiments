import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.external.kafka import ReadFromKafka, WriteToKafka

class Hl7ToFhirTransform(beam.DoFn):
    def process(self,element):
        #print("element:",element)
        yield element

def main():



    options = PipelineOptions(["--runner=PortableRunner","--job_endpoint=localhost:8099"])
    with beam.Pipeline(options=options) as p:
        dataFromKafka = p | ReadFromKafka(consumer_config={"bootstrap.servers":'192.168.43.128:19092'},topics=['testData'])
        dataFromKafka | beam.ParDo(Hl7ToFhirTransform())
    


if __name__=="__main__":
    main()    

