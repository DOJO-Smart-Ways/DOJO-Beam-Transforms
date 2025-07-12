from enum import Enum

class BeamRunner(Enum):
    DIRECT = "DirectRunner"  # Execução local e sem distribuição
    DATAFLOW = "DataflowRunner"  # Execução no Google Cloud Dataflow
    SPARK = "SparkRunner"  # Execução no Apache Spark
    FLINK = "FlinkRunner"  # Execução no Apache Flink
    PORTABILITY = "PortableRunner"  # Execução portátil
    SAMZA = "SamzaRunner"  # Execução no Apache Samza
    TEST = "TestRunner"  # Runner de teste para fins de desenvolvimento

    @staticmethod
    def validate(runner):
        """Valida se o runner é um valor do Enum"""
        if not isinstance(runner, BeamRunner):
            raise ValueError(f"O valor {runner} não é válido. Deve ser um dos: {', '.join([r.value for r in BeamRunner])}.")
        return runner