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
        """Check if  runner is valid Enum"""
        if not isinstance(runner, BeamRunner) and runner not in BeamRunner._value2member_map_:
            raise ValueError(f"The value of runner: {runner} is not valid. Should be one of: {', '.join([r.value for r in BeamRunner])}.")
        return runner