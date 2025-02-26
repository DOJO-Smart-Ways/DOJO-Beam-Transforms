from enum import Enum

class DataflowMachineType(Enum):
    N1_STANDARD_1 = "n1-standard-1"  # 1 vCPU, 3.75 GB de memória
    N1_STANDARD_2 = "n1-standard-2"  # 2 vCPUs, 7.5 GB de memória
    N1_STANDARD_4 = "n1-standard-4"  # 4 vCPUs, 15 GB de memória
    N1_STANDARD_8 = "n1-standard-8"  # 8 vCPUs, 30 GB de memória
    N1_STANDARD_16 = "n1-standard-16"  # 16 vCPUs, 60 GB de memória
    N1_STANDARD_32 = "n1-standard-32"  # 32 vCPUs, 120 GB de memória
    N1_STANDARD_64 = "n1-standard-64"  # 64 vCPUs, 240 GB de memória
    N1_STANDARD_96 = "n1-standard-96"  # 96 vCPUs, 360 GB de memória
    
    N1_HIGHMEM_2 = "n1-highmem-2"    # 2 vCPUs, 13 GB de memória
    N1_HIGHCPU_4 = "n1-highcpu-4"    # 4 vCPUs, 3.6 GB de memória
    
    N2_STANDARD_2 = "n2-standard-2"  # 2 vCPUs, 8 GB de memória
    N2_HIGHCPU_4 = "n2-highcpu-4"    # 4 vCPUs, 4 GB de memória
    
    E2_STANDARD_4 = "e2-standard-4"  # 4 vCPUs, 16 GB de memória
    E2_HIGHMEM_2 = "e2-highmem-2"    # 2 vCPUs, 16 GB de memória

    CUSTOM = "custom"                # Tipo de máquina personalizada

    @staticmethod
    def validate(machine_type):
        """Valida se o DataflowMachineType é um valor do Enum"""
        if not isinstance(machine_type, DataflowMachineType):
            raise ValueError(f"O valor {machine_type} não é válido. Deve ser um dos: {', '.join([r.value for r in DataflowMachineType])}.")
        return machine_type