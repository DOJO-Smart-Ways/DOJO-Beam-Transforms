from enum import Enum

class DojoBeamTransformVersion(Enum):
    V1_0_0 = "1.0.0"
    V1_1_0 = "1.1.0"
    V2_0_0 = "2.0.0"

    @staticmethod
    def validate(version):
        """Valida se o version é um valor do Enum"""
        if not isinstance(version, DojoBeamTransformVersion):
            raise ValueError(f"O valor {version} não é válido. Deve ser um dos: {', '.join([r.value for r in DojoBeamTransformVersion])}.")
        return version