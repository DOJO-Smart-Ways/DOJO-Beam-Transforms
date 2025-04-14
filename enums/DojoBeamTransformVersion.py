from enum import Enum

class DojoBeamTransformVersion(Enum):
    V1_0_0 = "1.0.0"
    V1_1_0 = "1.1.0"
    V2_0_0 = "2.0.0"
    V3_0_0 = "3.0.0"

    @staticmethod
    def validate(version):
        """Check its a valid value"""
        if not isinstance(version, DojoBeamTransformVersion):
            raise ValueError(f"Value {version} is not valid. Should be one of: {', '.join([r.value for r in DojoBeamTransformVersion])}.")
        return version