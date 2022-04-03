import enum
import shadowtool.exceptions as exc


class BaseType(str, enum.Enum):
    @classmethod
    def _raise_error_unknown(cls, raw_value: str):
        raise exc.UnknownRegistraModelTypeError(
            model_name=cls.__name__, raw_type=raw_value
        )

    @classmethod
    def from_args(cls, raw_value: str):
        return cls[raw_value.upper()]


class DataLayer(BaseType):

    DUMP = "DUMP"
    RAW = "RAW"
    CLEAN = "CLEAN"
    APP = "APP"
