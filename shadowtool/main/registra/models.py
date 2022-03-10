from typing import Dict, Optional, List, Union, Any
from pydantic import BaseModel, root_validator
from shadowtool.interfaces.base_model import BaseType


class PydanticBaseModelWithExtra(BaseModel):
    extra: Dict[str, Any]

    @root_validator(pre=True)
    def construct_extra(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        """
        accept the additional arguments passed into the model as a dictionary,
        accessible with the name `extra`
        """
        all_required_field_names = {
            field.alias for field in cls.__fields__.values() if field.alias != "extra"
        }

        extra: Dict[str, Any] = {}
        for field_name in list(values):
            if field_name not in all_required_field_names:
                extra[field_name] = values.pop(field_name)

        values["extra"] = extra
        return values


class BaseRegistraType(BaseType):

    """
    type of the Registra format

    One should use this as the base class to include other Registra Types
    """
    ...
