from __future__ import annotations

import json
from functools import lru_cache

from jsonschema import Draft7Validator, ValidationError


@lru_cache(maxsize=128)
def load_validator(raw_schema: str) -> Draft7Validator:
    schema = json.loads(raw_schema)
    return Draft7Validator(schema)


def validate_payload(payload: dict, raw_schema: str):
    validator = load_validator(raw_schema)
    errors = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if errors:
        raise ValidationError("; ".join(error.message for error in errors))
