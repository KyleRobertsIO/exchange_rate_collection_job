import sys
import os
from marshmallow import fields, Schema, ValidationError
from pprint import pprint

class LoggerConfig(Schema):
    log_level = fields.String(
        required = True,
        allow_none = False
    )
    name = fields.String(
        required = True,
        allow_none = False
    )

class PostgresConfig(Schema):
    host = fields.String(
        required = True,
        allow_none = False
    )
    port = fields.Integer(
        required = True
    )
    database = fields.String(
        required = True,
        allow_none = False
    )
    username = fields.String(
        required = True,
        allow_none = False
    )
    password = fields.String(
        required = True,
        allow_none = False
    )

class EnvironmentVarSchema(Schema):
    postgres = fields.Nested(PostgresConfig())
    logger = fields.Nested(LoggerConfig())

def _handle_schema_validation(raw_config: dict) -> dict:
    try:
        config: dict = EnvironmentVarSchema().load(raw_config)
        return config
    except ValidationError as err:
        print("Failed to collect environment variable configurations.")
        pprint(err.messages_dict)
        sys.exit()

def _build_raw_config() -> dict:
    return {
        "logger": {
            "log_level": os.getenv("LOGGER.LOG_LEVEL"),
            "name": os.getenv("LOGGER.NAME")
        },
        "postgres": {
            "host": os.getenv("POSTGRES.HOST"),
            "port": int(os.getenv("POSTGRES.PORT")),
            "database": os.getenv("POSTGRES.DATABASE"),
            "username": os.getenv("POSTGRES.USERNAME"),
            "password": os.getenv("POSTGRES.PASSWORD")
        }
    }

def get_environment_config() -> dict:
    raw_config: dict = _build_raw_config()
    validated_config = _handle_schema_validation(raw_config = raw_config)
    return validated_config