import sys
import os
from marshmallow import fields, Schema, ValidationError
from pprint import pprint

from src.enums import JobType
from src.clients.ExchangeRateHost import BaseCurrency

class LoggerConfig(Schema):
    log_level = fields.String(
        required = True,
        allow_none = False
    )
    name = fields.String(
        required = True,
        allow_none = False
    )
    json_file_path = fields.String(
        required = True,
        allow_none = False
    )

class JobConfig(Schema):
    type = fields.Enum(
        enum = JobType, 
        by_value = True,
        required = True,
        error_messages = { "required": "job.type is required to launch" }
    )
    historical_end_date = fields.Date(
        required = False,
        allow_none = False
    )
    historical_previous_days = fields.Integer(
        required = False
    )
    base_currency = fields.Enum(
        enum = BaseCurrency, 
        by_value = True,
        required = True,
        error_messages = { "required": "job.base_currency is required to launch" }
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
    job = fields.Nested(JobConfig())

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
        "job": {
            "type": os.getenv("JOB.TYPE"),
            "historical_end_date": os.getenv("JOB.HISTORICAL_END_DATE"),
            "historical_previous_days": os.getenv("JOB.HISTORICAL_PREVIOUS_DAYS"),
            "base_currency": os.getenv("JOB.BASE_CURRENCY")
        },
        "logger": {
            "log_level": os.getenv("LOGGER.LOG_LEVEL"),
            "name": os.getenv("LOGGER.NAME"),
            "json_file_path": os.getenv("LOGGER.JSON_FILE_PATH")
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