# Copyright 2023 Privacy Dynamics, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from datetime import datetime
from enum import Enum
from typing import Dict, List, Union

from pydantic import UUID4, ConfigDict, Field, field_validator, model_validator

from pvcy.connection_types import PvcyBaseModel, SimpleConnection


class TreatmentMethod(Enum):
    HIDE = "hide"
    LOCK = "lock"
    HASH = "hash"
    MASK = "mask"
    FAKE = "fake"
    ANONYMIZE = "anonymize"


class PiiClass(Enum):
    ABA_ROUTING_NUMBER = "aba_routing_number"
    CREDIT_CARD = "credit_card"
    CRYPTO = "crypto"
    EMAIL_ADDRESS = "email_address"
    IP_ADDRESS = "ip_address"
    SG_NRIC_FIN = "sg_nric_fin"
    US_BANK_NUMBER = "us_bank_number"
    US_DRIVER_LICENSE = "us_driver_license"
    US_ITIN = "us_itin"
    US_TAX_ID = "us_tax_id"
    US_PASSPORT = "us_passport"
    PHONE_NUMBER = "phone_number"
    US_SSN = "us_ssn"
    ADVERTISING_ID = "advertising_id"
    ESNDEC_ID = "esndec_id"
    ESNHEX_ID = "esnhex_id"
    IMEI = "imei"
    MAC_ADDRESS = "mac_address"
    MEID = "meid"
    PASSWORD = "password"
    PERSON_NAME = "person_name"
    FIRST_NAME = "first_name"
    MIDDLE_NAME = "middle_name"
    LAST_NAME = "last_name"
    FULL_NAME = "full_name"
    US_ADDRESS = "us_address"
    US_ADDRESS_FULL = "us_address_full"
    AGE = "age"
    GENDER = "gender"
    CITY = "city"
    COORDINATE = "coordinate"
    NATIONALITY = "nationality"
    PARTIAL_COORDINATE = "partial_coordinate"
    DATE = "date"
    BIRTH_DATE = "birth_date"
    DEATH_DATE = "death_date"
    TIME = "time"
    US_STATE = "us_state"
    US_ZIPCODE = "us_zipcode"
    DOMAIN_NAME = "domain_name"
    GENERIC_DID = "generic_did"
    GENERIC_CONTINUOUS = "generic_continuous"
    GENERIC_CATEGORY = "generic_category"


class ColumnConfig(PvcyBaseModel):
    treatment_method: Union[TreatmentMethod, None] = None
    pii_class: Union[PiiClass, None] = None


class JobRunType(Enum):
    LOAD_WRITE = "load_write"
    LOAD_ASSESS = "load_assess"
    LOAD_TREAT_WRITE = "load_treat_write"


class KStrategy(Enum):
    ANONYMIZE = "anonymize"
    SUPPRESS = "suppress"


class NewJobDefinition(PvcyBaseModel):
    table_name: str
    job_definition_id: Union[UUID4, None] = None
    enabled: Union[bool, None] = None
    job_run_type: Union[JobRunType, None] = None
    k_strategy: Union[KStrategy, None] = None
    k_target: Union[int, None] = None
    schedule: Union[str, None] = None
    column_config: Union[Dict[str, ColumnConfig], None] = None

    @model_validator(mode="after")
    def treat_needs_k_fields(self) -> "NewJobDefinition":
        if self.job_run_type and self.job_run_type is JobRunType.LOAD_TREAT_WRITE:
            if self.k_strategy is None or self.k_target is None:
                raise ValueError(
                    "Job definitions with a LOAD_TREAT_WRITE strategy require a "
                    "k_strategy and k_target."
                )
        return self

    @classmethod
    def from_job_definition(
        cls, job_definition: "JobDefinition", keep_id: bool = True
    ) -> "NewJobDefinition":
        return cls(
            table_name=job_definition.origin_table,
            job_definition_id=job_definition.job_definition_id if keep_id else None,
            enabled=job_definition.enabled,
            job_run_type=job_definition.job_run_type,
            k_strategy=job_definition.k_strategy,
            k_target=job_definition.k_target,
            schedule=job_definition.schedule,
            column_config=job_definition.column_config,
        )


# class Assessment(PvcyBaseModel):
#     assessment_id: UUID4
#     created: datetime
#     job_definition_id: UUID4
#     columns_processed: int
#     rows_processed: int
#     rows_treated: int
#     cells_treated: int
#     cells_processed: int
#     k_target: int
#     phisher_average_type_a: float
#     phisher_average_type_a_initial: float
#     schedule: str
#     k_strategy: Union[KStrategy, None]


# class LastJobRun(PvcyBaseModel):
#     job_run_id: int
#     start_timestamp: datetime
#     end_timestamp: datetime
#     duration: float
#     last_event_type: str
#     last_event_result: str
#     last_event_message: Union[str, None]


class NameSpace(PvcyBaseModel):
    db_schema: Union[str, None] = Field(default=None, alias="schema")
    dataset: Union[str, None] = None
    prefix: Union[str, None] = None
    bucket: Union[str, None] = None
    directory: Union[str, None] = None


class JobDefinition(PvcyBaseModel):
    model_config = ConfigDict(extra="ignore")

    job_definition_id: UUID4
    origin_namespace: Union[NameSpace, None] = None
    origin_table: str
    destination_namespace: Union[NameSpace, None] = None
    destination_table: Union[str, None]
    k_strategy: Union[KStrategy, None]
    k_target: Union[int, None]
    enabled: bool
    job_run_type: JobRunType
    created_at: Union[datetime, None] = Field(None, alias="created")
    updated_at: Union[datetime, None] = Field(None, alias="updated")
    column_config: Union[Dict[str, ColumnConfig], None]
    celery_schedule_id: Union[UUID4, None] = None
    schedule: Union[str, None] = None
    # Fields excluded due to planned future API changes
    # assessment: Union[Assessment, None] = None
    # last_job_run: Union[LastJobRun, None] = None


class Project(PvcyBaseModel):
    auto_create_job_definitions: bool
    days_expire: Union[str, None]
    default_job_run_type: str
    destination_connection: Union[SimpleConnection, None]
    destination_namespace: Union[NameSpace, None]
    did_default_treatment_method: Union[str, None]
    job_definitions: List[JobDefinition]
    origin_connection: Union[SimpleConnection, None]
    origin_namespace: Union[NameSpace, None]
    project_id: UUID4
    project_owner: str
    project_title: str
    qid_anonymize: Union[bool, None]
    risk_levels: Union[str, None]
    schedule: Union[str, None]
    created_at: datetime = Field(alias="created")
    updated_at: datetime = Field(alias="updated")


class ProjectConfig(PvcyBaseModel):
    project_title: str
    project_id: UUID4
    job_definitions: List[NewJobDefinition]

    @classmethod
    def from_project(cls, project: Project) -> "ProjectConfig":
        job_definitions = [
            NewJobDefinition.from_job_definition(jd) for jd in project.job_definitions
        ]
        return cls(
            project_title=project.project_title,
            project_id=project.project_id,
            job_definitions=job_definitions,
        )

    @classmethod
    def from_project_and_job_defs(
        cls, project: Project, job_definitions: List[NewJobDefinition]
    ) -> "ProjectConfig":
        return cls(
            project_title=project.project_title,
            project_id=project.project_id,
            job_definitions=job_definitions.copy(),
        )


class ConfigFile(PvcyBaseModel):
    @field_validator("version")
    @classmethod
    def version_one_only(cls, v: int) -> int:
        if v != 1:
            raise ValueError("Only version 1 config files accepted.")
        return v

    version: int
    projects: List[ProjectConfig]
