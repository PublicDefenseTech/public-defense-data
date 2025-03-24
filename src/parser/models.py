from typing import Optional, List
from sqlmodel import SQLModel, Field, Relationship
from datetime import date


class CaseMetadata(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    county_of_jurisdiction: Optional[str]
    court_case_number: Optional[str]
    good_motions: Optional[str]
    has_evidence_of_representation: Optional[bool]
    parsing_date: Optional[date]
    html_hash: Optional[str]
    odyssey_id: Optional[str]
    court_case_number_hashed: Optional[str]
    case_name: Optional[str]
    case_type: Optional[str]
    date_filed: Optional[str]
    location: Optional[str]
    version: Optional[int]
    top_charge_name: Optional[str]
    top_charge_level: Optional[str]
    dismissed_charges_count: Optional[int]

    related_cases: List["RelatedCase"] = Relationship(back_populates="case_metadata")
    defendant: "Defendant" = Relationship(
        back_populates="case_metadata", sa_relationship_kwargs={"uselist": False}
    )
    defense_attorneys: List["DefenseAttorney"] = Relationship(
        back_populates="case_metadata"
    )
    state_information: "StateInformation" = Relationship(
        back_populates="case_metadata", sa_relationship_kwargs={"uselist": False}
    )
    charges: List["Charge"] = Relationship(back_populates="case_metadata")
    dispositions: List["Disposition"] = Relationship(back_populates="case_metadata")
    events: List["Event"] = Relationship(back_populates="case_metadata")


class RelatedCase(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    case_id: int = Field(foreign_key="casemetadata.id")
    related_case: Optional[str]

    case_metadata: "CaseMetadata" = Relationship(back_populates="related_cases")


class Defendant(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    case_id: int = Field(foreign_key="casemetadata.id", unique=True)
    name: Optional[str]
    sex: Optional[str]
    race: Optional[str]
    date_of_birth: Optional[str]
    height: Optional[str]
    weight: Optional[str]
    address: Optional[str]
    sid: Optional[str]

    case_metadata: "CaseMetadata" = Relationship(back_populates="defendant")


class DefenseAttorney(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    case_id: int = Field(foreign_key="casemetadata.id")
    name: Optional[str]
    phone: Optional[str]
    appointed_retained: Optional[str]
    attorney_hash: Optional[str]

    case_metadata: "CaseMetadata" = Relationship(back_populates="defense_attorneys")


class StateInformation(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    case_id: int = Field(foreign_key="casemetadata.id", unique=True)
    prosecuting_attorney: Optional[str]
    prosecuting_attorney_phone: Optional[str]

    case_metadata: "CaseMetadata" = Relationship(back_populates="state_information")


class Charge(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    case_id: int = Field(foreign_key="casemetadata.id")
    charge_id: Optional[int]
    charge_level: Optional[str]
    original_charge: Optional[str]
    statute: Optional[str]
    is_primary_charge: Optional[bool]
    charge_date: Optional[date]
    charge_name: Optional[str]
    uccs_code: Optional[str]
    charge_desc: Optional[str]
    offense_category_desc: Optional[str]
    offense_type_desc: Optional[str]

    case_metadata: "CaseMetadata" = Relationship(back_populates="charges")


class Disposition(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    case_id: int = Field(foreign_key="casemetadata.id")
    date: Optional[date]
    event: Optional[str]
    judicial_officer: Optional[str]
    details: List["DispositionDetail"] = Relationship(back_populates="disposition")

    case_metadata: "CaseMetadata" = Relationship(back_populates="dispositions")


class DispositionDetail(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    disposition_id: int = Field(foreign_key="disposition.id")
    charge: Optional[str]
    outcome: Optional[str]

    disposition: "Disposition" = Relationship(back_populates="details")


class Event(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    case_id: int = Field(foreign_key="casemetadata.id")
    date: Optional[date]
    event: Optional[str]
    details: Optional[str]

    case_metadata: "CaseMetadata" = Relationship(back_populates="events")
