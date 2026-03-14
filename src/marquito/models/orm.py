"""
SQLAlchemy ORM models
"""

import uuid as uuidlib
from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func

from marquito.db.session import Base


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


class Tag(Base):
    __tablename__ = "tags"

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    name: Mapped[str] = mapped_column(String(1024), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)


# ---------------------------------------------------------------------------
# Namespaces
# ---------------------------------------------------------------------------


class Namespace(Base):
    __tablename__ = "namespaces"

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    name: Mapped[str] = mapped_column(String(1024), unique=True, nullable=False)
    current_owner_name: Mapped[str | None] = mapped_column(String(1024))
    description: Mapped[str | None] = mapped_column(Text)
    is_hidden: Mapped[bool] = mapped_column(Boolean, default=False)

    datasets: Mapped[list["Dataset"]] = relationship(
        "Dataset", back_populates="namespace"
    )
    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="namespace")


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


class Source(Base):
    __tablename__ = "sources"

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    type: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(1024), unique=True, nullable=False)
    connection_url: Mapped[str | None] = mapped_column(Text)
    description: Mapped[str | None] = mapped_column(Text)

    datasets: Mapped[list["Dataset"]] = relationship("Dataset", back_populates="source")


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


class Dataset(Base):
    __tablename__ = "datasets"
    __table_args__ = (UniqueConstraint("namespace_uuid", "name"),)

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    namespace_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("namespaces.uuid"), nullable=False
    )
    source_uuid: Mapped[uuidlib.UUID | None] = mapped_column(ForeignKey("sources.uuid"))
    name: Mapped[str] = mapped_column(String(1024), nullable=False)
    type: Mapped[str] = mapped_column(String(64), nullable=False, default="DB_TABLE")
    physical_name: Mapped[str | None] = mapped_column(String(1024))
    description: Mapped[str | None] = mapped_column(Text)
    is_hidden: Mapped[bool] = mapped_column(Boolean, default=False)
    last_modified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    facets: Mapped[dict] = mapped_column(JSON, default=dict, server_default="{}")
    current_version_uuid: Mapped[uuidlib.UUID | None] = mapped_column(
        UUID(as_uuid=True)
    )

    namespace: Mapped["Namespace"] = relationship(
        "Namespace", back_populates="datasets"
    )
    source: Mapped["Source | None"] = relationship("Source", back_populates="datasets")
    fields: Mapped[list["DatasetField"]] = relationship(
        "DatasetField", back_populates="dataset"
    )
    tags: Mapped[list["DatasetTag"]] = relationship(
        "DatasetTag", back_populates="dataset"
    )


class DatasetField(Base):
    __tablename__ = "dataset_fields"
    __table_args__ = (UniqueConstraint("dataset_uuid", "name"),)

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    dataset_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("datasets.uuid"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(1024), nullable=False)
    type: Mapped[str | None] = mapped_column(String(64))
    description: Mapped[str | None] = mapped_column(Text)

    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="fields")
    tags: Mapped[list["DatasetFieldTag"]] = relationship(
        "DatasetFieldTag", back_populates="field"
    )


class DatasetTag(Base):
    __tablename__ = "dataset_tags"
    __table_args__ = (UniqueConstraint("dataset_uuid", "name"),)

    dataset_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("datasets.uuid"), primary_key=True
    )
    name: Mapped[str] = mapped_column(String(1024), primary_key=True)
    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="tags")


class DatasetFieldTag(Base):
    __tablename__ = "dataset_field_tags"
    __table_args__ = (UniqueConstraint("field_uuid", "name"),)

    field_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("dataset_fields.uuid"), primary_key=True
    )
    name: Mapped[str] = mapped_column(String(1024), primary_key=True)
    field: Mapped["DatasetField"] = relationship("DatasetField", back_populates="tags")


# ---------------------------------------------------------------------------
# Dataset versions
# ---------------------------------------------------------------------------


class DatasetVersion(Base):
    __tablename__ = "dataset_versions"

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    dataset_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("datasets.uuid"), nullable=False
    )
    version: Mapped[uuidlib.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    namespace_name: Mapped[str | None] = mapped_column(String(1024))
    dataset_name: Mapped[str | None] = mapped_column(String(1024))
    lifecycle_state: Mapped[str | None] = mapped_column(String(64))
    run_uuid: Mapped[uuidlib.UUID | None] = mapped_column(UUID(as_uuid=True))
    schema_version_uuid: Mapped[uuidlib.UUID | None] = mapped_column(UUID(as_uuid=True))

    dataset: Mapped["Dataset"] = relationship("Dataset")


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (UniqueConstraint("namespace_uuid", "name"),)

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    namespace_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("namespaces.uuid"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(1024), nullable=False)
    type: Mapped[str] = mapped_column(String(64), nullable=False, default="BATCH")
    description: Mapped[str | None] = mapped_column(Text)
    current_version_uuid: Mapped[uuidlib.UUID | None] = mapped_column(
        UUID(as_uuid=True)
    )
    is_hidden: Mapped[bool] = mapped_column(Boolean, default=False)
    facets: Mapped[dict] = mapped_column(JSON, default=dict, server_default="{}")

    namespace: Mapped["Namespace"] = relationship("Namespace", back_populates="jobs")
    runs: Mapped[list["Run"]] = relationship("Run", back_populates="job")


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


class Run(Base):
    __tablename__ = "runs"

    uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    job_uuid: Mapped[uuidlib.UUID | None] = mapped_column(ForeignKey("jobs.uuid"))
    nominal_start_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    nominal_end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    current_run_state: Mapped[str | None] = mapped_column(String(64))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    facets: Mapped[dict | None] = mapped_column(JSON)

    job: Mapped["Job | None"] = relationship("Job", back_populates="runs")
    input_datasets: Mapped[list["RunDatasetInput"]] = relationship(
        "RunDatasetInput", back_populates="run"
    )
    output_datasets: Mapped[list["RunDatasetOutput"]] = relationship(
        "RunDatasetOutput", back_populates="run"
    )


class RunDatasetInput(Base):
    __tablename__ = "runs_input_mapping"
    __table_args__ = (UniqueConstraint("run_uuid", "dataset_uuid"),)

    run_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("runs.uuid"), primary_key=True
    )
    dataset_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("datasets.uuid"), primary_key=True
    )

    run: Mapped["Run"] = relationship("Run", back_populates="input_datasets")
    dataset: Mapped["Dataset"] = relationship("Dataset")


class RunDatasetOutput(Base):
    __tablename__ = "runs_output_mapping"
    __table_args__ = (UniqueConstraint("run_uuid", "dataset_uuid"),)

    run_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("runs.uuid"), primary_key=True
    )
    dataset_uuid: Mapped[uuidlib.UUID] = mapped_column(
        ForeignKey("datasets.uuid"), primary_key=True
    )

    run: Mapped["Run"] = relationship("Run", back_populates="output_datasets")
    dataset: Mapped["Dataset"] = relationship("Dataset")


# ---------------------------------------------------------------------------
# OpenLineage events (raw storage)
# ---------------------------------------------------------------------------


class LineageEvent(Base):
    __tablename__ = "lineage_events"

    event_uuid: Mapped[uuidlib.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuidlib.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    run_uuid: Mapped[uuidlib.UUID | None] = mapped_column(UUID(as_uuid=True))
    job_name: Mapped[str | None] = mapped_column(String(1024))
    job_namespace: Mapped[str | None] = mapped_column(String(1024))
    producer: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
