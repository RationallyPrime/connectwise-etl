"""Dimension table configuration models. NO OPTIONAL FIELDS."""

from enum import Enum

from pydantic import BaseModel, ConfigDict, Field

from ..utils.base import ErrorCode
from ..utils.exceptions import ETLConfigError


class DimensionType(str, Enum):
    """Types of dimensions."""
    STANDARD = "standard"      # Regular dimension from column values
    DATE = "date"             # Date/calendar dimension
    TIME = "time"             # Time of day dimension
    JUNK = "junk"             # Junk dimension for flags/indicators
    CONFORMED = "conformed"   # Conformed dimension across sources


class DimensionConfig(BaseModel):
    """Configuration for creating dimension tables. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)

    # Dimension identification - REQUIRED
    name: str = Field(description="Dimension name (without dim_ prefix)")
    type: DimensionType = Field(description="Type of dimension")
    source_table: str = Field(description="Source table name")
    source_column: str = Field(description="Source column name")

    # Key configuration - REQUIRED
    natural_key_column: str = Field(description="Natural key column name")
    surrogate_key_column: str = Field(description="Surrogate key column name")

    # Additional columns - REQUIRED
    description_column: str = Field(description="Description column name")
    additional_columns: list[str] = Field(description="Additional columns to include")

    # Processing configuration - REQUIRED
    add_unknown_member: bool = Field(description="Add unknown member (-1)")
    unknown_member_key: int = Field(description="Key value for unknown member")
    unknown_member_description: str = Field(description="Description for unknown member")

    # Audit configuration - REQUIRED
    add_audit_columns: bool = Field(description="Add ETL audit columns")

    def validate_config(self) -> None:
        """Validate dimension configuration. FAIL FAST on issues."""
        # Validate required fields
        if not self.name:
            raise ETLConfigError(
                "Dimension name is required",
                code=ErrorCode.CONFIG_MISSING
            )

        if not self.source_table:
            raise ETLConfigError(
                "Source table is required",
                code=ErrorCode.CONFIG_MISSING,
                details={"dimension": self.name}
            )

        if not self.source_column:
            raise ETLConfigError(
                "Source column is required",
                code=ErrorCode.CONFIG_MISSING,
                details={"dimension": self.name, "source_table": self.source_table}
            )

        if not self.natural_key_column:
            raise ETLConfigError(
                "Natural key column name is required",
                code=ErrorCode.CONFIG_MISSING,
                details={"dimension": self.name}
            )

        if not self.surrogate_key_column:
            raise ETLConfigError(
                "Surrogate key column name is required",
                code=ErrorCode.CONFIG_MISSING,
                details={"dimension": self.name}
            )

        # Validate unknown member configuration
        if self.add_unknown_member:
            if self.unknown_member_key >= 0:
                raise ETLConfigError(
                    "Unknown member key must be negative",
                    code=ErrorCode.CONFIG_INVALID,
                    details={
                        "dimension": self.name,
                        "unknown_member_key": self.unknown_member_key
                    }
                )

            if not self.unknown_member_description:
                raise ETLConfigError(
                    "Unknown member description is required when add_unknown_member is True",
                    code=ErrorCode.CONFIG_INVALID,
                    details={"dimension": self.name}
                )
