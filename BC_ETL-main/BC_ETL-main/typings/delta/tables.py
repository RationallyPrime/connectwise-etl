"""Type stubs for delta.tables module."""

from pyspark.sql import DataFrame, SparkSession


class DeltaTable:
    """Type stub for DeltaTable class."""

    @classmethod
    def forPath(cls, spark: SparkSession, path: str) -> "DeltaTable":
        """Create a DeltaTable from the given path."""
        ...

    @classmethod
    def forName(cls, spark: SparkSession, tableName: str) -> "DeltaTable":
        """Create a DeltaTable from the given table name."""
        ...

    @classmethod
    def isDeltaTable(cls, spark: SparkSession, path: str) -> bool:
        """Check if the given path is a Delta table."""
        ...

    def toDF(self) -> DataFrame:
        """Convert to DataFrame."""
        ...

    def alias(self, alias: str) -> "DeltaTable":
        """Apply an alias to the DeltaTable."""
        ...

    def update(
        self, condition: str | None = None, set: dict[str, str] | None = None
    ) -> "DeltaTable":
        """Update rows that match the given condition."""
        ...

    def updateExpr(
        self, condition: str | None = None, set: dict[str, str] | None = None
    ) -> "DeltaTable":
        """Update rows that match the given condition using expressions."""
        ...

    def delete(self, condition: str | None = None) -> "DeltaTable":
        """Delete rows that match the given condition."""
        ...

    def merge(self, source: DataFrame, condition: str) -> "DeltaMergeBuilder":
        """Merge source DataFrame with this Delta table."""
        ...

    def history(self, limit: int | None = None) -> DataFrame:
        """Get the history of this table."""
        ...

    def vacuum(self, retentionHours: float | None = None) -> DataFrame:
        """Vacuum files not needed by the table for the given retention period."""
        ...

    def generate(self, mode: str) -> DataFrame:
        """Generate manifest files for the table."""
        ...

    def optimize(self) -> "DeltaOptimizeBuilder":
        """Optimize the layout of Delta table data."""
        ...


class DeltaMergeBuilder:
    """Type stub for DeltaMergeBuilder class."""

    def whenMatched(self, condition: str | None = None) -> "DeltaMergeMatchedActionBuilder":
        """Specify the condition to match."""
        ...

    def whenNotMatched(self, condition: str | None = None) -> "DeltaMergeNotMatchedActionBuilder":
        """Specify the condition not to match."""
        ...

    def execute(self) -> None:
        """Execute the merge operation."""
        ...


class DeltaMergeMatchedActionBuilder:
    """Type stub for DeltaMergeMatchedActionBuilder class."""

    def update(self, set: dict[str, str]) -> "DeltaMergeBuilder":
        """Update matched rows."""
        ...

    def updateExpr(self, set: dict[str, str]) -> "DeltaMergeBuilder":
        """Update matched rows using expressions."""
        ...

    def delete(self) -> "DeltaMergeBuilder":
        """Delete matched rows."""
        ...


class DeltaMergeNotMatchedActionBuilder:
    """Type stub for DeltaMergeNotMatchedActionBuilder class."""

    def insert(self, values: dict[str, str]) -> "DeltaMergeBuilder":
        """Insert new rows."""
        ...

    def insertExpr(self, values: dict[str, str]) -> "DeltaMergeBuilder":
        """Insert new rows using expressions."""
        ...


class DeltaOptimizeBuilder:
    """Type stub for DeltaOptimizeBuilder class."""

    def zOrderBy(self, *cols: str) -> DataFrame:
        """Z-order by the given columns."""
        ...
