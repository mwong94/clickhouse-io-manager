"""
Dagster IO manager for ClickHouse database.
"""

import os
import pandas as pd
from typing import Any, Dict, Mapping, Optional, Sequence, Union
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ConfigurableResource,
    MetadataValue,
    io_manager,
)
from clickhouse_driver import Client


class ClickHouseConfig(ConfigurableResource):
    """
    Configurable resource for ClickHouse connection.
    """
    host: str
    port: int = 9000
    user: str
    password: str
    database: str
    settings: Optional[Dict[str, Any]] = None


class ClickHousePandasIOManager(ConfigurableIOManager):
    """
    IO manager for ClickHouse using pandas DataFrames.

    This IO manager handles loading and storing pandas DataFrames to and from ClickHouse tables.
    """
    config: ClickHouseConfig

    def _get_client(self) -> Client:
        """
        Create a ClickHouse client using the configuration.
        """
        return Client(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            settings=self.config.settings or {},
        )

    def _get_table_name(self, context: Union[InputContext, OutputContext]) -> str:
        """
        Get the table name from the asset key or metadata.
        """
        metadata = context.metadata or {}
        schema = metadata.get("schema", "default").lower()
        table = metadata.get("table", context.asset_key.path[-1]).lower()

        return f"{schema}.{table}"

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        """
        Store a pandas DataFrame to a ClickHouse table.
        """
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Expected pandas DataFrame, got {type(obj)}")

        if obj.empty:
            context.log.info("DataFrame is empty, skipping write to ClickHouse")
            return

        table_name = self._get_table_name(context)
        client = self._get_client()

        # Convert DataFrame column names to lowercase for consistency
        obj.columns = [col.lower() for col in obj.columns]

        # Add loaded_at timestamp if not present
        if "loaded_at" not in obj.columns:
            obj["loaded_at"] = pd.Timestamp.now()

        # Convert DataFrame to list of tuples for insertion
        data = [tuple(x) for x in obj.to_records(index=False)]
        column_names = list(obj.columns)

        # Check if the asset is partitioned
        partition_expr = context.metadata.get("partition_expr") if context.metadata else None
        partition_value = context.asset_partition_key if hasattr(context, "has_asset_partitions") and context.has_asset_partitions else None

        # If partitioned, delete previous data from that partition before inserting
        if partition_expr and partition_value:
            try:
                delete_query = f"DELETE FROM {table_name}"

                # For date-based partitioning
                if "date" in partition_expr.lower() or "time" in partition_expr.lower():
                    delete_condition = f" WHERE {partition_expr} >= toDate('{partition_value}') AND {partition_expr} < toDate('{partition_value}') + INTERVAL 1 DAY"
                else:
                    # For other types of partitioning
                    delete_condition = f" WHERE {partition_expr} = '{partition_value}'"

                client.execute(delete_query + delete_condition)
                context.log.info(f"Deleted previous data from partition {partition_value} in {table_name}")
            except Exception as e:
                context.log.error(f"Error deleting previous data from partition {partition_value} in {table_name}: {str(e)}")
                # Continue with insertion even if deletion fails

        # Insert data into ClickHouse
        try:
            client.execute(
                f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES",
                data
            )

            context.add_output_metadata({
                "table": MetadataValue.text(table_name),
                "num_rows": MetadataValue.int(len(obj)),
                "columns": MetadataValue.json(column_names),
                "preview": MetadataValue.md(obj.head().to_markdown() if not obj.empty else "No data"),
            })

            context.log.info(f"Successfully inserted {len(obj)} rows into {table_name}")
        except Exception as e:
            context.log.error(f"Error inserting data into {table_name}: {str(e)}")
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """
        Load a pandas DataFrame from a ClickHouse table.
        """
        table_name = self._get_table_name(context)
        client = self._get_client()

        # Check if we need to filter by partition
        partition_expr = context.metadata.get("partition_expr") if context.metadata else None
        partition_value = context.asset_partition_key if context.has_asset_partitions else None

        query = f"SELECT * FROM {table_name}"

        # Add partition filtering if applicable
        if partition_expr and partition_value:
            # Parse the partition value (assuming ISO date format)
            try:
                # For date-based partitioning
                if "date" in partition_expr.lower() or "time" in partition_expr.lower():
                    # Convert ISO date to ClickHouse-compatible format
                    query += f" WHERE {partition_expr} >= toDate('{partition_value}') AND {partition_expr} < toDate('{partition_value}') + INTERVAL 1 DAY"
                else:
                    # For other types of partitioning
                    query += f" WHERE {partition_expr} = '{partition_value}'"
            except Exception as e:
                context.log.error(f"Error parsing partition value: {str(e)}")
                raise

        # Execute query and fetch results
        try:
            result = client.execute(query, with_column_types=True)
            data, columns = result

            # Create DataFrame from results
            df = pd.DataFrame(data, columns=[col[0] for col in columns])

            context.log.info(f"Successfully loaded {len(df)} rows from {table_name}")
            return df
        except Exception as e:
            context.log.error(f"Error loading data from {table_name}: {str(e)}")
            raise
