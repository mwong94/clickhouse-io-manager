# `clickhouse-io-manager`
Dagster io manager to integrate a ClickHouse db with Pandas. Thanks ChatGPT!

## Usage
```python
from clickhouse_io_manager import ClickHousePandasIOManager, ClickHouseConfig
import os

@io_manager(config_schema=ClickHouseConfig.to_config_schema())
def clickhouse_pandas_io_manager(init_context):
    return ClickHousePandasIOManager(
        config=ClickHouseConfig(
            host=init_context.resource_config.get("host", os.getenv("CLICKHOUSE_HOST", "localhost")),
            port=init_context.resource_config.get("port", int(os.getenv("CLICKHOUSE_PORT", "9000"))),
            user=init_context.resource_config.get("user", os.getenv("CLICKHOUSE_USER", "default")),
            password=init_context.resource_config.get("password", os.getenv("CLICKHOUSE_PASSWORD", "")),
            database=init_context.resource_config.get("database", os.getenv("CLICKHOUSE_DB", "default")),
            settings=init_context.resource_config.get("settings", {}),
        )
    )
```
