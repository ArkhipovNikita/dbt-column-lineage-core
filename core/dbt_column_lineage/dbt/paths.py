import os

from dbt.config import RuntimeConfig
from dbt_column_lineage.dbt.consts import (
    COLUMN_LINEAGE_DIRNAME,
    COLUMN_LINEAGE_MANIFEST_FILENAME,
)


def get_column_lineage_directory(config: RuntimeConfig) -> str:
    return os.path.join(
        config.target_path,
        COLUMN_LINEAGE_DIRNAME,
    )


def get_column_lineage_manifest_path(config: RuntimeConfig) -> str:
    directory = get_column_lineage_directory(config)
    return os.path.join(directory, COLUMN_LINEAGE_MANIFEST_FILENAME)
