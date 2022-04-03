import os
import logging
from typing import Optional, List, Any, Tuple, Dict
from datetime import datetime
from abc import abstractmethod
import pyspark
from abc import ABC
from xenpy.slack_tools import send_slack_message
from cached_property import cached_property
from pyspark.sql import SparkSession
import xenpy.hooks.lakehouse as lhm
from xenpy.s3 import databricks_path, dbfs_path
from xenpy.registra.manager import RegistraManager
import inspect
import pprint

from xenpy.models import (
    SourceType,
    PartitionKey,
    ColumnTransformation,
    ColumnRawTransformation,
    DataFormat,
)
from xenpy.data_replication.strategy.writer import (
    DeltaDataReplicationWriterStrategy,
    ParquetDataReplicationWriterStrategy,
)
from xenpy.s3 import s3n_path
import xenpy.exceptions as exc
import xenpy.models as models
from xenpy import IN_TESTING_ENVIRONMENT
import xenpy.utils as utils
import xenpy.constants as constants
from xenpy.helper_tools import get_db_url
from dataclasses import dataclass, field
from xenpy.data_replication.strategy.reporter import (
    DatabookReporterStrategy,
    DatadogReporterStrategy,
)
from xenpy.models.base_manager import BaseDataLakehouseOperationManager
from xenpy.data_quality_check import DataQualityCheck
from xenpy.models.data_directory import StandardDataDirectory, ReplicationDataDirectory
import xenpy.new_utils.mixins as mixins

logger = logging.getLogger(__name__)


@dataclass
class BaseConnector(mixins.TableNameAliasMixin, BaseDataLakehouseOperationManager):
    """
    Base class for Connector class.
    A connector is defined as a class that manages the replication / extraction of data into the target location.

    The source of the data could be but not limited to:
        1. external database system
        2. REST API call result
        3. files in a file store (e.g. AWS S3)
    """

    db_name: str = None
    tbl_name: str = None
    source_name: str = None
    data_format: Optional[str] = "PARQUET"  # TODO: used only when needed to export
    team: Optional[str] = "default"
    etl_mode: Optional[str] = "INCREMENTAL"

    # table identifier modifiers
    tbl_name_alias: Optional[str] = None
    source_tbl_name: Optional[str] = None
    use_enriched_db_name: Optional[bool] = False

    # common usage
    upsert_key: Optional[List[str]] = field(default_factory=lambda: ["id"])

    # persistence
    persisting_original: Optional[bool] = False
    persisting_raw: Optional[bool] = False

    # extract
    skip_extract: Optional[bool] = False  # skip the extraction step
    backfill_filters: Optional[List[models.BackfillFilter]] = field(
        default_factory=list
    )

    # write
    partition_keys: Optional[List[PartitionKey]] = field(default_factory=list)
    column_transformations: Optional[List[ColumnTransformation]] = field(
        default_factory=list
    )
    z_order_by: Optional[List[str]] = field(default_factory=list)
    column_raw_transformations: Optional[List[ColumnRawTransformation]] = field(
        default_factory=list
    )
    partitions_count: Optional[int] = 100
    is_paginated: Optional[bool] = False

    # registering
    grant_read_to: Optional[List[str]] = field(default_factory=lambda: ["default-user"])
    schema_evolution: Optional[bool] = True

    # dqc
    run_quality_check: Optional[bool] = True
    dq_last_x_days: Optional[int] = 60
    dqc_tolerance: Optional[Tuple[int, int]] = (-20, 10)
    dqc_key: Optional[str] = None

    # airflow scheduling (this is not used in the ETL logic itself)
    scheduling: Optional[models.DatabricksJobsSchedulingConfig] = None

    # private, to be excluded from Arguments check
    _source_type: SourceType = None
    _raw_data_directory: StandardDataDirectory = None  # used for extraction step
    _data_directory: ReplicationDataDirectory = None
    _writer_strategy: Any = None
    _reader_strategy: Any = None
    _dqc_strategy: Any = None

    _extractor_strategy_kwargs: Dict = field(default_factory=dict)
    _writer_strategy_kwargs: Dict = field(default_factory=dict)

    _config_in_raw_yaml: Optional[str] = None
    _registra_main_folder_name: Optional[str] = "batch_pipeline"

    def __post_init__(self):
        assert self.source_name is not None
        assert self.db_name is not None
        assert self.tbl_name is not None

        self._config_from_registra()  # TO BaseManager

        # enum conversion
        self._etl_mode = models.ETLMode[self.etl_mode.upper()]
        self._data_format = models.DataFormat[self.data_format.upper()]

        # init some other runtime property
        self.started_at = datetime.utcnow().replace(microsecond=0)

        # create partition keys, sort by order first
        self.partition_keys.sort(key=lambda x: x.order)

        self._init_data_directory()

        # These two will be converted to client
        self.dd_reporter = DatadogReporterStrategy(
            fq_tbl_name=self._data_directory.fq_tbl_name
        )

        self.db_reporter = DatabookReporterStrategy(
            source_type=self._source_type,
            source_name=self.source_name,
            db_name=self.db_name,
            tbl_name=self.tbl_name,
            presto_tbl_name=self._data_directory.fq_tbl_name,
            lakehouse_hook=self.lakehouse_hook,
        )

        self._init_datadog_monitoring()

    def _init_data_directory(self) -> None:
        """
        initialise the data objects for s3 data paths and
        tbl names in the final analytical engine

        paths and names are accessible via both direct instance attributes
        and the private dataclass attributes

        In the future, we should deprecate the direct exposure of those paths.
        """
        self.apply_tbl_name_alias()  # set self._tbl_name_final
        self._data_directory = ReplicationDataDirectory(
            functional_prefix=models.DataLayer.CLEAN.to_s3_path_value(),
            team=self.team,
            test_env=IN_TESTING_ENVIRONMENT,
            db_name=self.db_name,
            tbl_name=self._tbl_name_final,
            source_name=self.source_name if self.use_enriched_db_name else None,
        )

        self._raw_data_directory = StandardDataDirectory(
            functional_prefix=models.DataLayer.RAW.to_s3_path_value(),
            team=self.team,
            test_env=IN_TESTING_ENVIRONMENT,
            db_name=self.db_name,
            tbl_name=self._tbl_name_final,
            source_name=self.source_name if self.use_enriched_db_name else None,
        )

    def _config_from_registra(
        self, registra_manager: RegistraManager = RegistraManager
    ) -> None:

        registra_manager.set_registra_search_path(
            constants.REGISTRA_BATCH_REPLICATION_PIPELINE_PREFIX
        )

        if not registra_manager.has_source(source_name=self.source_name):
            logger.warning(
                f"Source Name {self.source_name} can't be found in registra. ETL pipeline "
                f"will proceed using configs set by user in constructor. "
            )
            self._report_final_config()
            return

        if registra_manager.check_source_type(
            source_name=self.source_name, source_type=self._source_type
        ):
            tbl_registra = registra_manager.get_tbl_registra(
                source_name=self.source_name,
                db_name=self.db_name,
                table_name=self.tbl_name,
            )
        else:
            raise Exception(
                f"ETL Class Type for table `{self.source_name}.{self.db_name}.{self.tbl_name}` seems to be wrong. "
            )

        if tbl_registra is None:
            logger.warning(
                f"Skipping config resolution since there is None from the Registra."
            )
        else:
            self._resolve_config(tbl_registra)

        self._report_final_config()

    @staticmethod
    def configure(spark_session, conf):
        """Set Spark configuration."""

        base_conf = [["spark.sql.sources.partitionOverwriteMode", "dynamic"]]
        if conf:
            base_conf += conf

        for configuration in base_conf:
            spark_session.conf.set(*configuration)

    def _report_pipeline_run_meta(self):
        if not IN_TESTING_ENVIRONMENT:
            if self.update_databook:
                self.db_reporter.insert_data_pipeline_run(
                    pipeline=self._data_directory.fq_tbl_name,
                    started_at=self.started_at,
                )
                self.db_reporter.report()

    def _report_dqc_result(self, check_result):
        if not IN_TESTING_ENVIRONMENT:
            if self.send_to_datadog:
                self.dd_reporter.report_metrics(
                    status=check_result,
                    metric_name=constants.DD_QUALITY_CHECK_METRIC_NAME,
                )

            # report DQC result to db
            if self.update_databook:
                self.db_reporter.send_result_to_postgres(status=check_result)

    def _init_datadog_monitoring(self):

        if IN_TESTING_ENVIRONMENT:
            return

        logger.debug(
            f"Initialising datadog monitoring for table {self._data_directory.fq_tbl_name} "
            f"This is likely skipped if the table already exists. "
        )
        try:
            self.dd_reporter.initialise_monitors()
        except Exception as e:
            message = (
                "*Datadog monitoring initialisation - "
                f"Datadog Monitors failed to be created Exception:* _{self.db_name}.{self.tbl_name}_"
            )
            text = "{}"
            attachments = [{"color": "#FF0000", "text": text.format(str(e))}]
            send_slack_message(
                message=message, channel="data-engg-alerts", attachments=attachments
            )

    def _register_lakehouse_table(self, drop_before_create: Optional[bool] = False):
        self.lakehouse_hook.create_table_by_data_prefix(
            s3_data_prefix=self._data_directory.clean_s3_data_path,
            data_format=self._data_format,
            partition_keys=self.partition_keys,
            fq_tbl_name=self._data_directory.fq_tbl_name,
            drop_original=self.is_dropping_original(self._data_directory.fq_tbl_name)
            or drop_before_create,
        )

    def is_dropping_original(self, fq_tbl_name: str):
        """
        determine whether to drop the original presto table in data lakehouse
        the goal of dropping and recreate is to explicitly guranatee a clean start

        The following cases will be considered:
            1. if etl_mode is in FULL_RELOAD, we will drop and recreate
            2. if the existing underlying table is in PARQUET and mode is in DELTA
            3. if the existing underlying table is in DELTA and mode is in PARQUET
        """
        ddl = self.lakehouse_hook.get_ddl_from_system(fq_tbl_name)

        if ddl is None:
            logger.warning(
                f"Unable to locate table {fq_tbl_name} in Hive when retrieving DDL. "
            )
            return True

        logger.debug(f"Fetched DDL for table {fq_tbl_name}: {ddl}")

        if (
            "_symlink_format_manifest" in ddl
            and self._data_format == models.DataFormat.PARQUET
        ):
            return True

        if (
            "_symlink_format_manifest" not in ddl
            and self._data_format == models.DataFormat.DELTA
        ):
            return True

        if self._etl_mode == models.ETLMode.FULL_RELOAD:
            return True

        return False

    def run(self):
        """
        Entry point.

        The main process:
            1. Load data from a hook into Python memory as a Spark / Pandas dataframe
            2. Optionally persisted in targeted RAW layer, locally or in a file store
            3. Write into the Target location, if this is a
            4. Target table registration (for Secondary engine like presto)

        """
        logger.warning(
            "Step 1: Extracting data from source and persisting into RAW layer ..."
        )
        df = self.extract()

        logger.warning(
            f"Step 2: Read from the data passed from step 1, persisting into CLEAN layer ..."
        )
        write_status = self._writer_strategy.write(
            df=df, **self._writer_strategy_kwargs
        )

        # this part can be handled by Airflow branching logic
        if write_status:
            logger.warning(f"Step 3: Create Presto table ...")
            self.create_lakehouse_table()
        else:
            logger.warning(
                f"Step 3: Skipping create presto table due to no additional data persisted in step 2 ..."
            )

        if write_status or not self._writer_strategy._reload:
            logger.warning(f"Step 4: Execute cross system DQC ...")
            if self.run_quality_check:
                self.dqc()
            else:
                logger.warning(
                    f"Step 4: Skipping DQC since it's explicitly disabled in configs. "
                )
        else:
            logger.warning(
                f"Step 4: Skipping DQC since a FULL_RELOAD on empty source occurred ... "
            )

        # pipeline reporting
        self._report_pipeline_run_meta()
        # TODO: potential steps in the future: in lakehouse DQC, with dbt

    def create_lakehouse_table(self, drop_before_create: Optional[bool] = False):
        self._register_lakehouse_table(drop_before_create)
        self._grant_tbl_access()
        self._repair_hive_table()

    def dqc(self):
        """main entry point for executing DQC"""
        dqc_manager = DataQualityCheck(
            source_name=self.source_name,
            db_name=self.db_name,
            tbl_name=self.tbl_name,
            fq_tbl_name=self._data_directory.fq_tbl_name,
            send_to_datadog=self.send_to_datadog,
            lakehouse_hook=self.lakehouse_hook,
            dqc_strategy=self._dqc_strategy,
        )

        dqc_result = dqc_manager.standard_check()

        # report dqc result
        self._report_dqc_result(dqc_result)

        # process outcome in application
        if not dqc_result:

            # delta version rollback, once fully migrated, this clause can be refactored
            if self.data_format == models.DataFormat.DELTA:
                logger.warning(
                    "One or more DQC checks failed. Restoring the previous data version in Delta Lake. "
                )
                self._writer_strategy.rollback()

            raise exc.DQCCheckFailureException()

    def extract(self):
        if not self.skip_extract:
            df = self._reader_strategy.extract(**self._extractor_strategy_kwargs)
        else:
            logger.warning(
                "Extraction step is explicitly skipped. Trying to read data directly "
                f"from s3 data path {self._data_directory.dbfs_raw_s3_data_path}"
            )
            try:
                df = self.spark_session.read.format(self._data_format.value).run(
                    self._data_directory.dbfs_raw_s3_data_path
                )
            except pyspark.sql.utils.AnalysisException:
                logger.error(
                    "Unable to read the data as a Spark Dataframe. Please check, and most likely you will "
                    "need to reload the entire raw folder. "
                )
                raise
        return df

    def _init_writer(self):
        """generic and default delta writer"""
        # init writer
        if self._data_format == DataFormat.PARQUET:
            self._writer_strategy = ParquetDataReplicationWriterStrategy(
                spark_session=self.spark_session,
                data_format=self._data_format,
                data_directory=self._data_directory,
                lakehouse_hook=self.lakehouse_hook,
                etl_mode=self._etl_mode,
                partition_keys=self.partition_keys,
                partitions_count=self.partitions_count,
                column_transformations=self.column_transformations,
                dbutils_manager=self.dbutils_manager,
            )
        elif self._data_format == DataFormat.DELTA:
            self._writer_strategy = DeltaDataReplicationWriterStrategy(
                spark_session=self.spark_session,
                data_format=self._data_format,
                data_directory=self._data_directory,
                lakehouse_hook=self.lakehouse_hook,
                etl_mode=self._etl_mode,
                partition_keys=self.partition_keys,
                upsert_key=self.upsert_key,
                partitions_count=self.partitions_count,
                column_transformations=self.column_transformations,
                dbutils_manager=self.dbutils_manager,
                z_order_by=self.z_order_by,
                schema_evolution=self.schema_evolution,
            )
        else:
            raise Exception(
                f"Unknown OutputFormat type, unable to determine the writer "
                f"strategy based on this format: {self._data_format}. "
            )


@dataclass
class FileConnector(BaseConnector, ABC):
    """ A base Connector that reads directly from a file path"""
    ...


@dataclass
class ThirdPartyConnector(BaseConnector, ABC):

    """
    A base Connector that reads directly from a third party system.
    Typically this is achieved using API or Python SDK (wrapper around the API)
    """

    is_cached: Optional[bool] = False

    def _init_data_directory(self) -> None:
        super()._init_data_directory()
        self._cached_data_directory = StandardDataDirectory(
            functional_prefix=models.DataLayer.CACHED.to_s3_path_value(),
            team=self.team,
            test_env=IN_TESTING_ENVIRONMENT,
            db_name=self.db_name,
            tbl_name=self._tbl_name_final,
            source_name=self.source_name if self.use_enriched_db_name else None,
        )


@dataclass
class DatabaseConnector(BaseConnector, ABC):

    is_paginated: Optional[bool] = False
    pagination_key: Optional[str] = None
    pagination_size: Optional[int] = 1000000
    fields_to_remove: Optional[List[str]] = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        self.source_db_url = get_db_url(self.source_name, self.db_name)
