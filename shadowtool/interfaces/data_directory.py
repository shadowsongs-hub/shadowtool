from dataclasses import dataclass
from cached_property import cached_property
import xenpy.utils as utils
from xenpy.s3 import dbfs_path, s3n_path
import shadowtool.models as models
from typing import Optional


@dataclass
class BaseDataDirectory:
    """
    a base model to provide direction for data
    paths and tbl identifiers and object location
    """

    # this tbl name should be the final tbl name, aliasing is done out side of
    # data directory entry
    data_layer: models.Data
    tbl_name: str
    db_name: str
    team: str
    source_name: Optional[str] = None

    @cached_property
    def fq_tbl_name(self):

        return utils.tbl_parts_to_tbl_name(
            functional_prefix=self.functional_prefix,
            db_name=self.enriched_db_name
            if self.source_name is not None
            else self.db_name,
            tbl_name=self.tbl_name,
            test_env=self.test_env,
        )

    @cached_property
    def enriched_db_name(self):
        """
        to further enhance the uniqueness of the db name, source name will be used as well
        """
        return utils.normalise_data_path_and_table(f"{self.source_name}_{self.db_name}")

    @cached_property
    def normalised_db_name(self):
        return utils.normalise_data_path_and_table(self.db_name)

    @cached_property
    def normalised_fq_tbl_name(self):
        return utils.normalise_data_path_and_table(self.fq_tbl_name).replace(".", "_")

    @cached_property
    def full_db_name(self):
        db_name, _ = utils.get_parts_from_tbl_name(self.fq_tbl_name)
        return db_name


@dataclass
class StandardDataDirectory(BaseDataDirectory):
    """directory for transform tables"""

    @cached_property
    def s3_data_path(self):
        return utils.tbl_parts_to_s3_data_prefix(
            db_name=self.enriched_db_name
            if self.source_name is not None
            else self.db_name,
            tbl_name=self.tbl_name,
            functional_prefix=self.functional_prefix,
            team=self.team,
            test_env=self.test_env,
        )

    @cached_property
    def dbfs_s3_data_path(self):
        return dbfs_path(self.s3_data_path)

    @cached_property
    def s3n_s3_data_path(self):
        return s3n_path(self.s3_data_path)


class ReplicationDataDirectory(StandardDataDirectory):
    """directory for clean and raw tables"""

    @cached_property
    def s3_data_path(self):
        return utils.tbl_parts_to_s3_data_prefix(
            db_name=self.enriched_db_name
            if self.source_name is not None
            else self.db_name,
            tbl_name=self.tbl_name,
            functional_prefix=self.functional_prefix,
            team=self.team,
            test_env=self.test_env,
        )

    @cached_property
    def clean_s3_data_path(self):
        return utils.tbl_parts_to_s3_data_prefix(
            db_name=self.enriched_db_name
            if self.source_name is not None
            else self.db_name,
            tbl_name=self.tbl_name,
            functional_prefix=self.functional_prefix,
            team=self.team,
            test_env=self.test_env,
        )

    @cached_property
    def raw_s3_data_path(self):
        return utils.tbl_parts_to_s3_data_prefix(
            db_name=self.enriched_db_name
            if self.source_name is not None
            else self.db_name,
            tbl_name=self.tbl_name,
            functional_prefix=models.DataLayer.RAW.to_s3_path_value(),
            team=self.team,
            test_env=self.test_env,
        )

    @cached_property
    def cached_s3_data_path(self):
        return utils.tbl_parts_to_s3_data_prefix(
            db_name=self.enriched_db_name
            if self.source_name is not None
            else self.db_name,
            tbl_name=self.tbl_name,
            functional_prefix=models.DataLayer.CACHED.to_s3_path_value(),
            team=self.team,
            test_env=self.test_env,
        )

    @cached_property
    def dbfs_s3_data_path(self):
        return dbfs_path(self.clean_s3_data_path)

    @cached_property
    def dbfs_clean_s3_data_path(self):
        return dbfs_path(self.clean_s3_data_path)

    @cached_property
    def dbfs_raw_s3_data_path(self):
        return dbfs_path(self.raw_s3_data_path)

    @cached_property
    def dbfs_cached_s3_data_path(self):
        return dbfs_path(self.cached_s3_data_path)

    @cached_property
    def s3n_s3_data_path(self):
        return s3n_path(self.clean_s3_data_path)

    @cached_property
    def s3n_clean_s3_data_path(self):
        return s3n_path(self.clean_s3_data_path)

    @cached_property
    def s3n_raw_s3_data_path(self):
        return s3n_path(self.raw_s3_data_path)

    @cached_property
    def s3n_cached_s3_data_path(self):
        return s3n_path(self.cached_s3_data_path)

    @cached_property
    def raw_fq_tbl_name(self):
        return utils.tbl_parts_to_tbl_name(
            functional_prefix=models.DataLayer.RAW.to_s3_path_value(),
            db_name=self.enriched_db_name
            if self.source_name is not None
            else self.db_name,
            tbl_name=self.tbl_name,
            test_env=self.test_env,
        )
