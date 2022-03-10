from typing import Dict, Optional, List, Union
import yaml
import sys
from collections import defaultdict
import os
from typing import Any

from shadowtool.main.general.logging_utils import LoggingMixin
import shadowtool.main.registra.models as models
import shadowtool.main.registra.parsers as parsers


class RegistraManagerMeta(type, LoggingMixin):
    """

    used to declare a class property

    refer to: https://stackoverflow.com/questions/128573/using-property-on-classmethods
    """

    _registra: Dict[str, Any] = None

    @property
    def registra(cls):

        if cls._registra is None:
            cls._registra = cls.load_registra()

        return cls._registra


class RegistraManager(metaclass=RegistraManagerMeta):

    """
    handle the reading and parsing of the registra yaml files, which
    stores the configuration of tables
    """

    local_registra_path: str
    registra_search_path: Optional[str]
    registra_module_path: List[str] = []
    registra_parsers: Dict[models.BaseRegistraType, parsers.BaseRegistraParser]

    def set_registra_parsers(self, parsers_mapping: Dict[models.BaseRegistraType, ]):


    @classmethod
    def load_registra(cls):

        cls.registra_module_path = cls._discover_registra_module_path(
            cls.local_registra_path
        )

        registra_result = defaultdict(str)

        for subfolder in cls.registra_module_path:
            module_path = os.path.join(cls.local_registra_path, subfolder)
            logger.debug(f"Currently reading registra files from: {module_path}")

            raw_yamls = os.listdir(module_path)
            for item in raw_yamls:
                if utils.is_yaml_file(item):
                    yaml_raw_text = cls._load_yaml_resource(
                        file_path=os.path.join(module_path, item)
                    )

                    try:
                        registra_type = yaml_raw_text["registra_type"]
                    except KeyError:
                        logger.error(
                            f"Unable to find registra_type in the file {item}, please check "
                            f"that each yaml file contains the field registra_type so the parser know"
                            f"which template to use. "
                        )
                        sys.exit(1)

                    current_parser = cls.registra_parsers.get(registra_type)

                    assert current_parser is not None, (
                        f"Unable to determine the Registra parser with the given "
                        f"Registra Type: {registra_type}"
                    )

                    current_parsed_result = current_parser(
                        file_name=item, raw_yaml_dict=yaml_raw_text
                    ).parse()

                    try:
                        assert (
                            registra_result.get(current_parsed_result.source_name)
                            is None
                        ), (
                            f"Detected duplicated source name `{current_parsed_result.source_name}` "
                            f"in yaml file `{item}`, the same cluster name has been registered by another yaml file. "
                            f"Please check. "
                        )

                        # if the source registra is marked as inactive, we will skip the loading
                        if current_parsed_result.is_active:
                            # we use lower throughout in the registra keys so its easier to search
                            registra_result[
                                current_parsed_result.source_name.lower()
                            ] = current_parsed_result
                    except KeyError:
                        logger.error(
                            f"`{item}` yaml file seems to be malformed. Please verify. "
                        )
                        utils.log_full_traceback(logger)
                        sys.exit(1)

        return registra_result

    @staticmethod
    def _discover_registra_module_path(local_registra_path: str) -> List[str]:
        result = []
        try:
            files = os.listdir(local_registra_path)
        except FileNotFoundError:
            logger.warning(
                f"No files found when searching for local registra path at: {local_registra_path}, "
                f"ignore and take configs from constructor. "
            )
            return []

        for filename in files:
            if os.path.isdir(os.path.join(local_registra_path, filename)):

                # ignoring the model path, hardcoded
                # To be more generic, needs to add a ignore file
                if filename != "model":
                    result.append(filename)

        logger.debug(f"{len(result)} module paths discovered. ")

        return result

    @classmethod
    def set_registra_search_path(cls, prefix: str):
        """set the registra path searched by xenpy.
        If not provided, it will sync and look for the registra files
        """
        if settings.REGISTRA_PATH is None:
            logger.info(
                "REGISTRA PATH is not explicitly configured. Using default and actively synced for "
                "every execution. "
            )
            local_registra_path = os.path.join(
                constants.TEMP_FOLDER_DIRECTORY,
                "registra",
                prefix,
            )
            registra_bucket = S3Hook(constants.REGISTRA_S3_BUCKET_NAME)

            registra_bucket.bulk_download_files(
                s3_prefix=prefix,
                local_path=local_registra_path,
            )
            logger.info(f"Registra synced from s3 bucket into {local_registra_path}")
        else:
            local_registra_path = os.path.join(settings.REGISTRA_PATH, prefix)
            logger.info(
                f"REGISTRA PATH is explicitly configured. Locating REGISTRA files in path {local_registra_path}"
            )

        cls.local_registra_path = local_registra_path

    @classmethod
    def alert_extra(cls):
        for source_name, sr in cls.registra.items():
            for db_name, dr in sr.data_config.items():
                for tbl_name, tr in dr.tables.items():
                    if tr.extra:
                        logger.warning(
                            f"Table {source_name}-{db_name}-{tbl_name} found extra registra configs {tr.extra}, "
                            f"these are passed into the Pydantic model but will be ignored. "
                        )

    @staticmethod
    def _load_yaml_resource(file_path: str):
        try:
            with open(file_path, "r") as f:
                raw_yaml_text = f.read()
            return yaml.safe_load(raw_yaml_text)
        except (yaml.parser.ParserError, yaml.scanner.ScannerError):
            logger.error(
                f"The yaml file seems to be malformed. Please verify."
                f"Location: {file_path} "
            )
            utils.log_full_traceback(logger)
            sys.exit(1)

    @classmethod
    def has_table(cls, source_name: str, db_name: str, table_name: str) -> bool:
        assert cls.has_db(source_name=source_name, db_name=db_name), (
            f"Cluster name `{source_name}` and DB name `{db_name}` "
            f"not found when checking table_name"
        )
        return (
            table_name in cls.registra[source_name].data_config[db_name].tables.keys()
        )

    @classmethod
    def has_db(cls, source_name: str, db_name: str) -> bool:
        assert cls.has_source(
            source_name
        ), f"Cluster name `{source_name}` not found when checking for {db_name}"
        return db_name in cls._get_all_db_names_by_source_name(source_name)

    @classmethod
    def has_source(cls, source_name: str) -> bool:
        return source_name in cls.registra.keys()

    @classmethod
    def check_source_type(cls, source_name: str, source_type: models.SourceType):
        assert cls.has_source(source_name), (
            "Cluster not found in registra. Please ensure "
            "that registra availability is first validated "
            "in a prior logic. "
        )
        return source_type == cls.registra[source_name].source_type

    @classmethod
    def get_tbl_registra(
        cls, source_name: str, db_name: str, table_name: str
    ) -> Optional[models.BaseTableRegistraTemplate]:
        try:
            if cls.has_table(
                source_name=source_name, db_name=db_name, table_name=table_name
            ):
                return cls.registra[source_name].data_config[db_name].tables[table_name]
            else:
                logger.warning(
                    f"No table registra can be found for `{source_name}.{db_name}.{table_name}`. "
                    f"Please register it or check the registra file. "
                )
        except AssertionError:
            logger.warning(
                f"Registra is not found for `{source_name}.{db_name}.{table_name}`. Default to the original argument "
                "configuration"
            )

    @classmethod
    def get_source_registra(
        cls, source_name: str
    ) -> Optional[models.ReplicationSourceRegistra]:
        if cls.has_source(source_name=source_name):
            return cls.registra[source_name]

    @classmethod
    def get_db_registra(
        cls, source_name: str, db_name: str
    ) -> Optional[models.DBRegistra]:
        if cls.has_db(source_name=source_name, db_name=db_name):
            return cls.registra[source_name].data_config[db_name]

    @classmethod
    def get_all_available_registra_tasks(
        cls,
        source_name: Optional[str] = None,
        db_name: Optional[str] = None,
        tbl_name: Optional[str] = None,
    ) -> List[models.RegistraTask]:
        """
        get a list of registra tasks that fits the filtering logic,
        templated notebooks / airflow operators could be generated based on these

        It is simply a dataclass that contains the three identifiers,

        `source_name`, `db_name` and `tbl_name`

        The filtering logic:
            when all three arguments are provided, it should return just one tbl registra
            when only source and db name is provided, it should return all tbl registras in
                a list that belongs to the same db, same source
            when only source name is provided, the tbl registra should return everything under it
            when none is provided, it returns all loaded registra
        """
        if source_name is None:
            target_source_names = cls._get_all_source_names()
        else:
            target_source_names = [source_name]

        result = []
        for s in target_source_names:

            target_db_names = [db_name]

            if db_name is None:
                target_db_names = cls._get_all_db_names_by_source_name(s)

            for d in target_db_names:

                target_tbl_names = [tbl_name]

                if tbl_name is None:

                    target_tbl_names = cls._get_all_tbl_names_by_db_name(
                        source_name=s, db_name=d
                    )

                for t in target_tbl_names:

                    result.append(
                        models.RegistraTask(source_name=s, db_name=d, tbl_name=t)
                    )

        return result

    @classmethod
    def _get_all_tbl_names_by_db_name(cls, source_name, db_name):
        return cls.registra[source_name].data_config[db_name].tables.keys()

    @classmethod
    def _get_all_db_names_by_source_name(cls, source_name):
        assert cls.has_source(
            source_name
        ), f"Unable to find {source_name} when searching for db names"
        return cls.registra[source_name].data_config.keys()

    @classmethod
    def _get_all_source_names(cls):
        return cls.registra.keys()
