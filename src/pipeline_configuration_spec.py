import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List

from core_data_modules.data_models import CodeScheme
from core_data_modules.analysis.traffic_analysis import TrafficLabel

from src.common.configuration import (RapidProClientConfiguration, CodaClientConfiguration, UUIDTableClientConfiguration,
                                      EngagementDatabaseClientConfiguration, OperationsDashboardConfiguration,
                                      ArchiveConfiguration)
from src.csv_to_engagement_db.configuration import (CSVSource, CSVDatasetConfiguration)

from src.engagement_db_coda_sync.configuration import (CodaSyncConfiguration, CodaDatasetConfiguration,
                                                       CodeSchemeConfiguration)
from src.engagement_db_to_rapid_pro.configuration import (EngagementDBToRapidProConfiguration, DatasetConfiguration,
                                                          WriteModes, ContactField)
from src.rapid_pro_to_engagement_db.configuration import (FlowResultConfiguration, UuidFilter,
                                                          RapidProToEngagementDBConfiguration)
from src.engagement_db_to_analysis.configuration import (AnalysisDatasetConfiguration, OperatorDatasetConfiguration,
                                                         DatasetTypes, AgeCategoryConfiguration, AnalysisLocations,
                                                         CodingConfiguration, GoogleDriveUploadConfiguration,
                                                         MembershipGroupConfiguration, AnalysisConfiguration)


def load_code_scheme(fname):
    with open(f"code_schemes/{fname}.json") as f:
        return CodeScheme.from_firebase_map(json.load(f))


def make_rqa_coda_dataset_configs(dataset_name_prefix, coda_dataset_id_prefix, code_scheme_prefix, number_of_datasets, update_users_and_code_schemes=True):
    """
    Creates a list of n rqa coda dataset configs, indexed from 1 to `number_of_datasets`.
    This allows us to configure the highly repetitive rqa configurations very succinctly.
    Note handles rqas less than 10
    """
    dataset_configs = []
    for i in range(1, number_of_datasets + 1):
        dataset_configs.append(
            CodaDatasetConfiguration(
                coda_dataset_id=f"{coda_dataset_id_prefix}{i}",
                engagement_db_dataset=f"{dataset_name_prefix}{i}",
                code_scheme_configurations=[
                    CodeSchemeConfiguration(
                        code_scheme=load_code_scheme(f"{code_scheme_prefix}{i}"),
                        auto_coder=None, coda_code_schemes_count=3)
                ],
                ws_code_match_value=f"{dataset_name_prefix}{i}",
                update_users_and_code_schemes=update_users_and_code_schemes
            )
        )
    return dataset_configs

def make_rqa_analysis_dataset_configs(dataset_name_prefix, code_scheme_prefix, number_of_datasets):
    dataset_configs = []
    for i in range(3, number_of_datasets + 1):
        dataset_configs.append(
            AnalysisDatasetConfiguration(
                engagement_db_datasets=[f"{dataset_name_prefix}{i}"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset=f"{dataset_name_prefix}{i}_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme(f"{code_scheme_prefix}{i}"),
                        analysis_dataset=f"{code_scheme_prefix}{i}"
                    )
                ]
            )
        )
    return dataset_configs


@dataclass
class RapidProSource:
    rapid_pro: RapidProClientConfiguration
    sync_config: RapidProToEngagementDBConfiguration


@dataclass
class CodaConfiguration:
    coda: CodaClientConfiguration
    sync_config: CodaSyncConfiguration


@dataclass
class RapidProTarget:
    rapid_pro: RapidProClientConfiguration
    sync_config: EngagementDBToRapidProConfiguration


@dataclass
class PipelineConfiguration:
    pipeline_name: str

    engagement_database: EngagementDatabaseClientConfiguration
    uuid_table: UUIDTableClientConfiguration
    operations_dashboard: OperationsDashboardConfiguration
    archive_configuration: ArchiveConfiguration
    project_start_date: datetime = None
    project_end_date: datetime = None
    test_participant_uuids: [] = None
    description: str = None
    rapid_pro_sources: [RapidProSource] = None
    csv_sources: Optional[List[CSVSource]] = None
    coda_sync: CodaConfiguration = None
    rapid_pro_target: RapidProTarget = None
    analysis: AnalysisConfiguration = None
