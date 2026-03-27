from core_data_modules.cleaners import Codes, somali
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


def make_analysis_dataset_config(dataset_name, dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER):
    return AnalysisDatasetConfiguration(
        engagement_db_datasets=[dataset_name.strip()],
        dataset_type=dataset_type,
        raw_dataset=f"{dataset_name}_raw".strip(),
        coding_configs=[
            CodingConfiguration(
                code_scheme=load_code_scheme(f"rqas/{dataset_name.strip()}"),
                analysis_dataset=dataset_name.strip()
            )
        ]
    )

def make_rqa_coda_dataset_config(coda_dataset_id):
    return CodaDatasetConfiguration(
        coda_dataset_id=coda_dataset_id.strip(),
        engagement_db_dataset=coda_dataset_id.lower().strip(),
        code_scheme_configurations=[
            CodeSchemeConfiguration(code_scheme=load_code_scheme(f"rqas/{coda_dataset_id.lower().strip()}"),
                                    auto_coder=None, coda_code_schemes_count=3),
        ],
        ws_code_match_value=coda_dataset_id.lower().strip()
    )

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="SDC SIRA S03 Rapid Survey Pipeline",
    description="SDC Somalia SIRA S03 Rapid Survey data pipeline",
    test_participant_uuids=[
        "avf-participant-uuid-368c7741-7034-474a-9a87-6ae32a51f5a0",
        "avf-participant-uuid-5ca68e07-3dba-484b-a29c-7a6c989036b7",
        "avf-participant-uuid-45d15c2d-623c-4f89-bd91-7518147bf1dc",
        "avf-participant-uuid-88ef05ba-4c56-41f8-a00c-29104abab73e",
        "avf-participant-uuid-d9745740-3da5-43cc-a9d1-37fccb75380b",
        "avf-participant-uuid-96ff0ba1-a7df-4715-84c5-9c90e9093eb4"
    ],
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-engagement-databases-firebase-credentials-file.json",
        database_path="engagement_databases/IMAQAL-2"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-id-infrastructure-firebase-adminsdk-6xps8-b9173f2bfd.json",
        table_name="avf-global-urn-to-participant-uuid",
        uuid_prefix="avf-participant-uuid-"
    ),
    operations_dashboard=OperationsDashboardConfiguration(
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json"
    ),
    kobotoolbox_sources=[
        KoboToolBoxSource(
            token_file_url="gs://avf-credentials/uraia-kobotoolbox-token.json",
            sync_config=KoboToolBoxToEngagementDBConfiguration(
                asset_uid="aL3jCmsNjGEEDWtTuou5QE",
                ignore_invalid_mobile_numbers=True,
                question_configurations=[
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Support_level", engagement_db_dataset="sdc_sira_s03_support_level"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Support_improvements", engagement_db_dataset="sdc_sira_s03_support_improvements"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Feedback_level", engagement_db_dataset="sdc_sira_s03_feedback_level"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Feedback_Improvements", engagement_db_dataset="sdc_sira_s03_feedback_improvements"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Inclusion_level", engagement_db_dataset="sdc_sira_s03_inclusion_level"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Inclusion_Improvements", engagement_db_dataset="sdc_sira_s03_inclusion_improvements"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Government_level", engagement_db_dataset="sdc_sira_s03_government_level"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_kq2zt25/Government_Improvements", engagement_db_dataset="sdc_sira_s03_government_improvements"),

                    KoboToolBoxQuestionConfiguration(data_column_name="group_gb1db76/Location", engagement_db_dataset="location"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gb1db76/Household_Languages", engagement_db_dataset="household_language"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gb1db76/Gender", engagement_db_dataset="gender"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gb1db76/Age", engagement_db_dataset="age"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gb1db76/Disability", engagement_db_dataset="disability"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gb1db76/Displacement", engagement_db_dataset="recently_displaced"),
                ]
            )
        ),    
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/age"),
                                                auto_coder=lambda text: str(
                                                    somali.DemographicCleaner.clean_age_within_range(text))
                                                ),
                    ],
                    ws_code_match_value="age",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/gender"),
                                                auto_coder=somali.DemographicCleaner.clean_gender)
                    ],
                    ws_code_match_value="gender",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/household_language"), auto_coder=None)
                    ],
                    ws_code_match_value="household_language",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/mogadishu_sub_district"),
                                                auto_coder=somali.DemographicCleaner.clean_mogadishu_sub_district),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_district"), auto_coder=lambda text:
                        somali.DemographicCleaner.clean_somalia_district(text)
                        if somali.DemographicCleaner.clean_mogadishu_sub_district == Codes.NOT_CODED else Codes.NOT_CODED),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_region"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_state"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/somalia_zone"), auto_coder=None),
                    ],
                    ws_code_match_value="location",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_recently_displaced",
                    engagement_db_dataset="recently_displaced",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/recently_displaced"),
                                                auto_coder=somali.DemographicCleaner.clean_yes_no)
                    ],
                    ws_code_match_value="recently_displaced",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_disability",
                    engagement_db_dataset="disability",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/disability"),
                                                auto_coder=somali.DemographicCleaner.clean_yes_no)
                    ],
                    ws_code_match_value="disability",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_clan",
                    engagement_db_dataset="clan",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/clan"))
                    ],
                    ws_code_match_value="clan",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),

                # RQA datasets
                make_rqa_coda_dataset_config("sdc_sira_s03_support_level"),
                make_rqa_coda_dataset_config("sdc_sira_s03_support_improvements"),
                make_rqa_coda_dataset_config("sdc_sira_s03_feedback_level"),
                make_rqa_coda_dataset_config("sdc_sira_s03_feedback_improvements"),
                make_rqa_coda_dataset_config("sdc_sira_s03_inclusion_level"),
                make_rqa_coda_dataset_config("sdc_sira_s03_inclusion_improvements"),
                make_rqa_coda_dataset_config("sdc_sira_s03_government_level"),
                make_rqa_coda_dataset_config("sdc_sira_s03_government_improvements"),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2023/SDC-Survey/coda_users.json",
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2023/SDC-Survey"
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="sdc_sira_s03_analysis_outputs"
        ),
        dataset_configurations=[
            OperatorDatasetConfiguration(
                raw_dataset="operator_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/imaqal_operator"),
                        analysis_dataset="operator",
                        analysis_location=AnalysisLocations.SOMALIA_OPERATOR
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/age_category"),
                        analysis_dataset="age_category",
                        age_category_config=AgeCategoryConfiguration(
                            age_analysis_dataset="age",
                            categories={
                                (10, 14): "10 to 14",
                                (15, 17): "15 to 17",
                                (18, 35): "18 to 35",
                                (36, 54): "36 to 54",
                                (55, 99): "55 to 99"
                            }
                        )
                    ),
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["gender"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="gender_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/gender"),
                        analysis_dataset="gender"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["location"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="location_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/mogadishu_sub_district"),
                        analysis_dataset="mogadishu_sub_district",
                        analysis_location=AnalysisLocations.MOGADISHU_SUB_DISTRICT
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_district"),
                        analysis_dataset="district",
                        analysis_location=AnalysisLocations.SOMALIA_DISTRICT
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_region"),
                        analysis_dataset="region",
                        analysis_location=AnalysisLocations.SOMALIA_REGION
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_state"),
                        analysis_dataset="state",
                        analysis_location=AnalysisLocations.SOMALIA_STATE
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/somalia_zone"),
                        analysis_dataset="zone",
                        analysis_location=AnalysisLocations.SOMALIA_ZONE
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["recently_displaced"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="recently_displaced",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/recently_displaced"),
                        analysis_dataset="recently_displaced"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["disability"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="disability",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/disability"),
                        analysis_dataset="disability"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["household_language"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="household_language",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/household_language"),
                        analysis_dataset="household_language"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["clan"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="clan",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/clan"),
                        analysis_dataset="clan"
                    )
                ]
            ),

            # RQA analysis datasets
            make_analysis_dataset_config("sdc_sira_s03_support_level"),
            make_analysis_dataset_config("sdc_sira_s03_support_improvements"),
            make_analysis_dataset_config("sdc_sira_s03_feedback_level"),
            make_analysis_dataset_config("sdc_sira_s03_feedback_improvements"),
            make_analysis_dataset_config("sdc_sira_s03_inclusion_level"),
            make_analysis_dataset_config("sdc_sira_s03_inclusion_improvements"),
            make_analysis_dataset_config("sdc_sira_s03_government_level"),
            make_analysis_dataset_config("sdc_sira_s03_government_improvements"),
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        traffic_labels=[]
    )
)
