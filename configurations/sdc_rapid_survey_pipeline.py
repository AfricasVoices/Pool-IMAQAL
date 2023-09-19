from core_data_modules.cleaners import Codes, somali
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="SDC Rapid SMS survey",
    description="Impact Assessment, Minority Inclusion, and Service Delivery Evaluation on SDC Projects in Somalia",
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
        # Sync back to IMAQAL-2 for now because the current IMAQAL pool has duplicated CSV messages that need
        # understanding.
        # TODO: Overwrite the IMAQAL pool with the current IMAQAL-2 pool and change this ref to use that IMAQAL
        #       pool as soon as possible.
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
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/imaqal-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("sdc_survey_demog", "imaqal_pool_district", "location"),
                    FlowResultConfiguration("sdc_survey_demog", "imaqal_pool_gender", "gender"),
                    FlowResultConfiguration("sdc_survey_demog", "imaqal_pool_age", "age"),
                    FlowResultConfiguration("sdc_survey_demog", "imaqal_pool_recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("sdc_survey_demog", "imaqal_pool_disability", "disability"),
                    FlowResultConfiguration("sdc_survey_demog", "imaqal_pool_household_language", "household_language"),

                    FlowResultConfiguration("sdc_survey_SIRA_s01_sms_ad", "sdc_survey_sira_complains_feedback", "sdc_survey_sira_complains_feedback"),
                    FlowResultConfiguration("sdc_survey_SIRA_s01_sms_ad", "sdc_survey_sira_government_services", "sdc_survey_sira_government_services"),
                    FlowResultConfiguration("sdc_survey_SIRA_s01_sms_ad", "sdc_survey_sira_inclusion", "sdc_survey_sira_inclusion"),
                    FlowResultConfiguration("sdc_survey_SIRA_s01_sms_ad", "sdc_survey_sira_suggestions", "sdc_survey_sira_suggestions"),
                    FlowResultConfiguration("sdc_survey_SIRA_s01_sms_ad", "sdc_survey_sira_support", "sdc_survey_sira_support"),

                    FlowResultConfiguration("sdc_survey_CHASP_s01_sms_ad", "sdc_survey_chasp_complains_feedback", "sdc_survey_chasp_complains_feedback"),
                    FlowResultConfiguration("sdc_survey_CHASP_s01_sms_ad", "sdc_survey_chasp_government_services", "sdc_survey_chasp_government_services"),
                    FlowResultConfiguration("sdc_survey_CHASP_s01_sms_ad", "sdc_survey_chasp_inclusion", "sdc_survey_chasp_inclusion"),
                    FlowResultConfiguration("sdc_survey_CHASP_s01_sms_ad", "sdc_survey_chasp_suggestions", "sdc_survey_chasp_suggestions"),
                    FlowResultConfiguration("sdc_survey_CHASP_s01_sms_ad", "sdc_survey_chasp_support", "sdc_survey_chasp_support"),

                    FlowResultConfiguration("sdc_survey_SOMREP_s01_sms_ad", "sdc_survey_somrep_feedback_mechanism", "sdc_survey_somrep_feedback_mechanism"),
                    FlowResultConfiguration("sdc_survey_SOMREP_s01_sms_ad", "sdc_survey_somrep_government_services", "sdc_survey_somrep_government_services"),
                    FlowResultConfiguration("sdc_survey_SOMREP_s01_sms_ad", "sdc_survey_somrep_inclusion", "sdc_survey_somrep_inclusion"),
                    FlowResultConfiguration("sdc_survey_SOMREP_s01_sms_ad", "sdc_survey_somrep_resilience_support", "sdc_survey_somrep_resilience_support"),
                    FlowResultConfiguration("sdc_survey_SOMREP_s01_sms_ad", "sdc_survey_somrep_suggestions", "sdc_survey_somrep_suggestions"),
                ]
            )
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                # SDC Survey SIRA
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SIRA_complains_feedback",
                    engagement_db_dataset="sdc_survey_sira_complains_feedback",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/sira_complains_feedback"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_sira_complains_feedback"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SIRA_government_services",
                    engagement_db_dataset="sdc_survey_sira_government_services",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/sira_government_services"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_sira_government_services"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SIRA_inclusion",
                    engagement_db_dataset="sdc_survey_sira_inclusion",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/sira_inclusion"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_sira_inclusion"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SIRA_suggestions",
                    engagement_db_dataset="sdc_survey_sira_suggestions",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/sira_suggestions"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_sira_suggestions"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SIRA_support",
                    engagement_db_dataset="sdc_survey_sira_support",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/sira_support"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_sira_support"
                ),

                # SDC Survey CHASP
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_CHASP_complains_feedback",
                    engagement_db_dataset="sdc_survey_chasp_complains_feedback",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_complains_feedback"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_chasp_complains_feedback"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_CHASP_government_services",
                    engagement_db_dataset="sdc_survey_chasp_government_services",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_government_services"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_chasp_government_services"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_CHASP_inclusion",
                    engagement_db_dataset="sdc_survey_chasp_inclusion",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_inclusion"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_chasp_inclusion"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_CHASP_suggestions",
                    engagement_db_dataset="sdc_survey_chasp_suggestions",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_suggestions"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_chasp_suggestions"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_CHASP_support",
                    engagement_db_dataset="sdc_survey_chasp_support",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_support"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_chasp_support"
                ),

                # SDC Survey SOMREP
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SOMREP_feedback_mechanism",
                    engagement_db_dataset="sdc_survey_somrep_feedback_mechanism",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_feedback_mechanism"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_somrep_feedback_mechanism"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SOMREP_government_services",
                    engagement_db_dataset="sdc_survey_somrep_government_services",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_government_services"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_somrep_government_services"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SOMREP_inclusion",
                    engagement_db_dataset="sdc_survey_somrep_inclusion",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_inclusion"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_somrep_inclusion"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SOMREP_resilience_support",
                    engagement_db_dataset="sdc_survey_somrep_resilience_support",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_resilience_support"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_somrep_resilience_support"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_SOMREP_suggestions",
                    engagement_db_dataset="sdc_survey_somrep_suggestions",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_suggestions"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_somrep_suggestions"
                ),

                # Demogs
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
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2023/SDC-Somalia-Health/coda_users.json"
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2023/SDC-Somalia-Health"
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="sdc_survey_analysis_outputs"
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
            # SDC Survey SIRA
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_sira_complains_feedback"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sira_complains_feedback_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/sira_complains_feedback"),
                        analysis_dataset="sira_complains_feedback"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_sira_government_services"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sira_government_services_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/sira_government_services"),
                        analysis_dataset="sira_government_services"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_sira_inclusion"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sira_inclusion_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/sira_inclusion"),
                        analysis_dataset="sira_inclusion"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_sira_suggestions"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sira_suggestions_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/sira_suggestions"),
                        analysis_dataset="sira_suggestions"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_sira_support"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sira_support_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/sira_support"),
                        analysis_dataset="sira_support"
                    )
                ]
            ),

            # SDC Survey CHASP
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_chasp_complains_feedback"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="chasp_complains_feedback_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_complains_feedback"),
                        analysis_dataset="chasp_complains_feedback"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_chasp_government_services"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="chasp_government_services_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_government_services"),
                        analysis_dataset="chasp_government_services"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_chasp_inclusion"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="chasp_inclusion_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_inclusion"),
                        analysis_dataset="chasp_inclusion"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_chasp_suggestions"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="chasp_suggestions_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_suggestions"),
                        analysis_dataset="chasp_suggestions"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_chasp_support"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="chasp_support_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/chasp_support"),
                        analysis_dataset="chasp_support"
                    )
                ]
            ),

            # SDC Survey SOMREP
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_somrep_feedback_mechanism"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="somrep_feedback_mechanism_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_feedback_mechanism"),
                        analysis_dataset="somrep_feedback_mechanism"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_somrep_government_services"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="somrep_government_services_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_government_services"),
                        analysis_dataset="somrep_government_services"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_somrep_inclusion"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="somrep_inclusion_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_inclusion"),
                        analysis_dataset="somrep_inclusion"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_somrep_resilience_support"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="somrep_resilience_support_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_resilience_support"),
                        analysis_dataset="somrep_resilience_support"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_survey_somrep_suggestions"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="somrep_suggestions_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_somalia/somrep_suggestions"),
                        analysis_dataset="somrep_suggestions"
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
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        traffic_labels=[]
    )
)
