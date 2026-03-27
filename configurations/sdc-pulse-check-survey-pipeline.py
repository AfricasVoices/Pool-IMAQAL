from core_data_modules.cleaners import Codes, somali
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="SDC Pulse Check Survey",
    description="Create an enabling environment for citizens to access better services, improve their participation in holding \
        authorities accountable in regard to service delivery in order to enhance stability in Somalia.",
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
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/imaqal-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("sdc_pulse_check_e02_activation", "rqa_e02", "sdc_pulse_check_e02"),
                    FlowResultConfiguration("sdc_pulse_check_e02_follow_up_activation", "rqa_e02_follow_up", "sdc_pulse_check_e02_follow_up"),

                    FlowResultConfiguration("sdc_pulse_check_e02_demog", "imaqal_pool_district", "location"),
                    FlowResultConfiguration("sdc_pulse_check_e02_demog", "imaqal_pool_gender", "gender"),
                    FlowResultConfiguration("sdc_pulse_check_e02_demog", "imaqal_pool_age", "age"),
                    FlowResultConfiguration("sdc_pulse_check_e02_demog", "imaqal_pool_recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("sdc_pulse_check_e02_demog", "imaqal_pool_disability", "disability"),
                    FlowResultConfiguration("sdc_pulse_check_e02_demog", "imaqal_pool_household_language", "household_language"),

                    # FlowResultConfiguration("sdc_pulse_check_e01_activation", "rqa_sdc_pulse_check_e01", "sdc_pulse_check_e01"),
                    # FlowResultConfiguration("sdc_pulse_check_e01_activation_sms_whatsapp", "rqa_sdc_pulse_check_e01", "sdc_pulse_check_e01"),

                    # FlowResultConfiguration("SDC_baseline_demog", "imaqal_pool_district", "location"),
                    # FlowResultConfiguration("SDC_baseline_demog", "imaqal_pool_gender", "gender"),
                    # FlowResultConfiguration("SDC_baseline_demog", "imaqal_pool_age", "age"),
                    # FlowResultConfiguration("SDC_baseline_demog", "imaqal_pool_recently_displaced", "recently_displaced"),
                    # FlowResultConfiguration("SDC_baseline_demog", "imaqal_pool_disability", "disability"),
                    # FlowResultConfiguration("SDC_baseline_demog", "imaqal_pool_household_language", "household_language")
                ]
            )
        )
    ],
    kobotoolbox_sources=[
        # KoboToolBoxSource(
        #     token_file_url="gs://avf-credentials/uraia-kobotoolbox-token.json",
        #     sync_config=KoboToolBoxToEngagementDBConfiguration(
        #         asset_uid="av5tEnBgrDMLGw79Pg2H77",
        #         ignore_invalid_mobile_numbers=True,
        #         question_configurations=[
        #             KoboToolBoxQuestionConfiguration(data_column_name="SDC_Pulse_Check_e01", engagement_db_dataset="sdc_pulse_check_e01"),

        #             KoboToolBoxQuestionConfiguration(data_column_name="Gender", engagement_db_dataset="gender"),
        #             KoboToolBoxQuestionConfiguration(data_column_name="Age", engagement_db_dataset="age"),
        #             KoboToolBoxQuestionConfiguration(data_column_name="District", engagement_db_dataset="location"),
        #             KoboToolBoxQuestionConfiguration(data_column_name="Disability", engagement_db_dataset="disability"),

        #             KoboToolBoxQuestionConfiguration(data_column_name="Household_languages", engagement_db_dataset="household_language"),
        #             KoboToolBoxQuestionConfiguration(data_column_name="Displaced", engagement_db_dataset="recently_displaced"),
        #         ]
        #     )
        # ),
        KoboToolBoxSource(
            token_file_url="gs://avf-credentials/uraia-kobotoolbox-token.json",
            sync_config=KoboToolBoxToEngagementDBConfiguration(
                asset_uid="aGvqV7QFT8XZ7r6PB3mY3W",
                ignore_invalid_mobile_numbers=True,
                question_configurations=[
                    KoboToolBoxQuestionConfiguration(data_column_name="sdc_pulse_check_rqa", engagement_db_dataset="sdc_pulse_check_e02"),
                    KoboToolBoxQuestionConfiguration(data_column_name="sdc_pulse_check_follow_up", engagement_db_dataset="sdc_pulse_check_e02_follow_up"),

                    KoboToolBoxQuestionConfiguration(data_column_name="Gender", engagement_db_dataset="gender"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Age", engagement_db_dataset="age"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Location", engagement_db_dataset="location"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Disability", engagement_db_dataset="disability"),

                    KoboToolBoxQuestionConfiguration(data_column_name="Household_Languages", engagement_db_dataset="household_language"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Displacement", engagement_db_dataset="recently_displaced"),
                ]
            )
        ), 
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_youth_governance_s01e01",
                    engagement_db_dataset="sdc_youth_governance_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_youth_governance_s01e01"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_youth_governance_s01e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Pulse_Check_e02",
                    engagement_db_dataset="sdc_pulse_check_e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_pulse_check_e02"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_pulse_check_e02"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Pulse_Check_e02_follow_up",
                    engagement_db_dataset="sdc_pulse_check_e02_follow_up",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_pulse_check_e02_follow_up"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_pulse_check_e02_follow_up"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Pulse_Check_e01",
                    engagement_db_dataset="sdc_pulse_check_e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_pulse_check_e01"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_pulse_check_e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_s03e01",
                    engagement_db_dataset="sdc_survey_s03e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_survey_s03e01"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_s03e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_s03e02",
                    engagement_db_dataset="sdc_survey_s03e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_survey_s03e02"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_s03e02"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Baseline_onboarding_responses",
                    engagement_db_dataset="sdc_baseline_onboarding_responses",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_baseline_onboarding_responses"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_baseline_onboarding_responses"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Baseline_service_experience",
                    engagement_db_dataset="sdc_baseline_service_experience",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_baseline_service_experience"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_baseline_service_experience"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Baseline_community_voice",
                    engagement_db_dataset="sdc_baseline_community_voice",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_baseline_community_voice"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_baseline_community_voice"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Baseline_service_satisfaction",
                    engagement_db_dataset="sdc_baseline_service_satisfaction",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_baseline_service_satisfaction"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_baseline_service_satisfaction"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_Survey_s03e03",
                    engagement_db_dataset="sdc_survey_s03e03",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_survey_s03e03"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_survey_s03e03"
                ),
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
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/household_language"), 
                                                coda_code_schemes_count=4, auto_coder=None)
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
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2023/SDC-Survey/coda_users.json",
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2025/SDC-Minority-Inclusion"
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="sdc_pulse_check_e02_analysis_outputs"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_pulse_check_e02"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_pulse_check_e02_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_pulse_check_e02"),
                        analysis_dataset="sdc_pulse_check_e02"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_pulse_check_e02_follow_up"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_pulse_check_e02_follow_up_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_pulse_check_e02_follow_up"),
                        analysis_dataset="sdc_pulse_check_e02_follow_up"
                    )
                ]
            ),
            # AnalysisDatasetConfiguration(
            #     engagement_db_datasets=["sdc_pulse_check_e01"],
            #     dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
            #     raw_dataset="sdc_pulse_check_e01_raw",
            #     coding_configs=[
            #         CodingConfiguration(
            #             code_scheme=load_code_scheme("rqas/sdc_pulse_check_e01"),
            #             analysis_dataset="sdc_pulse_check_e01"
            #         )
            #     ]
            # ),
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
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        maps=[],
        cross_tabs=[
            ("age_category", "gender"),
            ("age_category", "recently_displaced"),
            ("age_category", "disability"),
            ("age_category", "location"),
            ("gender", "age_category"),
            ("gender", "location"),
            ("gender", "recently_displaced"),
            ("gender", "disability"),
            ("location", "recently_displaced"),
        ],
    )
)
