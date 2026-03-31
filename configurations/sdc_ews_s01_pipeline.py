from core_data_modules.cleaners import Codes, somali
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="SDC S01 EWS Survey",
    description="SDC Somalia S01 EWS Survey",
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
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/imaqal-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("sdc_ews_s01_demog", "imaqal_pool_district", "location"),
                    FlowResultConfiguration("sdc_ews_s01_demog", "imaqal_pool_gender", "gender"),
                    FlowResultConfiguration("sdc_ews_s01_demog", "imaqal_pool_age", "age"),
                    FlowResultConfiguration("sdc_ews_s01_demog", "imaqal_pool_recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("sdc_ews_s01_demog", "imaqal_pool_disability", "disability"),
                    FlowResultConfiguration("sdc_ews_s01_demog", "imaqal_pool_household_language", "household_language"),
                    FlowResultConfiguration("sdc_ews_s01_demog", "is_agropastoralist", "agropastoralist_status"),

                    FlowResultConfiguration("sdc_ews_s01e01_activation", "rqa_s01e01", "sdc_ews_s01e01"),
                    FlowResultConfiguration("sdc_ews_s01e02_activation", "rqa_s01e02", "sdc_ews_s01e02"),

                    FlowResultConfiguration("sdc_ews_s01e03_activation", "rqa_s01e03", "sdc_ews_s01e03"),
                    FlowResultConfiguration("sdc_ews_s01e03_follow_up_activation", "rqa_s01e03_follow_up", "sdc_ews_s01e03_follow_up"),

                    FlowResultConfiguration("sdc_ews_s01e04_activation", "rqa_s01e04", "sdc_ews_s01e04"),

                    FlowResultConfiguration("sdc_ews_s01e1&2_sms_ad_new_contacts", "rqa_s01e01", "sdc_ews_s01e01"),
                    FlowResultConfiguration("sdc_ews_s01e1&2_sms_ad_new_contacts", "rqa_s01e02", "sdc_ews_s01e02"),
                ]
            )
        )
    ],
    kobotoolbox_sources=[
        KoboToolBoxSource(
            token_file_url="gs://avf-credentials/uraia-kobotoolbox-token.json",
            sync_config=KoboToolBoxToEngagementDBConfiguration(
                asset_uid="aSB5n98rXtUey7sXDz9jf3",
                ignore_invalid_mobile_numbers=True,
                question_configurations=[
                    KoboToolBoxQuestionConfiguration(data_column_name="SDC_EWS_S01e01", engagement_db_dataset="sdc_ews_s01e01"),
                    KoboToolBoxQuestionConfiguration(data_column_name="SDC_EWS_S01e02", engagement_db_dataset="sdc_ews_s01e02"),
                    KoboToolBoxQuestionConfiguration(data_column_name="SDC_EWS_S01e03", engagement_db_dataset="sdc_ews_s01e03"),
                    KoboToolBoxQuestionConfiguration(data_column_name="SDC_EWS_S01e04", engagement_db_dataset="sdc_ews_s01e04"),
                    KoboToolBoxQuestionConfiguration(data_column_name="SDC_EWS_S01e05", engagement_db_dataset="sdc_ews_s01e05"),
                    KoboToolBoxQuestionConfiguration(data_column_name="SDC_EWS_s01E06", engagement_db_dataset="sdc_ews_s01e06"),

                    KoboToolBoxQuestionConfiguration(data_column_name="Gender", engagement_db_dataset="gender"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Age", engagement_db_dataset="age"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Location", engagement_db_dataset="location"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Disability", engagement_db_dataset="disability"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Agropastoralism", engagement_db_dataset="agropastoralist_status"),

                    KoboToolBoxQuestionConfiguration(data_column_name="Household_Languages", engagement_db_dataset="household_language"),
                    KoboToolBoxQuestionConfiguration(data_column_name="Displacement", engagement_db_dataset="recently_displaced"),
                ]
            )
        ),
        KoboToolBoxSource(
            token_file_url="gs://avf-credentials/uraia-kobotoolbox-token.json",
            sync_config=KoboToolBoxToEngagementDBConfiguration(
                asset_uid="a6oPkbdQxddKTvtYftKfmH",
                ignore_invalid_mobile_numbers=True,
                question_configurations=[
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gs1dd35/SDC_EWS_S01e01", engagement_db_dataset="sdc_ews_s01e01"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gs1dd35/SDC_EWS_S01e02", engagement_db_dataset="sdc_ews_s01e02"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gs1dd35/SDC_EWS_S01e03", engagement_db_dataset="sdc_ews_s01e03"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gs1dd35/sdc_ews_s01e03_follow_up", engagement_db_dataset="sdc_ews_s01e03_follow_up"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gs1dd35/SDC_EWS_S01e04", engagement_db_dataset="sdc_ews_s01e04"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gs1dd35/SDC_EWS_S01e05", engagement_db_dataset="sdc_ews_s01e05"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_gs1dd35/SDC_EWS_s01E06", engagement_db_dataset="sdc_ews_s01e06"),

                    KoboToolBoxQuestionConfiguration(data_column_name="group_hp1ov89/Gender", engagement_db_dataset="gender"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_hp1ov89/Age", engagement_db_dataset="age"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_hp1ov89/Location", engagement_db_dataset="location_2"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_hp1ov89/Disability", engagement_db_dataset="disability"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_hp1ov89/Agropastoralism", engagement_db_dataset="agropastoralist_status")
                ]
            )
        ),     
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_EWS_s01e01",
                    engagement_db_dataset="sdc_ews_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_ews_s01e01"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_ews_s01e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_EWS_s01e02",
                    engagement_db_dataset="sdc_ews_s01e02",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_ews_s01e02"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_ews_s01e02"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_EWS_s01e03",
                    engagement_db_dataset="sdc_ews_s01e03",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_ews_s01e03"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_ews_s01e03"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_EWS_s01e04",
                    engagement_db_dataset="sdc_ews_s01e04",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_ews_s01e04"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_ews_s01e04"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_EWS_s01e05",
                    engagement_db_dataset="sdc_ews_s01e05",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_ews_s01e05"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_ews_s01e05"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_EWS_s01e06",
                    engagement_db_dataset="sdc_ews_s01e06",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_ews_s01e06"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_ews_s01e06"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="SDC_EWS_s01e03_follow_up",
                    engagement_db_dataset="sdc_ews_s01e03_follow_up",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/sdc_ews_s01e03_follow_up"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="sdc_ews_s01e03_follow_up"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_agropastoralist_status",
                    engagement_db_dataset="agropastoralist_status",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/agropastoralist_status"),
                                                coda_code_schemes_count=3),
                    ],
                    ws_code_match_value="agropastoralist_status"
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
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/household_language"), auto_coder=None)
                    ],
                    ws_code_match_value="household_language",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_KE_location",
                    engagement_db_dataset="location_2",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_ward"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_constituency"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("demographics/kenya_county"), auto_coder=None)
                    ],
                    ws_code_match_value="location_2"
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
        bucket_dir_path="2023/SDC-Survey"
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="ews-analysis-output"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_ews_s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_ews_s01e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_ews_s01e01"),
                        analysis_dataset="sdc_ews_s01e01"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_ews_s01e02"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_ews_s01e02_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_ews_s01e02"),
                        analysis_dataset="sdc_ews_s01e02"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_ews_s01e03"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_ews_s01e03_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_ews_s01e03"),
                        analysis_dataset="sdc_ews_s01e03"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_ews_s01e04"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_ews_s01e04_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_ews_s01e04"),
                        analysis_dataset="sdc_ews_s01e04"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_ews_s01e05"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_ews_s01e05_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_ews_s01e05"),
                        analysis_dataset="sdc_ews_s01e05"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_ews_s01e06"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_ews_s01e06_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_ews_s01e06"),
                        analysis_dataset="sdc_ews_s01e06"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["sdc_ews_s01e03_follow_up"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="sdc_ews_s01e03_follow_up_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/sdc_ews_s01e03_follow_up"),
                        analysis_dataset="sdc_ews_s01e03_follow_up"
                    )
                ]
            ),
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
                engagement_db_datasets=["agropastoralist_status"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="agropastoralist_status_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/agropastoralist_status"),
                        analysis_dataset="agropastoralist_status"
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
                engagement_db_datasets=["location_2"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="location_2_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/kenya_county"),
                        analysis_dataset="kenya_county",
                        analysis_location=AnalysisLocations.KENYA_COUNTY
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/kenya_constituency"),
                        analysis_dataset="kenya_constituency",
                        analysis_location=AnalysisLocations.KENYA_CONSTITUENCY
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("demographics/kenya_ward"),
                        analysis_dataset="kenya_ward",
                        analysis_location=AnalysisLocations.KENYA_WARD
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
        traffic_labels=[
            TrafficLabel(isoparse("2026-02-14T00:00+03:00"), isoparse("2026-02-14T24:00+03:00"), "E01 PSA Audiograms"),
            TrafficLabel(isoparse("2026-02-15T00:00+03:00"), isoparse("2026-02-15T24:00+03:00"), "E01 PSA Audiograms & Radio PSAs"),
            TrafficLabel(isoparse("2026-02-16T00:00+03:00"), isoparse("2026-02-17T20:30+03:00"), "E01 Radio PSAs"),
            TrafficLabel(isoparse("2026-02-17T20:30+03:00"), isoparse("2026-02-17T24:00+03:00"), "E01 Live Discussion"),
            TrafficLabel(isoparse("2026-02-18T07:00+03:00"), isoparse("2026-02-18T24:00+03:00"), "E01 Radio Show"),

            TrafficLabel(isoparse("2026-02-21T00:00+03:00"), isoparse("2026-02-21T24:00+03:00"), "E02 PSA Audiograms"),
            TrafficLabel(isoparse("2026-02-22T00:00+03:00"), isoparse("2026-02-22T24:00+03:00"), "E02 PSA Audiograms & Radio PSAs"),
            TrafficLabel(isoparse("2026-02-23T00:00+03:00"), isoparse("2026-02-24T20:30+03:00"), "E02 Radio PSAs"),
            TrafficLabel(isoparse("2026-02-24T20:30+03:00"), isoparse("2026-02-24T24:00+03:00"), "E02 Live Discussion"),
            TrafficLabel(isoparse("2026-02-25T07:00+03:00"), isoparse("2026-02-25T24:00+03:00"), "E02 Radio Show"),

            TrafficLabel(isoparse("2026-02-28T00:00+03:00"), isoparse("2026-02-28T24:00+03:00"), "E03 PSA Audiograms"),
            TrafficLabel(isoparse("2026-03-01T00:00+03:00"), isoparse("2026-03-01T24:00+03:00"), "E03 PSA Audiograms & Radio PSAs"),
            TrafficLabel(isoparse("2026-03-02T00:00+03:00"), isoparse("2026-03-03T20:30+03:00"), "E03 Radio PSAs"),
            TrafficLabel(isoparse("2026-03-03T20:30+03:00"), isoparse("2026-03-03T24:00+03:00"), "E03 Live Discussion"),
            TrafficLabel(isoparse("2026-03-04T07:00+03:00"), isoparse("2026-03-04T24:00+03:00"), "E03 Radio Show"),

            TrafficLabel(isoparse("2026-03-07T00:00+03:00"), isoparse("2026-03-07T24:00+03:00"), "E04 PSA Audiograms"),
            TrafficLabel(isoparse("2026-03-08T00:00+03:00"), isoparse("2026-03-08T24:00+03:00"), "E04 PSA Audiograms & Radio PSAs"),
            TrafficLabel(isoparse("2026-03-09T00:00+03:00"), isoparse("2026-03-10T20:30+03:00"), "E04 Radio PSAs"),
            TrafficLabel(isoparse("2026-03-10T20:30+03:00"), isoparse("2026-03-10T24:00+03:00"), "E04 Live Discussion"),
            TrafficLabel(isoparse("2026-03-11T07:00+03:00"), isoparse("2026-03-11T24:00+03:00"), "E04 Radio Show"),
        ]
    )
)
