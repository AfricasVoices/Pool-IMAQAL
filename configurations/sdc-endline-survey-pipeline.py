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
    pipeline_name="SDC Endline Survey Pipeline",
    description="SDC Endline Survey Data Pipeline for analysis and reporting.",
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
                    FlowResultConfiguration("SDC_endline_ad", "sdc_endline_leadership_roles", "sdc_endline_leadership_roles"),
                    FlowResultConfiguration("SDC_endline_ad", "sdc_endline_income_generation", "sdc_endline_income_generating"),
                    FlowResultConfiguration("SDC_endline_ad", "sdc_endline_decision_making", "sdc_endline_decision_making"),
                    FlowResultConfiguration("SDC_endline_ad", "sdc_endline_accessing_aid", "sdc_endline_access_to_aid"),

                    FlowResultConfiguration("SDC_endline_demog", "imaqal_pool_district", "location"),
                    FlowResultConfiguration("SDC_endline_demog", "imaqal_pool_gender", "gender"),
                    FlowResultConfiguration("SDC_endline_demog", "imaqal_pool_age", "age"),
                    FlowResultConfiguration("SDC_endline_demog", "imaqal_pool_recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("SDC_endline_demog", "imaqal_pool_disability", "disability"),
                    FlowResultConfiguration("SDC_endline_demog", "imaqal_pool_household_language", "household_language"),
                ]
            )
        )
    ],
    csv_sources=[
        CSVSource(
            "gs://avf-project-datasets/2025/SDC-Endline/SDC-endline_leadership_roles_de_identified.csv",
            engagement_db_datasets=[CSVDatasetConfiguration("sdc_endline_leadership_roles")],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2025/SDC-Endline/SDC-endline_income_generating_de_identified.csv",
            engagement_db_datasets=[CSVDatasetConfiguration("sdc_endline_income_generating")],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2025/SDC-Endline/SDC-endline_decision_making_de_identified.csv",
            engagement_db_datasets=[CSVDatasetConfiguration("sdc_endline_decision_making")],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2025/SDC-Endline/SDC-endline_access_to_aid_de_identified.csv",
            engagement_db_datasets=[CSVDatasetConfiguration("sdc_endline_access_to_aid")],
            timezone="Africa/Mogadishu"
        ),
    ],
    kobotoolbox_sources=[
        KoboToolBoxSource(
            token_file_url="gs://avf-credentials/uraia-kobotoolbox-token.json",
            sync_config=KoboToolBoxToEngagementDBConfiguration(
                asset_uid="aNE8i5NzSM9ybwFdSqiaEa",
                ignore_invalid_mobile_numbers=True,
                question_configurations=[
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/SDC_endline_service_experience", engagement_db_dataset="sdc_endline_service_experience"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/SDC_endline_contributors", engagement_db_dataset="sdc_endline_contributors"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/SDC_endline_Challenges", engagement_db_dataset="sdc_endline_challenges"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/SDC_endline_service_satisfaction", engagement_db_dataset="sdc_endline_service_satisfaction"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/SDC_endline_community_voice", engagement_db_dataset="sdc_endline_community_voice"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_wj5hl79/SDC_endline_feedback", engagement_db_dataset="sdc_endline_feedback"),

                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/Gender", engagement_db_dataset="gender"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/Age", engagement_db_dataset="age"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/District", engagement_db_dataset="location"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/Disability", engagement_db_dataset="disability"),

                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/Household_Languages", engagement_db_dataset="household_language"),
                    KoboToolBoxQuestionConfiguration(data_column_name="group_dp2wm27/Displacement", engagement_db_dataset="recently_displaced"),
                ]
            )
        ),    
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                # RQA datasets
                make_rqa_coda_dataset_config("sdc_endline_leadership_roles"),
                make_rqa_coda_dataset_config("sdc_endline_income_generating"),
                make_rqa_coda_dataset_config("sdc_endline_decision_making"),
                make_rqa_coda_dataset_config("sdc_endline_access_to_aid"),

                make_rqa_coda_dataset_config("sdc_endline_service_experience"),
                make_rqa_coda_dataset_config("sdc_endline_contributors"),
                make_rqa_coda_dataset_config("sdc_endline_challenges"),
                make_rqa_coda_dataset_config("sdc_endline_service_satisfaction"),
                make_rqa_coda_dataset_config("sdc_endline_community_voice"),
                make_rqa_coda_dataset_config("sdc_endline_feedback"),
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
        bucket_dir_path="2025/SDC-Endline-Survey"
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="sdc_endline_analysis_outputs"
        ),
        dataset_configurations=[
            # RQA analysis datasets
            make_analysis_dataset_config("sdc_endline_leadership_roles"),
            make_analysis_dataset_config("sdc_endline_income_generating"),
            make_analysis_dataset_config("sdc_endline_decision_making"),
            make_analysis_dataset_config("sdc_endline_access_to_aid"),

            make_analysis_dataset_config("sdc_endline_service_experience"),
            make_analysis_dataset_config("sdc_endline_contributors"),
            make_analysis_dataset_config("sdc_endline_challenges"),
            make_analysis_dataset_config("sdc_endline_service_satisfaction"),
            make_analysis_dataset_config("sdc_endline_community_voice"),
            make_analysis_dataset_config("sdc_endline_feedback"),
            
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
    )
)
