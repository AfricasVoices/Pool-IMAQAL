from core_data_modules.cleaners import Codes, somali
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="WorldBank-SCD",
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
                    FlowResultConfiguration("worldbank_scd_demog", "imaqal_pool_district", "location"),
                    FlowResultConfiguration("worldbank_scd_demog", "imaqal_pool_gender", "gender"),
                    FlowResultConfiguration("worldbank_scd_demog", "imaqal_pool_age", "age"),
                    FlowResultConfiguration("worldbank_scd_demog", "imaqal_pool_recently_displaced", "recently_displaced"),
                    FlowResultConfiguration("worldbank_scd_demog", "imaqal_pool_household_language", "household_language"),
                    FlowResultConfiguration("worldbank_scd_demog", "imaqal_pool_disability", "disability"),

                    FlowResultConfiguration("worldbank_scd_s01e01_activation", "worldbank_scd_s01e01", "worldbank_scd_s01e01"),

                    # We captured the follow-ups in 2 places, so we can send a thank-you reply the first time we hear
                    # from someone, while silently collecting any future messages we get later.
                    FlowResultConfiguration("worldbank_scd_s01e01_follow_up_1_ad", "worldbank_scd_s01e01_follow_up_1", "worldbank_scd_s01e01_follow_up_1"),
                    FlowResultConfiguration("worldbank_scd_s01e01_follow_up_1_activation", "worldbank_scd_s01e01_follow_up_1", "worldbank_scd_s01e01_follow_up_1"),

                    FlowResultConfiguration("worldbank_scd_s01e01_follow_up_2_ad", "worldbank_scd_s01e01_follow_up_2", "worldbank_scd_s01e01_follow_up_2"),
                    FlowResultConfiguration("worldbank_scd_s01e01_follow_up_2_activation", "worldbank_scd_s01e01_follow_up_2", "worldbank_scd_s01e01_follow_up_2"),

                    # Evaluation asked and captured 2 questions (have voice & suggestions).
                    # We captured any other messages sent during this period into the evaluation_activation flow.
                    FlowResultConfiguration("worldbank_scd_s01_evaluation", "worldbank_scd_s01_have_voice", "worldbank_scd_s01_have_voice"),
                    FlowResultConfiguration("worldbank_scd_s01_evaluation", "worldbank_scd_s01_suggestions", "worldbank_scd_s01_suggestions"),
                    FlowResultConfiguration("worldbank_scd_s01_evaluation_activation", "worldbank_scd_s01_evaluation", "worldbank_scd_s01_evaluation"),
                ]
            )
        )
    ],
    csv_sources=[
        CSVSource(
            "gs://avf-project-datasets/2022/WorldBank-SCD/recovered_hormuud_2022_12_de_identified.csv",
            engagement_db_datasets=[
                # Recovered data is mainly from outages caused when running adverts, so add all recovered data to the
                # e01 dataset.
                CSVDatasetConfiguration("worldbank_scd_s01e01"),
            ],
            timezone="Africa/Mogadishu"
        )
    ],
    google_form_sources=[
        GoogleFormSource(
            # https://docs.google.com/forms/d/1bcXYzzvLK4zboc6BFISswRe2Gv4QtsshdE3Y0riXztM
            google_form_client=GoogleFormsClientConfiguration(
                credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json"
            ),
            sync_config=GoogleFormToEngagementDBConfiguration(
                form_id="1bcXYzzvLK4zboc6BFISswRe2Gv4QtsshdE3Y0riXztM",
                question_configurations=[
                    QuestionConfiguration(engagement_db_dataset="worldbank_scd_s01e01_google_form_consent", question_titles=["Do you consent to participate in the survey?"]),
                    QuestionConfiguration(engagement_db_dataset="worldbank_scd_s01e01", question_titles=["Maxay yihiin labada sheey ee ay tahay mudnaanta in lasiiyo si loo hormariyo daryeelka bulshada sadexda sane ee soo socota?\nWhat are two priorities that would improve your community’s welfare in the next three years?"]),
                    QuestionConfiguration(engagement_db_dataset="worldbank_scd_s01e01_follow_up_1", question_titles=["Aragtidaada, waa maxay caqabadaha ka hortaagan Soomaaliya inay cirib tirto saboolnimada?\nIn your opinion, what challenges are preventing Somalia from ending poverty?"]),
                    QuestionConfiguration(engagement_db_dataset="worldbank_scd_s01e01_follow_up_2", question_titles=["Yaa ka mas`uul ah go`aan qaadashada muhiimka ah gudaha bulshadaada?\nWho is responsible for making important decisions in your community?"]),

                    QuestionConfiguration(engagement_db_dataset="location", question_titles=["Degmadee ayaad ku nooshahay?\nIn which district of Somalia do you currently live?"]),
                    QuestionConfiguration(engagement_db_dataset="gender", question_titles=["Mahadsanid. Ma waxaad tahay Rag mise Dumar? Fadlan kaga jawaab Rag ama Dumar.\nWhat is your gender?"]),
                    QuestionConfiguration(engagement_db_dataset="age", question_titles=["Da'daadu maxay tahay? Fadlan kaga jawaab tiro.\nHow old are you? Please answer with a number in years."]),
                    QuestionConfiguration(engagement_db_dataset="recently_displaced", question_titles=["Ma waxaad tahay qof soo barakacay dhawaan? Hadii haa ay tahay jawaabtadu, Maxa kusoo barakiciyay?\nAre you currently displaced? If so, what made you leave your home?"]),
                    QuestionConfiguration(engagement_db_dataset="disability", question_titles=["Wax naafo ah miyaad leedahay? Haa/Maya\nDo you have a disability? Yes/No"]),
                    QuestionConfiguration(engagement_db_dataset="household_language", question_titles=["Luuqaddee ayaad caadi ahaan gurigiinna dhexdiisa uga hadashaan?\nWhat language do you usually speak in your household?"])
                ]
            )
        ),
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldBank_SCD_s01e01",
                    engagement_db_dataset="worldbank_scd_s01e01",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01"),
                                                coda_code_schemes_count=3
                                                ),
                    ],
                    ws_code_match_value="worldbank_scd_s01e01"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldBank_SCD_s01e01_follow_up_1",
                    engagement_db_dataset="worldbank_scd_s01e01_follow_up_1",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01_follow_up_1"),
                                                coda_code_schemes_count=3
                                                ),
                    ],
                    ws_code_match_value="worldbank_scd_s01e01_follow_up_1"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldBank_SCD_s01e01_follow_up_2",
                    engagement_db_dataset="worldbank_scd_s01e01_follow_up_2",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01_follow_up_2"),
                                                coda_code_schemes_count=3
                                                ),
                    ],
                    ws_code_match_value="worldbank_scd_s01e01_follow_up_2"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldBank_SCD_s01_have_voice",
                    engagement_db_dataset="worldbank_scd_s01_have_voice",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/worldbank_scd/s01_have_voice"),
                                                coda_code_schemes_count=3,
                                                auto_coder=somali.DemographicCleaner.clean_yes_no
                                                ),
                    ],
                    ws_code_match_value="worldbank_scd_s01_have_voice"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldBank_SCD_s01_suggestions",
                    engagement_db_dataset="worldbank_scd_s01_suggestions",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/worldbank_scd/s01_suggestions"),
                                                coda_code_schemes_count=3
                                                ),
                    ],
                    ws_code_match_value="worldbank_scd_s01_suggestions"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldBank_SCD_s01_evaluation",
                    engagement_db_dataset="worldbank_scd_s01_evaluation",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/worldbank_scd/s01_evaluation"),
                                                coda_code_schemes_count=3
                                                ),
                    ],
                    ws_code_match_value="worldbank_scd_s01_evaluation"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="WorldBank_SCD_s01e01_google_form_consent",
                    engagement_db_dataset="worldbank_scd_s01e01_google_form_consent",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01_google_form_consent"),
                                                coda_code_schemes_count=3
                                                ),
                    ],
                    ws_code_match_value="worldbank_scd_s01e01_google_form_consent"
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
            project_users_file_url="gs://avf-project-datasets/2022/WorldBank-SCD/coda_users.json"
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/WorldBank-SCD"
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="worldbank_scd_analysis_outputs"
        ),
        dataset_configurations=[
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["worldbank_scd_s01e01"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01"),
                        analysis_dataset="s01e01"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["worldbank_scd_s01e01_google_form_consent"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_google_form_consent_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01_google_form_consent"),
                        analysis_dataset="s01e01_google_form_consent"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["worldbank_scd_s01e01_follow_up_1"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_follow_up_1_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01_follow_up_1"),
                        analysis_dataset="s01e01_follow_up_1"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["worldbank_scd_s01e01_follow_up_2"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01e01_follow_up_2_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/worldbank_scd/s01e01_follow_up_2"),
                        analysis_dataset="s01e01_follow_up_2"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["worldbank_scd_s01_have_voice"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01_have_voice_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/worldbank_scd/s01_have_voice"),
                        analysis_dataset="s01_have_voice"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["worldbank_scd_s01_suggestions"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01_suggestions_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/worldbank_scd/s01_suggestions"),
                        analysis_dataset="s01_suggestions"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["worldbank_scd_s01_evaluation"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="s01_evaluation_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("rqas/worldbank_scd/s01_evaluation"),
                        analysis_dataset="s01_evaluation"
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
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        traffic_labels=[],
        cross_tabs=[
            ("operator", "state")
        ]
    )
)
