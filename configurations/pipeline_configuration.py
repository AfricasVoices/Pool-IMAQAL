from core_data_modules.cleaners import Codes, somali
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="RVI-ELECTIONS",
    project_start_date=isoparse("2022-03-11T00:00:00+03:00"),
    project_end_date=isoparse("2100-01-01T00:00:00+03:00"),
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
        database_path="engagement_databases/IMAQAL"
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
                    FlowResultConfiguration("RVI_elections_demog", "imaqal_pool_age", "age"),
                    FlowResultConfiguration("RVI_elections_demog", "imaqal_pool_district", "location"),
                    FlowResultConfiguration("RVI_elections_demog", "imaqal_pool_gender", "gender"),
                    FlowResultConfiguration("RVI_elections_demog", "imaqal_pool_recently_displaced", "recently_displaced"),

                    FlowResultConfiguration("RVI_elections_s01e01_activation", "rqa_rvi_elections_s01e01", "rvi_elections_s01e01"),
                    FlowResultConfiguration("RVI_elections_s01e02_activation", "rqa_rvi_elections_s01e02", "rvi_elections_s01e02"),
                    FlowResultConfiguration("RVI_elections_s01e03_activation", "rqa_rvi_elections_s01e03", "rvi_elections_s01e03"),
                    FlowResultConfiguration("RVI_elections_s01e04_activation", "rqa_rvi_elections_s01e04", "rvi_elections_s01e04"),
                    FlowResultConfiguration("RVI_elections_s01e05_activation", "rqa_rvi_elections_s01e05", "rvi_elections_s01e05"),
                    FlowResultConfiguration("RVI_elections_s01e06_activation", "rqa_rvi_elections_s01e06", "rvi_elections_s01e06"),
                    FlowResultConfiguration("RVI_elections_s01_closeout_activation", "rqa_rvi_elections_s01_closeout", "rvi_elections_s01_closeout")
                ]
            )
        )
    ],
    csv_sources=[
        # Hormud
        CSVSource(
            "gs://avf-project-datasets/2022/RVI-ELECTIONS/recovered_hormuud_2022_03_12_to_2022_03_18_de_identified.csv",
            engagement_db_datasets=[
                # This contains data from 12th March, until 18th March.
                CSVDatasetConfiguration("rvi_elections_s01e01",
                                        start_date=isoparse("2022-03-12T00:00:00+03:00"), 
                                        end_date=isoparse("2022-03-19T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/RVI-ELECTIONS/recovered_hormuud_2022_03_19_to_2022_03_25_de_identified.csv",
            engagement_db_datasets=[
                # This contains data from 19th March, until 25th March.
                CSVDatasetConfiguration("rvi_elections_s01e02",
                                        start_date=isoparse("2022-03-19T00:00:00+03:00"),
                                        end_date=isoparse("2022-03-26T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/RVI-ELECTIONS/recovered_hormuud_2022_03_26_to_2022_04_02_de_identified.csv",
            engagement_db_datasets=[
                # This contains data from 26th March, until 1st April.
                CSVDatasetConfiguration("rvi_elections_s01e03",
                                        start_date=isoparse("2022-03-26T00:00:00+03:00"),
                                        end_date=isoparse("2022-04-02T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/RVI-ELECTIONS/recovered_hormuud_2022_03_26_to_2022_04_02_de_identified.csv",
            engagement_db_datasets=[
                # This contains data from 2nd April.
                CSVDatasetConfiguration("rvi_elections_s01e04",
                                        start_date=isoparse("2022-04-02T00:00:00+03:00"),
                                        end_date=isoparse("2022-04-03T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/RVI-ELECTIONS/recovered_hormuud_2022_04_03_to_2022_04_07_de_identified.csv",
            engagement_db_datasets=[
                # This contains data from 3rd April, until 7th April.
                CSVDatasetConfiguration("rvi_elections_s01e04",
                                        start_date=isoparse("2022-04-03T00:00:00+03:00"),
                                        end_date=isoparse("2022-04-08T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        # Golis
        CSVSource(
            "gs://avf-project-datasets/2022/RVI-ELECTIONS/recovered_golis_2022_03_27_to_2022_03_28_de_identified.csv",
            engagement_db_datasets=[
                # This contains data from 27th March, until 28th March.
                CSVDatasetConfiguration("rvi_elections_s01e03",
                                        start_date=isoparse("2022-03-27T00:00:00+03:00"),
                                        end_date=isoparse("2022-03-29T00:00:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=make_rqa_coda_dataset_configs(
                coda_dataset_id_prefix="RVI_ELECTIONS_s01e0",
                dataset_name_prefix="rvi_elections_s01e0",
                code_scheme_prefix="s01e0",
                number_of_datasets=6
            ) +
            [
                CodaDatasetConfiguration(
                    coda_dataset_id="RVI_ELECTIONS_s01_closeout",
                    engagement_db_dataset="rvi_elections_s01_closeout",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(
                            code_scheme=load_code_scheme("s01_closeout"), 
                            auto_coder=None, coda_code_schemes_count=3)
                    ],
                    ws_code_match_value="rvi_elections_s01_closeout"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("age"),
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
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("gender"),
                                                auto_coder=somali.DemographicCleaner.clean_gender)
                    ],
                    ws_code_match_value="gender",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("household_language"), auto_coder=None)
                    ],
                    ws_code_match_value="household_language",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_location",
                    engagement_db_dataset="location",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("mogadishu_sub_district"),
                                                auto_coder=somali.DemographicCleaner.clean_mogadishu_sub_district),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("somalia_district"), auto_coder=lambda text:
                                                somali.DemographicCleaner.clean_somalia_district(text)
                                                if somali.DemographicCleaner.clean_mogadishu_sub_district == Codes.NOT_CODED else Codes.NOT_CODED),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("somalia_region"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("somalia_state"), auto_coder=None),
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("somalia_zone"), auto_coder=None),
                    ],
                    ws_code_match_value="location",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_recently_displaced",
                    engagement_db_dataset="recently_displaced",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("recently_displaced"),
                                                auto_coder=somali.DemographicCleaner.clean_yes_no)
                    ],
                    ws_code_match_value="recently_displaced",
                    dataset_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2022/RVI-ELECTIONS/coda_users.json"
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/RVI-ELECTIONS"
    ),
    analysis=AnalysisConfiguration(
        google_drive_upload=GoogleDriveUploadConfiguration(
            credentials_file_url="gs://avf-credentials/pipeline-runner-service-acct-avf-data-core-64cc71459fe7.json",
            drive_dir="rvi_elections_analysis_outputs"
        ),
        dataset_configurations=make_rqa_analysis_dataset_configs(
                dataset_name_prefix="rvi_elections_s01e0",
                code_scheme_prefix="s01e0",
                number_of_datasets=6
        ) +
        [
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["rvi_elections_s01_closeout"],
                dataset_type=DatasetTypes.RESEARCH_QUESTION_ANSWER,
                raw_dataset="rvi_elections_s01_closeout_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("s01_closeout"),
                        analysis_dataset="s01_closeout"
                    )
                ]
            ),
            AnalysisDatasetConfiguration(
                engagement_db_datasets=["age"],
                dataset_type=DatasetTypes.DEMOGRAPHIC,
                raw_dataset="age_raw",
                coding_configs=[
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age"),
                        analysis_dataset="age"
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("age_category"),
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
                        code_scheme=load_code_scheme("gender"),
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
                        code_scheme=load_code_scheme("mogadishu_sub_district"),
                        analysis_dataset="mogadishu_sub_district",
                        analysis_location=AnalysisLocations.MOGADISHU_SUB_DISTRICT
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("somalia_district"),
                        analysis_dataset="district",
                        analysis_location=AnalysisLocations.SOMALIA_DISTRICT
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("somalia_region"),
                        analysis_dataset="region",
                        analysis_location=AnalysisLocations.SOMALIA_REGION
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("somalia_state"),
                        analysis_dataset="state",
                        analysis_location=AnalysisLocations.SOMALIA_STATE
                    ),
                    CodingConfiguration(
                        code_scheme=load_code_scheme("somalia_zone"),
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
                        code_scheme=load_code_scheme("recently_displaced"),
                        analysis_dataset="recently_displaced"
                    )
                ]
            ),
        ],
        ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
        traffic_labels=[
            # S01E01
            # --------------------------------------- 12 - 13 March -----------------------------------------------                                       
            TrafficLabel(isoparse("2022-03-12T00:00+03:00"), isoparse("2022-03-12T24:00+03:00"), "E01 Sat, Promo"),
            TrafficLabel(isoparse("2022-03-13T00:00+03:00"), isoparse("2022-03-13T24:00+03:00"), "E01 Sun, Promo"),
            # --------------------------------------- 14 March ---------------------------------------------------
            TrafficLabel(isoparse("2022-03-14T00:00+03:00"), isoparse("2022-03-14T24:00+03:00"), "E01 Mon, None"),
            # --------------------------------------- 15 March ---------------------------------------------------
            TrafficLabel(isoparse("2022-03-15T00:00+03:00"), isoparse("2022-03-15T09:20+03:00"), "E01 Tue, None"),
            TrafficLabel(isoparse("2022-03-15T09:20+03:00"), isoparse("2022-03-15T11:00+03:00"), "E01 Tue, Ad, Group A (6,286)"),
            TrafficLabel(isoparse("2022-03-15T11:00+03:00"), isoparse("2022-03-15T24:00+03:00"), "E01 Tue, Radio & Ad"),
            # --------------------------------------- 16 March ------------------------------------------------------------------
            TrafficLabel(isoparse("2022-03-16T00:00+03:00"), isoparse("2022-03-16T24:00+03:00"), "E01 Wed, None"),
            # --------------------------------------- 17 March ---------------------------------------------------
            TrafficLabel(isoparse("2022-03-17T00:00+03:00"), isoparse("2022-03-17T16:30+03:00"), "E01 Thu, None"),
            TrafficLabel(isoparse("2022-03-17T16:30+03:00"), isoparse("2022-03-17T24:00+03:00"), "E01 Thu, Ad, Group B (8,397)"),
            # --------------------------------------- 18 March ------------------------------------------------------------------ 
            TrafficLabel(isoparse("2022-03-18T00:00+03:00"), isoparse("2022-03-18T24:00+03:00"), "E01 Fri, None"),

            # S01E02
            # --------------------------------------- 19 - 20 March -----------------------------------------------
            TrafficLabel(isoparse("2022-03-19T00:00+03:00"), isoparse("2022-03-19T24:00+03:00"), "E02 Sat, Promo"),
            TrafficLabel(isoparse("2022-03-20T00:00+03:00"), isoparse("2022-03-20T24:00+03:00"), "E02 Sun, Promo"),
            # --------------------------------------- 21 March ---------------------------------------------------
            TrafficLabel(isoparse("2022-03-21T00:00+03:00"), isoparse("2022-03-21T17:40+03:00"), "E02 Mon, None"),
            TrafficLabel(isoparse("2022-03-21T17:40+03:00"), isoparse("2022-03-21T24:00+03:00"), "E02 Mon, Ad (13,672)"),
            # --------------------------------------- 22 March ----------------------------------------------------------
            TrafficLabel(isoparse("2022-03-22T00:00+03:00"), isoparse("2022-03-22T11:00+03:00"), "E02 Tue, None"),
            TrafficLabel(isoparse("2022-03-22T11:00+03:00"), isoparse("2022-03-22T24:00+03:00"), "E02 Tue, Radio"),
            # --------------------------------------- 23 - 25 March ----------------------------------------------
            TrafficLabel(isoparse("2022-03-23T00:00+03:00"), isoparse("2022-03-23T24:00+03:00"), "E02 Wed, None"),
            TrafficLabel(isoparse("2022-03-24T00:00+03:00"), isoparse("2022-03-24T24:00+03:00"), "E02 Thu, None"),
            TrafficLabel(isoparse("2022-03-25T00:00+03:00"), isoparse("2022-03-25T24:00+03:00"), "E02 Fri, None"),

            # S01E03
            # --------------------------------------- 26 - 27 March -----------------------------------------------
            TrafficLabel(isoparse("2022-03-26T00:00+03:00"), isoparse("2022-03-26T24:00+03:00"), "E03 Sat, Promo"),
            TrafficLabel(isoparse("2022-03-27T00:00+03:00"), isoparse("2022-03-27T24:00+03:00"), "E03 Sun, Promo"),
            # --------------------------------------- 28 March ---------------------------------------------------
            TrafficLabel(isoparse("2022-03-28T00:00+03:00"), isoparse("2022-03-28T17:30+03:00"), "E03 Mon, None"),
            TrafficLabel(isoparse("2022-03-28T17:30+03:00"), isoparse("2022-03-28T24:00+03:00"), "E03 Mon, Ad (5,401)"),
            # --------------------------------------- 29 March ---------------------------------------------------------
            TrafficLabel(isoparse("2022-03-29T00:00+03:00"), isoparse("2022-03-29T11:00+03:00"), "E03 Tue, None"),
            TrafficLabel(isoparse("2022-03-29T11:00+03:00"), isoparse("2022-03-29T24:00+03:00"), "E03 Tue, Radio"),
            # --------------------------------------- 30 - 01 April ----------------------------------------------
            TrafficLabel(isoparse("2022-03-30T00:00+03:00"), isoparse("2022-03-30T24:00+03:00"), "E03 Wed, None"),
            TrafficLabel(isoparse("2022-03-31T00:00+03:00"), isoparse("2022-03-31T24:00+03:00"), "E03 Thur, None"),
            TrafficLabel(isoparse("2022-04-01T00:00+03:00"), isoparse("2022-04-01T24:00+03:00"), "E03 Fri, None"),

            # S01E04
            # --------------------------------------- 02 - 03 April ----------------------------------------------- 
            TrafficLabel(isoparse("2022-04-02T00:00+03:00"), isoparse("2022-04-02T24:00+03:00"), "E04 Sat, Promo"),
            TrafficLabel(isoparse("2022-04-03T00:00+03:00"), isoparse("2022-04-03T24:00+03:00"), "E04 Sun, Promo"),
            # --------------------------------------- 04 April ---------------------------------------------------
            TrafficLabel(isoparse("2022-04-04T00:00+03:00"), isoparse("2022-04-04T17:20+03:00"), "E04 Mon, None"),
            TrafficLabel(isoparse("2022-04-04T17:20+03:00"), isoparse("2022-04-04T24:00+03:00"), "E04 Mon, Ad (7,210)"),
            # --------------------------------------- 05 April ---------------------------------------------------------
            TrafficLabel(isoparse("2022-04-05T00:00+03:00"), isoparse("2022-04-05T11:00+03:00"), "E04 Tue, None"),
            TrafficLabel(isoparse("2022-04-05T11:00+03:00"), isoparse("2022-04-05T24:00+03:00"), "E04 Tue, Radio"),
            # --------------------------------------- 06 - 08 April ----------------------------------------------
            TrafficLabel(isoparse("2022-04-06T00:00+03:00"), isoparse("2022-04-06T24:00+03:00"), "E04 Wed, None"),
            TrafficLabel(isoparse("2022-04-07T00:00+03:00"), isoparse("2022-04-07T24:00+03:00"), "E04 Thu, None"),
            TrafficLabel(isoparse("2022-04-08T00:00+03:00"), isoparse("2022-04-08T24:00+03:00"), "E04 Fri, None"),

            # S01E05
            # --------------------------------------- 09 - 10 April -----------------------------------------------
            TrafficLabel(isoparse("2022-04-09T00:00+03:00"), isoparse("2022-04-09T24:00+03:00"), "E05 Sat, Promo"),
            TrafficLabel(isoparse("2022-04-10T00:00+03:00"), isoparse("2022-04-10T24:00+03:00"), "E05 Sun, Promo"),
            # --------------------------------------- 11 April ---------------------------------------------------
            TrafficLabel(isoparse("2022-04-11T00:00+03:00"), isoparse("2022-04-11T17:30+03:00"), "E05 Mon, None"),
            TrafficLabel(isoparse("2022-04-11T17:30+03:00"), isoparse("2022-04-11T24:00+03:00"), "E05 Mon, Ad (8,889)"),
            # --------------------------------------- 12 April ---------------------------------------------------------
            TrafficLabel(isoparse("2022-04-12T00:00+03:00"), isoparse("2022-04-12T11:00+03:00"), "E05 Tue, None"),
            TrafficLabel(isoparse("2022-04-12T11:00+03:00"), isoparse("2022-04-12T24:00+03:00"), "E05 Tue, Radio"),
            # --------------------------------------- 13 - 15 April ----------------------------------------------
            TrafficLabel(isoparse("2022-04-13T00:00+03:00"), isoparse("2022-04-13T24:00+03:00"), "E05 Wed, None"),
            TrafficLabel(isoparse("2022-04-14T00:00+03:00"), isoparse("2022-04-14T24:00+03:00"), "E05 Thu, None"),
            TrafficLabel(isoparse("2022-04-15T00:00+03:00"), isoparse("2022-04-15T24:00+03:00"), "E05 Fri, None"),

            # S01E06
            # --------------------------------------- 16 - 17 April -----------------------------------------------
            TrafficLabel(isoparse("2022-04-16T00:00+03:00"), isoparse("2022-04-16T24:00+03:00"), "E06 Sat, Promo"),
            TrafficLabel(isoparse("2022-04-17T00:00+03:00"), isoparse("2022-04-17T24:00+03:00"), "E06 Sun, Promo"),
            # --------------------------------------- 18 April ---------------------------------------------------
            TrafficLabel(isoparse("2022-04-18T00:00+03:00"), isoparse("2022-04-18T24:00+03:00"), "E06 Mon, None"),
            # --------------------------------------- 19 April ---------------------------------------------------
            TrafficLabel(isoparse("2022-04-19T00:00+03:00"), isoparse("2022-04-19T11:00+03:00"), "E06 Tue, None"),
            TrafficLabel(isoparse("2022-04-19T11:00+03:00"), isoparse("2022-04-19T24:00+03:00"), "E06 Tue, Radio"),
            # --------------------------------------- 20 April ----------------------------------------------------
            TrafficLabel(isoparse("2022-04-20T00:00+03:00"), isoparse("2022-04-20T17:30+03:00"), "E06 Wed, None"),
            TrafficLabel(isoparse("2022-04-20T17:30+03:00"), isoparse("2022-04-20T24:00+03:00"), "E06 Wed, Ad (11,632)"),
            # --------------------------------------- 21 - 22 April -----------------------------------------------------
            TrafficLabel(isoparse("2022-04-21T00:00+03:00"), isoparse("2022-04-21T24:00+03:00"), "E06 Thu, None"),
            TrafficLabel(isoparse("2022-04-22T00:00+03:00"), isoparse("2022-04-22T24:00+03:00"), "E06 Fri, None"),
        ]
    )
)
