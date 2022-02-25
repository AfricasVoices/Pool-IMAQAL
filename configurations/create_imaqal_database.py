from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="CREATE-IMAQAL-POOL",
    description="Creates the initial Imaqal Pool from demographics responses to IMAQAL, IMAQAL_COVID19, "
                "SSF-ELECTIONS, SSF-DCF, SSF-SLD, SSF-REC, and SSF-PPE.",
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
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
    ),
    rapid_pro_sources=[
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/imaqal-text-it-token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("imaqal_covid19_demog", "age", "age"),
                    FlowResultConfiguration("imaqal_covid19_demog", "district", "location"),
                    FlowResultConfiguration("imaqal_covid19_demog", "gender", "gender"),
                    FlowResultConfiguration("imaqal_covid19_demog", "household_language", "household_language"),
                    FlowResultConfiguration("imaqal_covid19_demog", "recently_displaced", "recently_displaced"),

                    FlowResultConfiguration("imaqal_demog", "age", "age"),
                    FlowResultConfiguration("imaqal_demog", "district", "location"),
                    FlowResultConfiguration("imaqal_demog", "gender", "gender"),
                    FlowResultConfiguration("imaqal_demog", "household_language", "household_language"),
                    FlowResultConfiguration("imaqal_demog", "recently_displaced", "recently_displaced"),

                    FlowResultConfiguration("ssf_elections_demog", "age", "age"),
                    FlowResultConfiguration("ssf_elections_demog", "district", "location"),
                    FlowResultConfiguration("ssf_elections_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_elections_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_elections_demog", "recently displaced", "recently_displaced"),
                ],
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/SSF_DCF-SLD-Textit-Token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("ssf_dcf_demog", "age", "age"),
                    FlowResultConfiguration("ssf_dcf_demog", "district", "location"),
                    FlowResultConfiguration("ssf_dcf_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_dcf_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_dcf_demog", "recently displaced", "recently_displaced"),

                    FlowResultConfiguration("ssf_sld_demog", "age", "age"),
                    FlowResultConfiguration("ssf_sld_demog", "district", "location"),
                    FlowResultConfiguration("ssf_sld_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_sld_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_sld_demog", "recently displaced", "recently_displaced"),
                ],
            )
        ),
        RapidProSource(
            rapid_pro=RapidProClientConfiguration(
                domain="textit.com",
                token_file_url="gs://avf-credentials/SSF_REC-PPE-Textit-Token.txt"
            ),
            sync_config=RapidProToEngagementDBConfiguration(
                flow_result_configurations=[
                    FlowResultConfiguration("ssf_ppe_demog", "age", "age"),
                    FlowResultConfiguration("ssf_ppe_demog", "district", "location"),
                    FlowResultConfiguration("ssf_ppe_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_ppe_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_ppe_demog", "recently displaced", "recently_displaced"),

                    FlowResultConfiguration("ssf_rec_demog", "age", "age"),
                    FlowResultConfiguration("ssf_rec_demog", "district", "location"),
                    FlowResultConfiguration("ssf_rec_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_rec_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_rec_demog", "recently displaced", "recently_displaced"),
                ],
            )
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            dataset_configurations=[
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("age"), 
                            auto_coder=lambda text: str(somali.DemographicCleaner.clean_age_within_range(text))
                        ),
                    ],
                    ws_code_string_value="age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("gender"), auto_coder=somali.DemographicCleaner.clean_gender)
                    ],
                    ws_code_string_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("household_language"), auto_coder=None)
                    ],
                    ws_code_string_value="hh language"
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
                    ws_code_string_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_recently_displaced",
                    engagement_db_dataset="recently_displaced",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("recently_displaced"), auto_coder=somali.DemographicCleaner.clean_yes_no)
                    ],
                    ws_code_string_value="recently displaced"
                ),
            ],
            ws_correct_dataset_code_scheme=load_code_scheme("ws_correct_dataset"),
            project_users_file_url="gs://avf-project-datasets/2022/IMAQAL-POOL/coda_users.json"
        )
    ),
    rapid_pro_target=RapidProTarget(
        rapid_pro=RapidProClientConfiguration(
            domain="textit.com",
            token_file_url="gs://avf-credentials/imaqal-text-it-token.txt"
        ),
        sync_config=EngagementDBToRapidProConfiguration(
            normal_datasets=[
                DatasetConfiguration(
                    engagement_db_datasets=["age"], 
                    rapid_pro_contact_field=ContactField(key="engagement_db_age", label="DB Age")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["gender"], 
                    rapid_pro_contact_field=ContactField(key="engagement_db_gender", label="DB Gender")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["household_language"], 
                    rapid_pro_contact_field=ContactField(key="engagement_db_household_language", label="DB Household Language")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["location"], 
                    rapid_pro_contact_field=ContactField(key="engagement_db_district", label="DB District")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["recently_displaced"], 
                    rapid_pro_contact_field=ContactField(key="engagement_db_recently_displaced", label="DB Recently Displaced")
                ),
            ],
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=["age", "gender", "household_language", "location", "recently_displaced"],
                rapid_pro_contact_field=ContactField(key="engagement_db_consent_withdrawn", label="DB Consent Withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/IMAQAL-POOL"
    )
)
