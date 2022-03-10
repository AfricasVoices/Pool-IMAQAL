from core_data_modules.cleaners import somali
from dateutil.parser import isoparse
from src.pipeline_configuration_spec import *


def make_rqa_coda_dataset_configs(dataset_name_prefix, coda_dataset_id_prefix, code_scheme_prefix, number_of_datasets):
    """
    Creates a list of n rqa coda dataset configs, indexed from 1 to `number_of_datasets`.
    This allows us to configure the highly repetitive rqa configurations from previous projects very succinctly.
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
                        auto_coder=None)
                ],
                ws_code_match_value=f"{dataset_name_prefix}{i}",
                update_users_and_code_schemes=False
            )
        )
    return dataset_configs


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
                    FlowResultConfiguration("ssf_rec_demog", "age", "age"),
                    FlowResultConfiguration("ssf_rec_demog", "district", "location"),
                    FlowResultConfiguration("ssf_rec_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_rec_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_rec_demog", "recently displaced", "recently_displaced"),

                    FlowResultConfiguration("ssf_ppe_demog", "age", "age"),
                    FlowResultConfiguration("ssf_ppe_demog", "district", "location"),
                    FlowResultConfiguration("ssf_ppe_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_ppe_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_ppe_demog", "recently displaced", "recently_displaced"),
                ],
            )
        )
    ],
    csv_sources=[
        # These CSV sources are from projects in late 2021 that had an unusually high loss rate from the operator.
        # CSVs are grouped by project and linked to their original project configuration files for convenience.
        # The gs urls are to copies of the original files that have had new uuids assigned under the new global uuid
        # table rather than the IMAQAL table that was used previously.

        # SSF-ELECTIONS
        # https://github.com/AfricasVoices/Project-SSF-ELECTIONS/blob/main/configuration/pipeline_config.json
        CSVSource(
            "gs://avf-project-datasets/2022/IMAQAL-POOL/recovery_csvs/2021_SSF_ELECTIONS_recovered_golis_s01e01_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("ssf_elections_s01e01", start_date=isoparse("2021-09-22T08:00+03:00"), end_date=isoparse("2021-09-28T08:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/IMAQAL-POOL/recovery_csvs/elections_recovered_hormuud_september_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("ssf_elections_s01e01", start_date=isoparse("2021-09-22T08:00+03:00"), end_date=isoparse("2021-09-28T08:00+03:00")),
                CSVDatasetConfiguration("ssf_elections_s01e02", start_date=isoparse("2021-09-28T08:00+03:00"), end_date=isoparse("2021-09-30T24:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        ),
        CSVSource(
            "gs://avf-project-datasets/2022/IMAQAL-POOL/recovery_csvs/elections_recovered_hormuud_october_de_identified.csv",
            engagement_db_datasets=[
                CSVDatasetConfiguration("ssf_elections_s01e03", start_date=isoparse("2021-10-05T08:00+03:00"), end_date=isoparse("2021-10-13T08:00+03:00")),
                CSVDatasetConfiguration("ssf_elections_s01e04", start_date=isoparse("2021-10-13T08:00+03:00"), end_date=isoparse("2021-10-20T08:00+03:00")),
                CSVDatasetConfiguration("ssf_elections_s01e05", start_date=isoparse("2021-10-20T08:00+03:00"), end_date=isoparse("2021-10-27T08:00+03:00")),
                CSVDatasetConfiguration("ssf_elections_s01e06", start_date=isoparse("2021-10-27T08:00+03:00"), end_date=isoparse("2021-11-03T08:00+03:00"))
            ],
            timezone="Africa/Mogadishu"
        )
    ],
    coda_sync=CodaConfiguration(
        coda=CodaClientConfiguration(credentials_file_url="gs://avf-credentials/coda-production.json"),
        sync_config=CodaSyncConfiguration(
            # Includes previous rqa dataset configurations for the projects that have recovery CSVs because the
            # CSV-recovered data were initially directed to rqa datasets. Relevant demogs will be WS-corrected to the
            # right place, allowing other datasets to be deleted in future.
            dataset_configurations=\
                make_rqa_coda_dataset_configs(
                    dataset_name_prefix="ssf_elections_s01e0",
                    coda_dataset_id_prefix="SSF_ELECTIONS_s01e0",
                    code_scheme_prefix="previous_rqas/ssf_elections/ssf_elections_rqa_s01e0",
                    number_of_datasets=7
                ) + [
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_age",
                    engagement_db_dataset="age",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("age"), 
                            auto_coder=lambda text: str(somali.DemographicCleaner.clean_age_within_range(text))
                        ),
                    ],
                    ws_code_match_value="age"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_gender",
                    engagement_db_dataset="gender",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("gender"), auto_coder=somali.DemographicCleaner.clean_gender)
                    ],
                    ws_code_match_value="gender"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_household_language",
                    engagement_db_dataset="household_language",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("household_language"), auto_coder=None)
                    ],
                    ws_code_match_value="household_language"
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
                    ws_code_match_value="location"
                ),
                CodaDatasetConfiguration(
                    coda_dataset_id="IMAQAL_recently_displaced",
                    engagement_db_dataset="recently_displaced",
                    code_scheme_configurations=[
                        CodeSchemeConfiguration(code_scheme=load_code_scheme("recently_displaced"), auto_coder=somali.DemographicCleaner.clean_yes_no)
                    ],
                    ws_code_match_value="recently_displaced"
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
                    rapid_pro_contact_field=ContactField(key="imaqal_pool_age", label="imaqal pool age")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["gender"], 
                    rapid_pro_contact_field=ContactField(key="imaqal_pool_gender", label="imaqal pool gender")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["household_language"], 
                    rapid_pro_contact_field=ContactField(key="imaqal_pool_household_language", label="imaqal pool household language")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["location"], 
                    rapid_pro_contact_field=ContactField(key="imaqal_pool_district", label="imaqal pool district")
                ),
                DatasetConfiguration(
                    engagement_db_datasets=["recently_displaced"], 
                    rapid_pro_contact_field=ContactField(key="imaqal_pool_recently_displaced", label="imaqal pool recently displaced")
                ),
            ],
            consent_withdrawn_dataset=DatasetConfiguration(
                engagement_db_datasets=["age", "gender", "household_language", "location", "recently_displaced"],
                rapid_pro_contact_field=ContactField(key="imaqal_pool_consent_withdrawn", label="imaqal pool consent withdrawn")
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/CREATE-IMAQAL-POOL"
    )
)
