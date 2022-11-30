from src.pipeline_configuration_spec import *


PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="IMAQAL-Pool-To-Text-It",
    description="Syncs the latest demographics and consent data from the IMAQAL pool to Text It",
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
        credentials_file_url="gs://avf-credentials/avf-dashboards-firebase-adminsdk-gvecb-ef772e79b6.json",
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
                engagement_db_datasets=[
                    # This list was created by reading all the unique datasets from an IMAQAL pool export.
                    "ssf_elections_s01e01", "ssf_elections_s01e02", "ssf_elections_s01e03",
                    "ssf_elections_s01e04", "ssf_elections_s01e05", "ssf_elections_s01e06",

                    "ssf_dcf_s01e01", "ssf_dcf_s01e02", "ssf_dcf_s01e03", "ssf_dcf_s01e04",
                    "ssf_sld_s01e01", "ssf_sld_s01e02", "ssf_sld_s01e03", "ssf_sld_s01e04",
                    "ssf_rec_s01e01", "ssf_rec_s01e02", "ssf_rec_s01e03", "ssf_rec_s01e04",
                    "ssf_ppe_s01e01", "ssf_ppe_s01e02",

                    "imaqal_covid19_s01e02", "imaqal_covid19_s02e01_promo", "imaqal_covid19_s02e02_promo",
                    "imaqal_decisions_minority_clan", "imaqal_s01e06", "imaqal_women_participation",

                    "rvi_elections_s01e01", "rvi_elections_s01e02", "rvi_elections_s01e03",
                    "rvi_elections_s01e04", "rvi_elections_s01e05", "rvi_elections_s01e06",
                    "rvi_elections_s01_closeout",

                    "age", "gender", "household_language", "location", "recently_displaced"
                ],
                rapid_pro_contact_field=ContactField(
                    key="imaqal_pool_consent_withdrawn",
                    label="imaqal pool consent withdrawn"
                )
            ),
            write_mode=WriteModes.CONCATENATE_TEXTS,
            allow_clearing_fields=True
        )
    ),
    archive_configuration=ArchiveConfiguration(
        archive_upload_bucket="gs://pipeline-execution-backup-archive",
        bucket_dir_path="2022/IMAQAL-Demographics-To-TextIt"
    )
)
