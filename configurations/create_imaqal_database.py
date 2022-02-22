from src.pipeline_configuration_spec import *

PIPELINE_CONFIGURATION = PipelineConfiguration(
    pipeline_name="CREATE-IMAQAL-POOL",
    engagement_database=EngagementDatabaseClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-engagement-databases-firebase-credentials-file.json",
        database_path="engagement_databases/IMAQAL"
    ),
    uuid_table=UUIDTableClientConfiguration(
        credentials_file_url="gs://avf-credentials/avf-id-infrastructure-firebase-adminsdk-6xps8-b9173f2bfd.json",
        table_name="IMAQAL",
        uuid_prefix="avf-phone-uuid-"
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
                    FlowResultConfiguration("imaqal_covid19_demog", "district", "district"),
                    FlowResultConfiguration("imaqal_covid19_demog", "gender", "gender"),
                    FlowResultConfiguration("imaqal_covid19_demog", "household_language", "household_language"),
                    FlowResultConfiguration("imaqal_covid19_demog", "recently_displaced", "recently_displaced"),

                    FlowResultConfiguration("imaqal_demog", "age", "age"),
                    FlowResultConfiguration("imaqal_demog", "district", "district"),
                    FlowResultConfiguration("imaqal_demog", "gender", "gender"),
                    FlowResultConfiguration("imaqal_demog", "household_language", "household_language"),
                    FlowResultConfiguration("imaqal_demog", "recently_displaced", "recently_displaced"),

                    FlowResultConfiguration("ssf_elections_demog", "age", "age"),
                    FlowResultConfiguration("ssf_elections_demog", "district", "district"),
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
                    FlowResultConfiguration("ssf_dcf_demog", "district", "district"),
                    FlowResultConfiguration("ssf_dcf_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_dcf_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_dcf_demog", "recently displaced", "recently_displaced"),

                    FlowResultConfiguration("ssf_sld_demog", "age", "age"),
                    FlowResultConfiguration("ssf_sld_demog", "district", "district"),
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
                    FlowResultConfiguration("ssf_ppe_demog", "district", "district"),
                    FlowResultConfiguration("ssf_ppe_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_ppe_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_ppe_demog", "recently displaced", "recently_displaced"),
                    
                    FlowResultConfiguration("ssf_rec_demog", "age", "age"),
                    FlowResultConfiguration("ssf_rec_demog", "district", "district"),
                    FlowResultConfiguration("ssf_rec_demog", "gender", "gender"),
                    FlowResultConfiguration("ssf_rec_demog", "household language", "household_language"),
                    FlowResultConfiguration("ssf_rec_demog", "recently displaced", "recently_displaced"),
                ],
            )
        )
    ],
)
