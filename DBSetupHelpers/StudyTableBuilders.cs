using Dapper;
using Npgsql;

namespace MDR_Importer;

public class StudyTableBuilders
{
    private readonly string _db_conn;

    public StudyTableBuilders(string db_conn)
    {
        _db_conn = db_conn;
    }

    public void Execute_SQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
    }

    public void create_ad_schema()
    {
        string sql_string = @"CREATE SCHEMA IF NOT EXISTS ad;";

        Execute_SQL(sql_string);
    }

    public void create_table_studies()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.studies;
        CREATE TABLE ad.studies(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , display_title          VARCHAR         NULL
          , title_lang_code        VARCHAR         NULL default 'en'
          , brief_description      VARCHAR         NULL
          , data_sharing_statement VARCHAR         NULL
          , study_start_year       INT             NULL
          , study_start_month      INT             NULL
          , study_type_id          INT             NULL
          , study_status_id        INT             NULL
          , study_enrolment        VARCHAR         NULL
          , study_gender_elig_id   INT             NULL
          , min_age                INT             NULL
          , min_age_units_id       INT             NULL
          , max_age                INT             NULL
          , max_age_units_id       INT             NULL
          , iec_level              INT             NULL
          , datetime_of_data_fetch TIMESTAMPTZ     NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
        );
        CREATE INDEX studies_sid ON ad.studies(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_identifiers()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_identifiers;
        CREATE TABLE ad.study_identifiers(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , identifier_value       VARCHAR         NULL
          , identifier_type_id     INT             NULL
          , source_id              INT             NULL
          , source                 VARCHAR         NULL
          , source_ror_id          VARCHAR         NULL
          , identifier_date        VARCHAR         NULL
          , identifier_link        VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL                                     
        );
        CREATE INDEX study_identifiers_sid ON ad.study_identifiers(sd_sid);";

        Execute_SQL(sql_string);
    }

    public void create_table_study_relationships()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_relationships;
        CREATE TABLE ad.study_relationships(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , relationship_type_id   INT             NULL
          , target_sd_sid          VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
        );
        CREATE INDEX study_relationships_sid ON ad.study_relationships(sd_sid);
        CREATE INDEX study_relationships_target_sid ON ad.study_relationships(target_sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_references()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_references;
        CREATE TABLE ad.study_references(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , pmid                   VARCHAR         NULL
          , citation               VARCHAR         NULL
          , doi                    VARCHAR         NULL	
          , type_id                INT             NULL
          , comments               VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
        );
        CREATE INDEX study_references_sid ON ad.study_references(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_titles()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_titles;
        CREATE TABLE ad.study_titles(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , title_type_id          INT             NULL
          , title_text             VARCHAR         NULL
          , lang_code              VARCHAR         NOT NULL default 'en'
          , lang_usage_id          INT             NOT NULL default 11
          , is_default             BOOLEAN         NULL
          , comments               VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
        );
        CREATE INDEX study_titles_sid ON ad.study_titles(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_people()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_people;
        CREATE TABLE ad.study_people(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , contrib_type_id        INT             NULL
          , person_given_name      VARCHAR         NULL
          , person_family_name     VARCHAR         NULL
          , person_full_name       VARCHAR         NULL
          , orcid_id               VARCHAR         NULL
          , person_affiliation     VARCHAR         NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL
        );
        CREATE INDEX study_people_sid ON ad.study_people(sd_sid);";

        Execute_SQL(sql_string);
    }
    
    
    public void create_table_study_organisations()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_organisations;
        CREATE TABLE ad.study_organisations(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , contrib_type_id        INT             NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL
        );
        CREATE INDEX study_organisations_sid ON ad.study_organisations(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_topics()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_topics;
        CREATE TABLE ad.study_topics(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , topic_type_id          INT             NULL
          , original_value         VARCHAR         NULL       
          , original_ct_type_id    INT             NULL
          , original_ct_code       VARCHAR         NULL 
          , mesh_code              VARCHAR         NULL
          , mesh_value             VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL
        );
        CREATE INDEX study_topics_sid ON ad.study_topics(sd_sid);";

        Execute_SQL(sql_string);
    }

    public void create_table_study_conditions()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_conditions;
        CREATE TABLE ad.study_conditions(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , original_value         VARCHAR         NULL
          , original_ct_type_id    INT             NULL
          , original_ct_code       VARCHAR         NULL                 
          , icd_code               VARCHAR         NULL
          , icd_name               VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL
        );
        CREATE INDEX study_conditions_sid ON ad.study_conditions(sd_sid);";

        Execute_SQL(sql_string);
    }

    public void create_table_study_features()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_features;
        CREATE TABLE ad.study_features(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , feature_type_id        INT             NULL
          , feature_value_id       INT             NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()

        );
        CREATE INDEX study_features_sid ON ad.study_features(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_links()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_links;
        CREATE TABLE ad.study_links(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , link_label             VARCHAR         NULL
          , link_url               VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
        );
        CREATE INDEX study_links_sid ON ad.study_links(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_locations()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_locations;
        CREATE TABLE ad.study_locations(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , facility_org_id        INT             NULL
          , facility               VARCHAR         NULL
          , facility_ror_id        VARCHAR         NULL
          , city_id                INT             NULL
          , city_name              VARCHAR         NULL
          , country_id             INT             NULL
          , country_name           VARCHAR         NULL
          , status_id              INT             NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL          
        );
        CREATE INDEX study_locations_sid ON ad.study_locations(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_countries()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_countries;
        CREATE TABLE ad.study_countries(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , country_id             INT             NULL
          , country_name           VARCHAR         NULL
          , status_id              INT             NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL                                         
        );
        CREATE INDEX study_countries_sid ON ad.study_countries(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_ipd_available()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_ipd_available;
        CREATE TABLE ad.study_ipd_available(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , ipd_id                 VARCHAR         NULL
          , ipd_type               VARCHAR         NULL
          , ipd_url                VARCHAR         NULL
          , ipd_comment            VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
        );
        CREATE INDEX study_ipd_available_sid ON ad.study_ipd_available(sd_sid);";

        Execute_SQL(sql_string);
    }
    
    private void create_iec_table(string table_name)
    {
        string sql_string = $@"DROP TABLE IF EXISTS ad.{table_name};
        CREATE TABLE ad.{table_name}(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , sd_sid                 VARCHAR         NOT NULL
          , seq_num                INT             NULL
          , iec_type_id            INT             NULL       
          , split_type             VARCHAR         NULL              
          , leader                 VARCHAR         NOT NULL
          , indent_level           INT             NULL
          , sequence_string        VARCHAR         NULL
          , iec_text               VARCHAR         NULL
          , iec_class_id           INT             NULL
          , iec_class              VARCHAR         NULL
          , iec_parsed_text        VARCHAR         NULL
          , added_on               TIMESTAMPTZ     NOT NULL default now()
          , coded_on               TIMESTAMPTZ     NULL                                                      
        );
        CREATE INDEX {table_name}_sid ON ad.{table_name}(sd_sid);";

        Execute_SQL(sql_string);
    }
    
    public void create_table_study_iec()
    {
        create_iec_table("study_iec");
    }
    
    public void create_table_study_iec_by_year_groups()
    {
        create_iec_table("study_iec_upto12");
        create_iec_table("study_iec_13to19");
        create_iec_table("study_iec_20on");
    }

    public void create_table_study_iec_by_years()
    {
        create_iec_table("study_iec_null");
        create_iec_table("study_iec_pre06");
        create_iec_table("study_iec_0608");
        create_iec_table("study_iec_0910");
        create_iec_table("study_iec_1112");
        create_iec_table("study_iec_1314");
        for (int i = 15; i <= 30; i++)
        {
            create_iec_table($"study_iec_{i}");
        }
    }
    
}