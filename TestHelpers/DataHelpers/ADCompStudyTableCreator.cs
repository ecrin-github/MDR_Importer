using Dapper;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Text;

namespace MDR_Importer;

public class ADCompStudyTableCreator
{
    string _db_conn;

    public ADCompStudyTableCreator(string db_conn)
    {
        _db_conn = db_conn;
    }

    private void Execute_SQL(string sql_string)
    {
        using (var conn = new NpgsqlConnection(_db_conn))
        {
            conn.Execute(sql_string);
        }
    }

    public void create_table_studies()
    {
        string sql_string = @"CREATE SCHEMA IF NOT EXISTS adcomp;
        DROP TABLE IF EXISTS adcomp.studies;
        CREATE TABLE adcomp.studies(
              id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 101 INCREMENT BY 1) PRIMARY KEY
            , source_id              INT             NOT NULL
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
            , datetime_of_data_fetch TIMESTAMPTZ     NULL
                        CHAR(32)        NULL
            , study_full_hash        CHAR(32)        NULL
        );
        CREATE INDEX studies_sid ON adcomp.studies(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_identifiers()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_identifiers;
        CREATE TABLE adcomp.study_identifiers(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , identifier_type_id     INT             NULL
          , identifier_value       VARCHAR         NULL
          , identifier_org_id      INT             NULL
          , identifier_org         VARCHAR         NULL
          , identifier_org_ror_id  VARCHAR         NULL
          , identifier_date        VARCHAR         NULL
          , identifier_link        VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_identifiers_sd_sid ON adcomp.study_identifiers(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_relationships()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_relationships;
        CREATE TABLE adcomp.study_relationships(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , relationship_type_id   INT             NULL
          , target_sd_sid          VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_relationships_sd_sid ON adcomp.study_relationships(sd_sid);
        CREATE INDEX study_relationships_target_sd_sid ON adcomp.study_relationships(target_sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_references()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_references;
        CREATE TABLE adcomp.study_references(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , pmid                   VARCHAR         NULL
          , citation               VARCHAR         NULL
          , doi                    VARCHAR         NULL	
          , comments               VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_references_sd_sid ON adcomp.study_references(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_titles()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_titles;
        CREATE TABLE adcomp.study_titles(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , title_type_id          INT             NULL
          , title_text             VARCHAR         NULL
          , lang_code              VARCHAR         NOT NULL default 'en'
          , lang_usage_id          INT             NOT NULL default 11
          , is_default             BOOLEAN         NULL
          , comments               VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_titles_sd_sid ON adcomp.study_titles(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_contributors()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_contributors;
        CREATE TABLE adcomp.study_contributors(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , contrib_type_id        INT             NULL
          , is_individual          BOOLEAN         NULL
          , person_id              INT             NULL
          , person_given_name      VARCHAR         NULL
          , person_family_name     VARCHAR         NULL
          , person_full_name       VARCHAR         NULL
          , orcid_id               VARCHAR         NULL
          , person_affiliation     VARCHAR         NULL
          , organisation_id        INT             NULL
          , organisation_name      VARCHAR         NULL
          , organisation_ror_id    VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_contributors_sd_sid ON adcomp.study_contributors(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_topics()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_topics;
        CREATE TABLE adcomp.study_topics(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , topic_type_id          INT             NULL
          , mesh_coded             BOOLEAN         NULL
          , mesh_code              VARCHAR         NULL
          , mesh_value             VARCHAR         NULL
          , original_ct_id         INT             NULL
          , original_ct_code       VARCHAR         NULL
          , original_value         VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_topics_sd_sid ON adcomp.study_topics(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_features()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_features;
        CREATE TABLE adcomp.study_features(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , feature_type_id        INT             NULL
          , feature_value_id       INT             NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_features_sd_sid ON adcomp.study_features(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_links()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_links;
        CREATE TABLE adcomp.study_links(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , link_label             VARCHAR         NULL
          , link_url               VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_links_sd_sid ON adcomp.study_links(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_locations()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_locations;
        CREATE TABLE ad.study_locations(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , facility_org_id        INT             NULL
          , facility               VARCHAR         NULL
          , facility_ror_id        VARCHAR         NULL
          , city_id                INT             NULL
          , city_name              VARCHAR         NULL
          , country_id             INT             NULL
          , country_name           VARCHAR         NULL
          , status_id              INT             NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_locations_sd_sid ON ad.study_locations(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_countries()
    {
        string sql_string = @"DROP TABLE IF EXISTS ad.study_countries;
        CREATE TABLE ad.study_countries(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , country_id             INT             NULL
          , country_name           VARCHAR         NULL
          , status_id              INT             NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_countries_sd_sid ON ad.study_countries(sd_sid);";

        Execute_SQL(sql_string);
    }

    public void create_table_ipd_available()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_ipd_available;
        CREATE TABLE adcomp.study_ipd_available(
            id                     INT             GENERATED ALWAYS AS IDENTITY(START WITH 201 INCREMENT BY 1) PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , ipd_id                 VARCHAR         NULL
          , ipd_type               VARCHAR         NULL
          , ipd_url                VARCHAR         NULL
          , ipd_comment            VARCHAR         NULL
                      CHAR(32)        NULL
        );
        CREATE INDEX study_ipd_available_sd_sid ON adcomp.study_ipd_available(sd_sid);";

        Execute_SQL(sql_string);
    }


    public void create_table_study_hashes()
    {
        string sql_string = @"DROP TABLE IF EXISTS adcomp.study_hashes;
        CREATE TABLE adcomp.study_hashes(
            id                     INT             GENERATED ALWAYS AS IDENTITY PRIMARY KEY
          , source_id              INT             NOT NULL
          , sd_sid                 VARCHAR         NOT NULL
          , hash_type_id           INT             NULL
          , hash_type              VARCHAR         NULL
          , composite_hash         CHAR(32)        NULL
        );
        CREATE INDEX study_hashes_sd_sid ON adcomp.study_hashes(sd_sid);";

        Execute_SQL(sql_string);
    }
}