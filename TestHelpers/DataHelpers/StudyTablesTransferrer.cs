using Dapper;
using Npgsql;

namespace MDR_Importer;

class StudyTablesTransferrer
{
    private readonly string _source_id;
    private readonly string _db_conn;

    public StudyTablesTransferrer(int? source_id, string db_conn)
    {
        _source_id = source_id?.ToString() ?? "";
        _db_conn = db_conn;
    }


    private void Execute_SQL(string sql_string)
    {
        using var conn = new NpgsqlConnection(_db_conn);
        conn.Execute(sql_string);
    }


    public void TransferStudies()
    {
        string sql_string = @"INSERT INTO adcomp.studies (source_id, sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id,
        study_status_id, study_enrolment, study_gender_elig_id, 
        min_age, min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
        record_hash, study_full_hash) 
        SELECT " + _source_id + @", sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, 
        min_age, min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
        record_hash, study_full_hash 
        FROM ad.studies";

        Execute_SQL(sql_string);
    }


    public void TransferStudyIdentifiers()
    {
        string sql_string = @"INSERT INTO adcomp.study_identifiers(source_id, sd_sid,
        identifier_value, identifier_type_id, 
        source_id, source, source_ror_id, 
        identifier_date, identifier_link)
        SELECT " + _source_id + @", sd_sid,
        identifier_value, identifier_type_id, 
        source_id, source, source_ror_id, 
        identifier_date, identifier_link
        FROM ad.study_identifiers";

        Execute_SQL(sql_string);
    }


    public void TransferStudyRelationships()
    {

        string sql_string = @"INSERT INTO adcomp.study_relationships(source_id, sd_sid,
        relationship_type_id, target_sd_sid)
        SELECT " + _source_id + @", sd_sid,
        relationship_type_id, target_sd_sid
        FROM ad.study_relationships";

        Execute_SQL(sql_string);
    }


    public void TransferStudyReferences()
    {

        string sql_string = @"INSERT INTO adcomp.study_references(source_id, sd_sid,
        pmid, citation, doi, comments)
        SELECT " + _source_id + @", sd_sid,
        pmid, citation, doi, comments
        FROM ad.study_references";

        Execute_SQL(sql_string);
    }


    public void TransferStudyTitles()
    {

        string sql_string = @"INSERT INTO adcomp.study_titles(source_id, sd_sid,
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments)
        SELECT " + _source_id + @", sd_sid,
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments
        FROM ad.study_titles";

        Execute_SQL(sql_string);
    }


    public void TransferStudyContributors()
    {
        string sql_string = @"INSERT INTO adcomp.study_contributors(source_id, sd_sid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT " + _source_id + @", sd_sid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id
        FROM ad.study_contributors";

        Execute_SQL(sql_string);
    }


    public void TransferStudyTopics()
    {
        string sql_string = @"INSERT INTO adcomp.study_topics(source_id, sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value)
        SELECT " + _source_id + @", sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value
        FROM ad.study_topics";

        Execute_SQL(sql_string);
    }


    public void TransferStudyFeatures()
    {
        string sql_string = @"INSERT INTO adcomp.study_features(source_id, sd_sid,
        feature_type_id, feature_value_id)
        SELECT " + _source_id + @", sd_sid,
        feature_type_id, feature_value_id
        FROM ad.study_features";

        Execute_SQL(sql_string);
    }


    public void TransferStudyLinks()
    {

        string sql_string = @"INSERT INTO adcomp.study_links(source_id, sd_sid,
        link_label, link_url)
        SELECT " + _source_id + @", sd_sid,
        link_label, link_url
        FROM ad.study_links";

        Execute_SQL(sql_string);
    }


    public void TransferStudyCountries()
    {

        string sql_string = @"INSERT INTO adcomp.study_countries(source_id, sd_sid,
        country_id, country_name, status_id)
        SELECT " + _source_id + @", sd_sid,
        country_id, country_name, status_id
        FROM ad.study_countries";

        Execute_SQL(sql_string);
    }


    public void TransferStudyLocations()
    {

        string sql_string = @"INSERT INTO adcomp.study_locations(source_id, sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id)
        SELECT " + _source_id + @", sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id
        FROM ad.study_locations";

        Execute_SQL(sql_string);
    }


    public void TransferStudyIPDAvailable()
    {
        string sql_string = @"INSERT INTO adcomp.study_ipd_available(source_id, sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment)
        SELECT " + _source_id + @", sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment
        FROM ad.study_ipd_available";

        Execute_SQL(sql_string);

    }

    public void TransferStudyHashes()
    {
        string sql_string = @"INSERT INTO adcomp.study_hashes(source_id, sd_sid,
        hash_type_id, composite_hash)
        SELECT " + _source_id + @", sd_sid,
        hash_type_id, composite_hash
        FROM ad.study_hashes;";

        Execute_SQL(sql_string);
    }

}