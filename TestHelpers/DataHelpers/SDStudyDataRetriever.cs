using Dapper;
using Npgsql;

namespace MDR_Importer;

class SDStudyDataRetriever
{
    private readonly string _sourceId;
    private readonly string _dbConn;

    public SDStudyDataRetriever(int? sourceId, string dbConn)
    {
        _sourceId = sourceId.ToString() ?? "";
        _dbConn = dbConn;
    }


    private void Execute_SQL(string sqlString)
    {
        using var conn = new NpgsqlConnection(_dbConn);
        conn.Execute(sqlString);
    }


    public void TransferStudies()
    {
        string sqlString = @"INSERT INTO sd.studies (sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, study_type,
        study_status_id, study_status, study_enrolment, study_gender_elig_id, study_gender_elig, 
        min_age, min_age_units_id, min_age_units, 
        max_age, max_age_units_id, max_age_units, datetime_of_data_fetch,
        record_hash, study_full_hash) 
        SELECT sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, study_type,
        study_status_id, study_status, study_enrolment, study_gender_elig_id, study_gender_elig,  
        min_age, min_age_units_id, min_age_units, 
        max_age, max_age_units_id, max_age_units, datetime_of_data_fetch,
        record_hash, study_full_hash 
        FROM sdcomp.studies
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }


    public void TransferStudyIdentifiers()
    {
        string sqlString = @"INSERT INTO sd.study_identifiers(sd_sid,
        identifier_value, identifier_type_id, identifier_type, 
        source_id, source, source_ror_id, 
        identifier_date, identifier_link)
        SELECT sd_sid,
        identifier_value, identifier_type_id, identifier_type, 
        source_id, source, source_ror_id, 
        identifier_date, identifier_link
        FROM sdcomp.study_identifiers
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }


    public void TransferStudyRelationships()
    {

        string sqlString = @"INSERT INTO sd.study_relationships(sd_sid,
        relationship_type_id, relationship_type, target_sd_sid)
        SELECT sd_sid,
        relationship_type_id, relationship_type, target_sd_sid
        FROM sdcomp.study_relationships
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyReferences()
    {

        string sqlString = @"INSERT INTO sd.study_references(sd_sid,
        pmid, citation, doi, comments)
        SELECT sd_sid,
        pmid, citation, doi, comments
        FROM sdcomp.study_references
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyTitles()
    {

        string sqlString = @"INSERT INTO sd.study_titles(sd_sid,
        title_type_id, title_type, title_text, lang_code, lang_usage_id,
        is_default, comments)
        SELECT sd_sid,
        title_type_id, title_type, title_text, lang_code, lang_usage_id,
        is_default, comments
        FROM sdcomp.study_titles
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }


    public void TransferStudyPeople()
    {
        string sqlString = @"INSERT INTO sd.study_contributors(sd_sid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT sd_sid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id
        FROM sdcomp.study_contributors
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }

    public void TransferStudyOrganisations()
    {
        string sqlString = @"INSERT INTO sd.study_contributors(sd_sid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT sd_sid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id
        FROM sdcomp.study_contributors
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }
    public void TransferStudyTopics()
    {
        string sqlString = @"INSERT INTO sd.study_topics(sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value)
        SELECT sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value
        FROM sdcomp.study_topics
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyFeatures()
    {
        string sqlString = @"INSERT INTO sd.study_features(sd_sid,
        feature_type_id, feature_type, feature_value_id, feature_value)
        SELECT sd_sid,
        feature_type_id, feature_type, feature_value_id, feature_value
        FROM sdcomp.study_features
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyLinks()
    {

        string sqlString = @"INSERT INTO sd.study_links(sd_sid,
        link_label, link_url)
        SELECT sd_sid,
        link_label, link_url
        FROM sdcomp.study_links
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyCountries()
    {

        string sqlString = @"INSERT INTO sd.study_countries(sd_sid,
        country_id, country_name, status_id)
        SELECT sd_sid,
        country_id, country_name, status_id
        FROM sdcomp.study_countries
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyLocations()
    {

        string sqlString = @"INSERT INTO sd.study_locations(sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id)
        SELECT sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id
        FROM sdcomp.study_locations
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyIpdAvailable()
    {
        string sqlString = @"INSERT INTO sd.study_ipd_available(sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment)
        SELECT sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment
        FROM sdcomp.study_ipd_available
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyHashes()
    {
        string sqlString = @"INSERT INTO sd.study_hashes(sd_sid,
        hash_type_id, hash_type, composite_hash)
        SELECT sd_sid,
        hash_type_id, hash_type, composite_hash
        FROM sdcomp.study_hashes
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }
}