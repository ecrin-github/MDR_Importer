﻿using Dapper;
using Npgsql;
namespace MDR_Importer;

internal class AdStudyDataRetriever
{
    private readonly string _sourceId;
    private readonly string _dbConn;

    public AdStudyDataRetriever(int? sourceId, string dbConn)
    {
        _sourceId = sourceId.ToString() ?? "";
        _dbConn = dbConn;
    }

    private void Execute_SQL(string sqlString)
    {
        using NpgsqlConnection conn = new(_dbConn);
        conn.Execute(sqlString);
    }


    public void TransferStudies()
    {
        string sqlString = @"INSERT INTO ad.studies (sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id,
        study_status_id, study_enrolment, study_gender_elig_id, 
        min_age, min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
        record_hash, study_full_hash) 
        SELECT sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, 
        min_age, min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
        record_hash, study_full_hash 
        FROM adcomp.studies
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyIdentifiers()
    {
        string sqlString = @"INSERT INTO ad.study_identifiers(sd_sid,
        identifier_value, identifier_type_id, source_id, source_ror_id, 
        source, identifier_date, identifier_link)
        SELECT sd_sid,
        identifier_value, identifier_type_id, source_id, source_ror_id, 
        source, identifier_date, identifier_link
        FROM adcomp.study_identifiers
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyRelationships()
    {

        string sqlString = @"INSERT INTO ad.study_relationships(sd_sid,
        relationship_type_id, target_sd_sid)
        SELECT sd_sid,
        relationship_type_id, target_sd_sid
        FROM adcomp.study_relationships
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyReferences()
    {

        string sqlString = @"INSERT INTO ad.study_references(sd_sid,
        pmid, citation, doi, comments)
        SELECT sd_sid,
        pmid, citation, doi, comments
        FROM adcomp.study_references
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyTitles()
    {

        string sqlString = @"INSERT INTO ad.study_titles(sd_sid,
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments)
        SELECT sd_sid,
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments
        FROM adcomp.study_titles
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }


    public void TransferStudyPeople()
    {
        string sqlString = @"INSERT INTO ad.study_contributors(sd_sid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT sd_sid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id
        FROM adcomp.study_contributors
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }
    
    
    public void TransferStudyOrganisations()
    {
        string sqlString = @"INSERT INTO ad.study_contributors(sd_sid, 
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT sd_sid,
        contrib_type_id, is_individual, 
        person_id, person_given_name, person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id
        FROM adcomp.study_contributors
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }


    public void TransferStudyTopics()
    {
        string sqlString = @"INSERT INTO ad.study_topics(sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value)
        SELECT sd_sid,
        topic_type_id, mesh_coded, mesh_code, mesh_value, 
        original_ct_id, original_ct_code,
        original_value
        FROM adcomp.study_topics
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyFeatures()
    {
        string sqlString = @"INSERT INTO ad.study_features(sd_sid,
        feature_type_id, feature_value_id)
        SELECT sd_sid,
        feature_type_id, feature_value_id
        FROM adcomp.study_features
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyLinks()
    {

        string sqlString = @"INSERT INTO ad.study_links(sd_sid,
        link_label, link_url)
        SELECT sd_sid,
        link_label, link_url
        FROM adcomp.study_links
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyCountries()
    {

        string sqlString = @"INSERT INTO ad.study_countries(sd_sid,
        country_id, country_name, status_id)
        SELECT sd_sid,
        country_id, country_name, status_id
        FROM adcomp.study_countries
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyLocations()
    {

        string sqlString = @"INSERT INTO ad.study_locations(sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id)
        SELECT sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id
        FROM adcomp.study_locations
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }


    public void TransferStudyIpdAvailable()
    {
        string sqlString = @"INSERT INTO ad.study_ipd_available(sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment)
        SELECT sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment
        FROM adcomp.study_ipd_available
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);

    }

    public void TransferStudyHashes()
    {
        string sqlString = @"INSERT INTO ad.study_hashes(sd_sid,
        hash_type_id, composite_hash)
        SELECT sd_sid,
        hash_type_id, composite_hash
        FROM adcomp.study_hashes
        where source_id = " + _sourceId;

        Execute_SQL(sqlString);
    }
}