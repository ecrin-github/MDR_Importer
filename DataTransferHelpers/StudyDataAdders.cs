
namespace MDR_Importer;

class StudyDataAdder
{
    ILoggingHelper _logging_helper;
    DBUtilities dbu;

    public StudyDataAdder(string connstring, ILoggingHelper logging_helper)
    {
        _logging_helper = logging_helper;
        dbu = new DBUtilities(connstring, _logging_helper);
    }

    public void TransferStudies()
    {
        string sql_string = @"INSERT INTO ad.studies (sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, min_age, 
        min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch)
        SELECT s.sd_sid, display_title,
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, min_age, 
        min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch,
        FROM sd.studies s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "studies", "Adding");
    }

    public void TransferStudyIdentifiers()
    {
        string sql_string = @"INSERT INTO ad.study_identifiers(sd_sid,
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link)
        SELECT s.sd_sid, 
        identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link
        FROM sd.study_identifiers s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_identifiers", "Adding");
    }

    public void TransferStudyTitles()
    {
        string sql_string = @"INSERT INTO ad.study_titles(sd_sid,
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments)
        SELECT s.sd_sid, 
        title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments
        FROM sd.study_titles s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_titles", "Adding");
    }

    public void TransferStudyReferences()
    {
        string sql_string = @"INSERT INTO ad.study_references(sd_sid,
        pmid, citation, doi, comments)
        SELECT s.sd_sid, 
        pmid, citation, doi, comments
        FROM sd.study_references s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_references", "Adding");
    }

    public void TransferStudyPeople()
    {
        string sql_string = @"INSERT INTO ad.study_contributors(sd_oid, 
        contrib_type_id, person_id, person_given_name, 
        person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT s.sd_oid, 
        contrib_type_id, person_id, person_given_name, 
        person_family_name, person_full_name,
        orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id
        FROM sd.study_contributors s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_people", "Adding");
    }
    
    
    public void TransferStudyOrganisations()
    {
        string sql_string = @"INSERT INTO ad.study_contributors(sd_oid, 
        contrib_type_id, organisation_id, 
        organisation_name, organisation_ror_id)
        SELECT s.sd_oid, 
        contrib_type_id, organisation_id, 
        organisation_name, organisation_ror_id
        FROM sd.study_contributors s
        INNER JOIN sd.to_ad_object_recs nd
        ON s.sd_oid = nd.sd_oid
        WHERE nd.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_organisations", "Adding");
    }

    public void TransferStudyTopics()
    {
        /*
        string sql_string = @"INSERT INTO ad.study_locations(sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id)
        SELECT s.sd_sid, 
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id
        FROM sd.study_locations s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_topics", "Adding");
        */
    }


    public void TransferStudyRelationships()
    {
        string sql_string = @"INSERT INTO ad.study_relationships(sd_sid,
        relationship_type_id, target_sd_sid)
        SELECT s.sd_sid, 
        relationship_type_id, target_sd_sid
        FROM sd.study_relationships s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_relationships", "Adding");
    }


    public void TransferStudyFeatures()
    {
        string sql_string = @"INSERT INTO ad.study_features(sd_sid,
        feature_type_id, feature_value_id)
        SELECT s.sd_sid, 
        feature_type_id, feature_value_id
        FROM sd.study_features s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_features", "Adding");
    }


    public void TransferStudyLinks()
    {
        string sql_string = @"INSERT INTO ad.study_links(sd_sid,
        link_label, link_url)
        SELECT s.sd_sid, 
        link_label, link_url
        FROM sd.study_links s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_links", "Adding");
    }


    public void TransferStudyCountries()
    {
        string sql_string = @"INSERT INTO ad.study_countries(sd_sid,
        country_id, country_name, status_id)
        SELECT s.sd_sid, 
        country_id, country_name, status_id
        FROM sd.study_countries s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_countries", "Adding");
    }

    
    public void TransferStudyLocations()
    {
        string sql_string = @"INSERT INTO ad.study_locations(sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id)
        SELECT s.sd_sid, 
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id
        FROM sd.study_locations s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_locations", "Adding");
    }


    public void TransferStudyIpdAvailable()
    {
        string sql_string = @"INSERT INTO ad.study_ipd_available(sd_sid,
        ipd_id, ipd_type, ipd_url, ipd_comment)
        SELECT s.sd_sid, 
        ipd_id, ipd_type, ipd_url, ipd_comment
        FROM sd.study_ipd_available s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_ipd_available", "Adding");
    }
    
   
    public void TransferStudyConditions()
    {
       /*
        string sql_string = @"INSERT INTO ad.study_locations(sd_sid,
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id)
        SELECT s.sd_sid, 
        facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id
        FROM sd.study_locations s
        INNER JOIN sd.to_ad_study_recs ts
        ON s.sd_sid = ts.sd_sid
        where ts.status = 1";

        dbu.ExecuteTransferSQL(sql_string, "study_locations", "Adding");
        */
    }
    
    public void TransferStudyIEC()
    {
   /*
       string sql_string = @"INSERT INTO ad.study_locations(sd_sid,
       facility_org_id, facility, facility_ror_id, 
       city_id, city_name, country_id, country_name, status_id)
       SELECT s.sd_sid, 
       facility_org_id, facility, facility_ror_id, 
       city_id, city_name, country_id, country_name, status_id
       FROM sd.study_locations s
       INNER JOIN sd.to_ad_study_recs ts
       ON s.sd_sid = ts.sd_sid
       where ts.status = 1";
   
       dbu.ExecuteTransferSQL(sql_string, "study_locations", "Adding");
       */
    } 
}
