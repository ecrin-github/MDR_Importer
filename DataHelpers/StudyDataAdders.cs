
namespace MDR_Importer;

class StudyDataAdder
{
    private readonly  DBUtilities _dbu;

    public StudyDataAdder(string db_conn, ILoggingHelper logging_helper)
    {
        _dbu = new DBUtilities(db_conn, logging_helper);
    }

   private readonly Dictionary<string, string> addFields = new() 
    {
        { "studies", @"sd_sid, display_title, 
        title_lang_code, brief_description, data_sharing_statement,
        study_start_year, study_start_month, study_type_id, 
        study_status_id, study_enrolment, study_gender_elig_id, min_age, 
        min_age_units_id, max_age, max_age_units_id, datetime_of_data_fetch" },
        { "study_identifiers", @"sd_sid, identifier_value, identifier_type_id, 
        identifier_org_id, identifier_org, identifier_org_ror_id,
        identifier_date, identifier_link" },
        { "study_titles", @"sd_sid, title_type_id, title_text, lang_code, lang_usage_id,
        is_default, comments" },
        { "study_references", @"sd_sid, pmid, citation, doi, type_id, comments" },
        { "study_people", @"sd_sid, contrib_type_id, person_given_name, 
        person_family_name, person_full_name, orcid_id, person_affiliation, organisation_id, 
        organisation_name, organisation_ror_id" },
        { "study_organisations", @"sd_sid, contrib_type_id, organisation_id, 
        organisation_name, organisation_ror_id" },
        { "study_topics", @"sd_sid, topic_type_id, original_value, original_ct_type_id, 
        original_ct_code, mesh_code, mesh_value" },
        { "study_relationships", @"sd_sid, relationship_type_id, target_sd_sid" },
        { "study_features", @"sd_sid, feature_type_id, feature_value_id" },
        { "study_links", @"sd_sid, link_label, link_url" },
        { "study_countries", @"sd_sid, country_id, country_name, status_id" },
        { "study_locations", @"sd_sid, facility_org_id, facility, facility_ror_id, 
        city_id, city_name, country_id, country_name, status_id" },
        { "study_ipd_available", @"sd_sid, ipd_id, ipd_type, ipd_url, ipd_comment" },
        { "study_conditions", @"sd_sid, original_value, original_ct_type_id, original_ct_code, 
        icd_code, icd_name" },
        { "study_iec", @"sd_sid, seq_num, leader, indent_level, level_seq_num, 
        iec_type_id, iec_text" }
    };
    
    
    public void AddData(string table_name)
    {
        string fields = addFields[table_name];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {fields}
        FROM sd.{table_name} s ";

        _dbu.ExecuteTransferSQL(sql_string, table_name, "Adding");
    }

    public void AddIECData(string fields_name, string table_name)
    {
        string fields = addFields[fields_name];
        
        string sql_string = $@"INSERT INTO ad.{table_name} ({fields})
        SELECT {fields}
        FROM sd.{table_name} s ";

        _dbu.ExecuteTransferSQL(sql_string, table_name, "Adding");
    }
    
}
