using Dapper.Contrib.Extensions;
namespace MDR_Importer;

[Table("sf.source_parameters")]
public class Source
{
    public int id { get; }
    public string? source_type { get; }
    public int? preference_rating { get; }
    public string? database_name { get; }
    public string? repo_name { get; }
    public string? db_conn { get; set; }
    public bool? uses_who_harvest { get; }
    public int? harvest_chunk { get; }
    public string? local_folder { get; }
    public bool? local_files_grouped { get; }
    public int? grouping_range_by_id { get; }
    public string? local_file_prefix { get; }
    public bool? has_study_tables { get; }
    public bool? has_study_topics { get; }
    public bool? has_study_conditions { get; }
    public bool? has_study_features { get; }
    public bool? has_study_iec{ get; }
    public bool? has_study_people{ get; }
    public bool? has_study_organisations{ get; }
    public bool? has_study_references { get; }
    public bool? has_study_relationships { get; }
    public bool? has_study_countries { get; }
    public bool? has_study_locations { get; }
    public bool? has_study_links { get; }
    public bool? has_study_ipd_available { get; }
    public string? study_iec_storage_type { get; }
    public bool? has_object_datasets { get; }
    public bool? has_object_instances { get; }
    public bool? has_object_dates { get; }
    public bool? has_object_descriptions { get; }
    public bool? has_object_identifiers { get; }
    public bool? has_object_people { get; }
    public bool? has_object_organisations { get; }
    public bool? has_object_topics { get; }
    public bool? has_object_comments { get; }
    public bool? has_object_db_links { get; }
    public bool? has_object_publication_types { get; }
    public bool? has_journal_details { get; }
    public bool? has_object_rights { get; }
    public bool? has_object_relationships { get; }

}


[Table("sf.import_events")]
public class ImportEvent
{
    [ExplicitKey]
    public int? id { get; set; }
    public int? source_id { get; set; }
    public DateTime? time_started { get; set; }
    public DateTime? time_ended { get; set; }
    public bool? tables_rebuilt { get; set; }
    public int? num_sd_studies { get; set; }
    public int? num_matched_studies { get; set; } = 0;
    public int? num_sd_objects { get; set; }
    public int? num_matched_objects { get; set; } = 0;
    public string? comments { get; set; }

    public ImportEvent(int? _id, int? _source_id, bool? _tables_rebuilt)
    {
        id = _id;
        source_id = _source_id;
        time_started = DateTime.Now;
        tables_rebuilt = _tables_rebuilt;
    }
}

