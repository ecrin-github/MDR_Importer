namespace MDR_Importer;

class AdBuilder
{
    private readonly ISource _source;
    private readonly ILoggingHelper _loggingHelper;

    private readonly StudyTableBuilders _studyBuilder;
    private readonly ObjectTableBuilders _objectBuilder;

    public AdBuilder(ISource source, ILoggingHelper loggingHelper)
    {
        _source = source;
        _loggingHelper = loggingHelper;        
        
        var connString = _source.db_conn ?? "";
        _studyBuilder = new StudyTableBuilders(connString);
        _objectBuilder = new ObjectTableBuilders(connString);
    }


    public void BuildNewAdTables()
    {
        if (_source.has_study_tables is true)
        {
            // these common to all databases

            _studyBuilder.create_ad_schema();
            _studyBuilder.create_table_studies();
            _studyBuilder.create_table_study_identifiers();
            _studyBuilder.create_table_study_titles();
            _studyBuilder.create_table_study_hashes();

            // these are database dependent
            if (_source.has_study_topics is true) _studyBuilder.create_table_study_topics();
            if (_source.has_study_features is true) _studyBuilder.create_table_study_features();
            if (_source.has_study_contributors is true) _studyBuilder.create_table_study_contributors();
            if (_source.has_study_references is true) _studyBuilder.create_table_study_references();
            if (_source.has_study_relationships is true) _studyBuilder.create_table_study_relationships();
            if (_source.has_study_links is true) _studyBuilder.create_table_study_links();
            if (_source.has_study_countries is true) _studyBuilder.create_table_study_countries();
            if (_source.has_study_locations is true) _studyBuilder.create_table_study_locations();
            if (_source.has_study_ipd_available is true) _studyBuilder.create_table_ipd_available();

            _loggingHelper.LogLine("Rebuilt AD study tables");
        }

        // object tables - these common to all databases

        _objectBuilder.create_table_data_objects();
        _objectBuilder.create_table_object_instances();
        _objectBuilder.create_table_object_titles();
        _objectBuilder.create_table_object_hashes();

        // these are database dependent		

        if (_source.has_object_datasets is true) _objectBuilder.create_table_object_datasets();
        if (_source.has_object_dates is true) _objectBuilder.create_table_object_dates();
        if (_source.has_object_relationships is true) _objectBuilder.create_table_object_relationships();
        if (_source.has_object_rights is true) _objectBuilder.create_table_object_rights();
        if (_source.has_object_pubmed_set is true)
        {
            _objectBuilder.create_table_object_contributors();
            _objectBuilder.create_table_object_topics();
            _objectBuilder.create_table_object_comments();
            _objectBuilder.create_table_object_descriptions();
            _objectBuilder.create_table_object_identifiers();
            _objectBuilder.create_table_object_db_links();
            _objectBuilder.create_table_object_publication_types();
        }

        _loggingHelper.LogLine("Rebuilt AD object tables");
    }
}