using Dapper;
using Npgsql;

namespace MDR_Importer;

internal class RetrieveSDDataBuilder
{
    private readonly int? _sourceId;
    private readonly string _dbConn;
    private readonly Source _source;

    public RetrieveSDDataBuilder(Source source)
    {
        _source = source;
        _sourceId = source.id;
        _dbConn = source.db_conn ?? "";
    }


    public void DeleteExistingSDStudyData()
    {
        DeleteData("studies");
        DeleteData("study_identifiers");
        DeleteData("study_titles");
        DeleteData("study_hashes");

        // these are database dependent

        if (_source.has_study_topics is true) DeleteData("study_topics");
        if (_source.has_study_features is true) DeleteData("study_features");
        if (_source.has_study_people is true) DeleteData("study_people");
        if (_source.has_study_organisations is true) DeleteData("study_organisations");
        if (_source.has_study_references is true) DeleteData("study_references");
        if (_source.has_study_relationships is true) DeleteData("study_relationships");
        if (_source.has_study_links is true) DeleteData("study_links");
        if (_source.has_study_countries is true) DeleteData("study_countries");
        if (_source.has_study_locations is true) DeleteData("study_locations");
        if (_source.has_study_ipd_available is true) DeleteData("study_ipd_available");
    }


    public void DeleteExistingSDObjectData()
    {
        DeleteData("data_objects");
        DeleteData("object_instances");
        DeleteData("object_titles");
        DeleteData("object_hashes");

        // these are database dependent		

        if (_source.has_object_datasets is true) DeleteData("object_datasets");
        if (_source.has_object_dates is true) DeleteData("object_dates");
        if (_source.has_object_relationships is true) DeleteData("object_relationships");
        if (_source.has_object_rights is true) DeleteData("object_rights");
        if (_source.has_object_pubmed_set is true)
        {
            DeleteData("object_contributors");
            DeleteData("object_topics");
            DeleteData("object_comments");
            DeleteData("object_descriptions");
            DeleteData("object_identifiers");
            DeleteData("object_db_links");
            DeleteData("object_publication_types");
        }
    }


    public void RetrieveStudyData()
    {
        if (_source.has_study_tables is true)
        {
            SDStudyDataRetriever sdr = new SDStudyDataRetriever(_sourceId, _dbConn);

            sdr.TransferStudies();
            sdr.TransferStudyIdentifiers();
            sdr.TransferStudyTitles();
            sdr.TransferStudyHashes();

            // these are database dependent

            if (_source.has_study_topics is true) sdr.TransferStudyTopics();
            if (_source.has_study_features is true) sdr.TransferStudyFeatures();
            if (_source.has_study_people is true) sdr.TransferStudyPeople();
            if (_source.has_study_organisations is true) sdr.TransferStudyOrganisations();
            if (_source.has_study_references is true) sdr.TransferStudyReferences();
            if (_source.has_study_relationships is true) sdr.TransferStudyRelationships();
            if (_source.has_study_links is true) sdr.TransferStudyLinks();
            if (_source.has_study_countries is true) sdr.TransferStudyCountries();
            if (_source.has_study_locations is true) sdr.TransferStudyLocations();
            if (_source.has_study_ipd_available is true) sdr.TransferStudyIpdAvailable();
        }
    }

    public void RetrieveObjectData()
    {
        SDObjectDataRetriever odr = new SDObjectDataRetriever(_sourceId, _dbConn);

        odr.TransferDataObjects();
        odr.TransferObjectInstances();
        odr.TransferObjectTitles();
        odr.TransferObjectHashes();

        // these are database dependent		

        if (_source.has_object_datasets is true) odr.TransferObjectDatasets();
        if (_source.has_object_dates is true) odr.TransferObjectDates();
        if (_source.has_object_relationships is true) odr.TransferObjectRelationships();
        if (_source.has_object_rights is true) odr.TransferObjectRights();

        if (_source.has_object_pubmed_set is true)
        {
            odr.TransferObjectContributors();
            odr.TransferObjectTopics();
            odr.TransferObjectComments();
            odr.TransferObjectDescriptions();
            odr.TransferObjectidentifiers();
            odr.TransferObjectDBLinks();
            odr.TransferObjectPublicationTypes();
        }
    }


    private void DeleteData(string table_name)
    {
        string sql_string = @"Delete from sd." + table_name;
        using var conn = new NpgsqlConnection(_dbConn);
        conn.Execute(sql_string);
    }

}