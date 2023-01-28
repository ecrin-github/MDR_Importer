using Dapper;
using Npgsql;

namespace MDR_Importer;

class TransferADDataBuilder
{
    private readonly int? _sourceId;
    private readonly string _dbConn;
    private readonly ISource _source;

    public TransferADDataBuilder(ISource source)
    {
        _source = source;
        _sourceId = source.id;
        _dbConn = source.db_conn ?? "";
    }


    public void DeleteExistingStudyData()
    {
        int studyNum = DeleteData(_sourceId, "studies");
        if (studyNum > 0)
        {
            DeleteData(_sourceId, "study_identifiers");
            DeleteData(_sourceId, "study_titles");
            DeleteData(_sourceId, "study_hashes");

            // these are database dependent

            if (_source.has_study_topics is true) DeleteData(_sourceId, "study_topics");
            if (_source.has_study_features is true) DeleteData(_sourceId, "study_features");
            if (_source.has_study_contributors is true) DeleteData(_sourceId, "study_contributors");
            if (_source.has_study_references is true) DeleteData(_sourceId, "study_references");
            if (_source.has_study_relationships is true) DeleteData(_sourceId, "study_relationships");
            if (_source.has_study_links is true) DeleteData(_sourceId, "study_links");
            if (_source.has_study_countries is true) DeleteData(_sourceId, "study_countgries");
            if (_source.has_study_locations is true) DeleteData(_sourceId, "study_locations");
            if (_source.has_study_ipd_available is true) DeleteData(_sourceId, "study_ipd_available");
        }
    }


    public void DeleteExistingObjectData()
    {
        int objectNum = DeleteData(_sourceId, "data_objects");
        if (objectNum > 0)
        {
            DeleteData(_sourceId, "object_instances");
            DeleteData(_sourceId, "object_titles");
            DeleteData(_sourceId, "object_hashes");

            // these are database dependent		

            if (_source.has_object_datasets is true) DeleteData(_sourceId, "object_datasets");
            if (_source.has_object_dates is true) DeleteData(_sourceId, "object_dates");
            if (_source.has_object_relationships is true) DeleteData(_sourceId, "object_relationships");
            if (_source.has_object_rights is true) DeleteData(_sourceId, "object_rights");
            if (_source.has_object_pubmed_set is true)
            {
                DeleteData(_sourceId, "object_contributors");
                DeleteData(_sourceId, "object_topics");
                DeleteData(_sourceId, "object_comments");
                DeleteData(_sourceId, "object_descriptions");
                DeleteData(_sourceId, "object_identifiers");
                DeleteData(_sourceId, "object_db_links");
                DeleteData(_sourceId, "object_publication_types");
            }
        }
    }


    public void TransferStudyData()
    {
        if (_source.has_study_tables is true)
        {
            StudyTablesTransferrer stt = new StudyTablesTransferrer(_sourceId, _dbConn);

            stt.TransferStudies();
            stt.TransferStudyIdentifiers();
            stt.TransferStudyTitles();
            stt.TransferStudyHashes();

            // these are database dependent

            if (_source.has_study_topics is true) stt.TransferStudyTopics();
            if (_source.has_study_features is true) stt.TransferStudyFeatures();
            if (_source.has_study_contributors is true) stt.TransferStudyContributors();
            if (_source.has_study_references is true) stt.TransferStudyReferences();
            if (_source.has_study_relationships is true) stt.TransferStudyRelationships();
            if (_source.has_study_links is true) stt.TransferStudyLinks();
            if (_source.has_study_countries is true) stt.TransferStudyCountries();
            if (_source.has_study_locations is true) stt.TransferStudyLocations();
            if (_source.has_study_ipd_available is true) stt.TransferStudyIPDAvaiable();
        }

    }


    public void TransferObjectData()
    {
        ObjectTablesTransferrer ott = new ObjectTablesTransferrer(_sourceId, _dbConn);

        ott.TransferDataObjects();
        ott.TransferObjectInstances();
        ott.TransferObjectTitles();
        ott.TransferObjectHashes();

        // these are database dependent		

        if (_source.has_object_datasets is true) ott.TransferObjectDatasets();
        if (_source.has_object_dates is true) ott.TransferObjectDates();
        if (_source.has_object_relationships is true) ott.TransferObjectRelationships();
        if (_source.has_object_rights is true) ott.TransferObjectRights();

        if (_source.has_object_pubmed_set is true)
        {
            ott.TransferObjectContributors();
            ott.TransferObjectTopics();
            ott.TransferObjectComments();
            ott.TransferObjectDescriptions();
            ott.TransferObjectidentifiers();
            ott.TransferObjectDBLinks();
            ott.TransferObjectPublicationTypes();
        }
    }

    
    private int DeleteData(int? sourceId, string tableName)
    {
        string srceId = sourceId?.ToString() ?? "";
        string sqlString = $@"Delete from adcomp.{tableName} where source_id = {srceId}";
        using var conn = new NpgsqlConnection(_dbConn);
        return conn.Execute(sqlString);
    }

}