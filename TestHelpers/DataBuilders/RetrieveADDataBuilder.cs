﻿using Dapper;
using Npgsql;


namespace MDR_Importer;

class RetrieveADDataBuilder
{

    private readonly int? _sourceId;
    private readonly string _dbConn;
    private ISource _source;

    public RetrieveADDataBuilder(ISource source)
    {
        _source = source;
        _sourceId = source.id;
        _dbConn = source.db_conn ?? "";
    }


    public void DeleteExistingADStudyData()
    {
        DeleteData("studies");
        DeleteData("study_identifiers");
        DeleteData("study_titles");
        DeleteData("study_hashes");

        // these are database dependent

        if (_source.has_study_topics is true) DeleteData("study_topics");
        if (_source.has_study_features is true) DeleteData("study_features");
        if (_source.has_study_contributors is true) DeleteData("study_contributors");
        if (_source.has_study_references is true) DeleteData("study_references");
        if (_source.has_study_relationships is true) DeleteData("study_relationships");
        if (_source.has_study_links is true) DeleteData("study_links");
        if (_source.has_study_countries is true) DeleteData("study_countries");
        if (_source.has_study_locations is true) DeleteData("study_locatgions");
        if (_source.has_study_ipd_available is true) DeleteData("study_ipd_available");
    }


    public void DeleteExistingADObjectData()
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
        AdStudyDataRetriever adr = new (_sourceId, _dbConn);

        adr.TransferStudies();
        adr.TransferStudyIdentifiers();
        adr.TransferStudyTitles();
        adr.TransferStudyHashes();

        // these are database dependent

        if (_source.has_study_topics is true) adr.TransferStudyTopics();
        if (_source.has_study_features is true) adr.TransferStudyFeatures();
        if (_source.has_study_contributors is true) adr.TransferStudyContributors();
        if (_source.has_study_references is true) adr.TransferStudyReferences();
        if (_source.has_study_relationships is true) adr.TransferStudyRelationships();
        if (_source.has_study_links is true) adr.TransferStudyLinks();
        if (_source.has_study_countries is true) adr.TransferStudyCountries();
        if (_source.has_study_locations is true) adr.TransferStudyLocations();
        if (_source.has_study_ipd_available is true) adr.TransferStudyIpdAvailable();
    }


    public void RetrieveObjectData()
    {
        ADObjectDataRetriever odr = new ADObjectDataRetriever(_sourceId, _dbConn);

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


    private int DeleteData(string table_name)
    {
        int res = 0;
        string sql_string = @"Delete from ad." + table_name;

        using (var conn = new NpgsqlConnection(_dbConn))
        {
            return res = conn.Execute(sql_string);
        }
    }
}