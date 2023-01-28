
namespace MDR_Importer;

class ImportBuilder
{
    private readonly Source _source;
    private readonly ILoggingHelper _loggingHelper;
    private readonly ImportTableCreator _itc;
    private readonly ImportTableManager _itm;

    public ImportBuilder(Source source, ILoggingHelper loggingHelper)
    {
        _source = source;
        _loggingHelper = loggingHelper;
        
        var connString = _source.db_conn ?? "";
        _itc = new ImportTableCreator(connString);
        _itm = new ImportTableManager(connString);
    }

    public void CreateImportTables()
    {
        if (_source.has_study_tables is true)
        {
            _itc.CreateStudyRecsToADTable();
            _itc.CreateStudyAttsToADTable();
            _loggingHelper.LogLine("Created studies to_ad tables");
        }
        _itc.CreateObjectRecsToADTable();
        _itc.CreateObjectAttsToADTable();
        _loggingHelper.LogLine("Created data objects to_ad tables");
    }


    public void FillImportTables(bool countDeleted)
    {
        if (_source.has_study_tables is true)
        {
            _itm.IdentifyNewStudies(); 
            _itm.IdentifyIdenticalStudies();
            _itm.IdentifyEditedStudies();
            if (countDeleted) _itm.IdentifyDeletedStudies();
            _itm.IdentifyChangedStudyRecs();
        }

        _loggingHelper.LogLine("Filled studies to_ad table");

        _itm.IdentifyNewDataObjects();
        _itm.IdentifyIdenticalDataObjects();
        _itm.IdentifyEditedDataObjects();
        if (countDeleted) _itm.IdentifyDeletedDataObjects();
        _itm.IdentifyChangedObjectRecs();
        if (_source.has_object_datasets is true) _itm.IdentifyChangedDatasetRecs();

        _loggingHelper.LogLine("Filled data objects to_ad table");

        if (_source.has_study_tables is true)
        {
            _itm.SetUpTempStudyAttSets();
            _itm.IdentifyChangedStudyAtts();
            _itm.IdentifyNewStudyAtts();
            if (countDeleted) _itm.IdentifyDeletedStudyAtts();
        }

        _loggingHelper.LogLine("Filled study atts table");

        _itm.SetUpTempObjectAttSets();
        _itm.IdentifyChangedObjectAtts();
        _itm.IdentifyNewObjectAtts();
        if (countDeleted) _itm.IdentifyDeletedObjectAtts();
        _itm.DropTempAttSets();
        _loggingHelper.LogLine("Filled data objects atts table");
    }

    public ImportEvent CreateImportEvent(int importId)
    {
        return _itm.CreateImportEvent(importId, _source);
    }
}