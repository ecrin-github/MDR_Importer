namespace MDR_Importer;

public class Importer
{
    private readonly ILoggingHelper _loggingHelper;
    private readonly IMonDataLayer _monDataLayer;

    public Importer(IMonDataLayer monDataLayer, ILoggingHelper loggingHelper)
    {
        _monDataLayer = monDataLayer;
        _loggingHelper = loggingHelper;
    }
    
    public void Run(Options opts)
    {
        try
        {
            // Simply import the data for each listed source.

            foreach (int sourceId in opts.SourceIds!)
            {
                // Obtain source details, augment with connection string for this database
                // Open up the logging file for this source and then call the main 
                // import routine. After initial checks source is guaranteed to be non-null.
                    
                Source source = _monDataLayer.FetchSourceParameters(sourceId)!;
                string dbName = source.database_name!;
                source.db_conn = _monDataLayer.GetConnectionString(dbName, false);
                
                _loggingHelper.OpenLogFile(source.database_name!);
                _loggingHelper.LogHeader("STARTING IMPORTER");
                _loggingHelper.LogCommandLineParameters(opts);
                _loggingHelper.LogStudyHeader(opts, "For source: " + source.id + ": " + dbName);
                
                ImportData(source, opts);
                
                //_loggingHelper.LogImportSummary(source);
                _loggingHelper.CloseLog();
            }

            _loggingHelper.CloseLog();
        }

        catch (Exception e)
        {
            _loggingHelper.LogHeader("UNHANDLED EXCEPTION");
            _loggingHelper.LogCodeError("Importer application aborted", e.Message, e.StackTrace);
            _loggingHelper.CloseLog();
        }
    }

    private void ImportData(Source source, Options opts)
    {
        // Obtain source details, augment with connection string for this database.
        // Establish top level builder classes and 
        // Set up sf monitor tables as foreign tables, temporarily.
        // Recreate ad tables if necessary.

        _loggingHelper.LogHeader("Setup");
        ImportBuilder ib = new ImportBuilder(source, _loggingHelper);
        DataTransferrer dataTransferrer = new DataTransferrer(source, _loggingHelper);
        dataTransferrer.EstablishForeignMonTables(_monDataLayer.Credentials);
        _loggingHelper.LogLine("Foreign (mon) tables established in database");
        if (opts.RebuildAdTables is true)
        {
            AdBuilder adb = new AdBuilder(source, _loggingHelper);
            adb.BuildNewAdTables();
        }
        
        // Create and fill temporary tables to hold ids and edit statuses  
        // of new, edited, deleted studies and data objects.

        _loggingHelper.LogHeader("Start Import Process");
        _loggingHelper.LogHeader("Create and fill diff tables");
        ib.CreateImportTables();
        bool countDeleted = _monDataLayer.CheckIfFullHarvest(source.id);
        ib.FillImportTables(countDeleted);
        _loggingHelper.LogDiffs(source);

        // Create import event log record and start 
        // the data transfer proper...

        int importId = _monDataLayer.GetNextImportEventId();
        ImportEvent import = ib.CreateImportEvent(importId);

        // Consider new studies, record dates, edited studies and / or objects,
        // and any deleted studies / objects

        _loggingHelper.LogHeader("Adding new data");
        if (source.has_study_tables is true)
        {
            dataTransferrer.AddNewStudies(importId);
        }
        dataTransferrer.AddNewDataObjects(importId);

        _loggingHelper.LogHeader("Editing existing data where necessary");
        if (source.has_study_tables is true)
        {
            dataTransferrer.UpdateEditedStudyData(importId);
        }
        dataTransferrer.UpdateEditedDataObjectData(importId);

        _loggingHelper.LogHeader("Updating dates of data");
        dataTransferrer.UpdateDatesOfData();
        
        _loggingHelper.LogHeader("Deleting data no longer present in source");
        if (source.has_study_tables is true)
        {
            dataTransferrer.RemoveDeletedStudyData(importId);
        }
        dataTransferrer.RemoveDeletedDataObjectData(importId);

        // Tidy up - 
        // Update the 'date imported' record in the mon.source data tables
        // Affects all records with status 1, 2 or 3 (non-test imports only)   
        // Remove foreign tables
        // Store import event for non-test imports.      
        
        _loggingHelper.LogHeader("Tidy up and finish");
        if (source.has_study_tables is true)
        {
            _monDataLayer.UpdateStudiesLastImportedDate(importId, source.id);
        }
        else
        {
            // only do the objects table if there are no studies (e.g. PubMed)
            _monDataLayer.UpdateObjectsLastImportedDate(importId, source.id);
        }
        dataTransferrer.DropForeignMonTables();
        _loggingHelper.LogLine("Foreign (mon) tables removed from database");    
        _monDataLayer.StoreImportEvent(import);

    } 
}

