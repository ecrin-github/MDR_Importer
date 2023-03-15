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
        if (opts.RebuildAdTables)
        {
            AdBuilder adb = new AdBuilder(source, _loggingHelper);
            adb.BuildNewAdTables();
        }
        
        // Start the data transfer.
        // Create import event log record.
        // Consider matched studies and objects - delete these first from the 
        // ad tables, re-assign ids on the ad tables, and then add ALL the 
        // sd data, matched and new, as new data.
        // (if rebuild all tables is true no need to delete any matched data first).
        
        _loggingHelper.LogHeader("Start Import Process");
 
        DataTransferManager dtm = new DataTransferManager(source, _loggingHelper);
        int importId = _monDataLayer.GetNextImportEventId();
        ImportEvent import = dtm.CreateImportEvent(importId);        
        dtm.EstablishForeignMonTables(_monDataLayer.Credentials);
        _loggingHelper.LogLine("Foreign (mon) tables established in database");

        if (!opts.RebuildAdTables)
        {
            if (source.has_study_tables is true)
            {
                dtm.DeleteMatchedStudyData(importId);
            }
            dtm.DeleteMatchedObjectData(importId);
            _loggingHelper.LogHeader("Matched data deleted from ad tables");
        }
       
        if (source.has_study_tables is true)
        {
            dtm.AddStudyData(importId);
        }
        dtm.AddObjectData(importId);
        _loggingHelper.LogHeader("New data added to ad tables");

        // Tidy up - Update the 'date imported' record in the
        // mon.source data tables. Remove foreign tables
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
        dtm.DropForeignMonTables();
        _loggingHelper.LogLine("Foreign (mon) tables removed from database");    
        _monDataLayer.StoreImportEvent(import);

    } 
}

