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
                source.db_conn = _monDataLayer.GetConnectionString(dbName);
                
                _loggingHelper.OpenLogFile(source.database_name!);
                _loggingHelper.LogHeader("STARTING IMPORTER");
                _loggingHelper.LogCommandLineParameters(opts);
                _loggingHelper.LogStudyHeader(opts, "For source: " + source.id + ": " + dbName);
                
                ImportData(source, opts);
                
                _loggingHelper.CloseLog();
            }
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
        // Recreate ad tables if necessary.

        _loggingHelper.LogHeader("Setup");
        if (opts.RebuildAdTables)
        {
            AdBuilder adb = new AdBuilder(source, _loggingHelper);
            adb.BuildNewAdTables();
        }
        
        // Establish monitoring schema as foreign tables and the transfer manager class
        // that orchestrates the transfer, then create import event log record.
        
        _loggingHelper.LogHeader("Start Import Process");
        ForeignTableManager ftm = new ForeignTableManager(source, _loggingHelper);
        ftm.EstablishForeignMonTables(_monDataLayer.Credentials);
        DataTransferManager dtm = new DataTransferManager(source, opts.RebuildAdTables, _loggingHelper);
        int importId = _monDataLayer.GetNextImportEventId();
        ImportEvent import = dtm.CreateImportEvent(importId, opts.RebuildAdTables);     
        
        // Then import the study data (if present) followed by the object data. For details
        // see the transfer manager class and the adder / deleter classes for studies and objects.

        if (source.has_study_tables is true)
        {
            dtm.ImportStudyData(import);
        }
        dtm.ImportObjectData(import);
        _loggingHelper.LogHeader("New data added to ad tables");

        // Tidy up - Update the 'date imported' record in the mon.source data tables.
        // Remove foreign tables and store import event.      
        
        _loggingHelper.LogHeader("Tidy up and finish");
        if (source.has_study_tables is true)
        {
            ftm.UpdateStudiesImportedDateInMon(importId);
        }
        else
        {
            ftm.UpdateObjectsImportedDateInMon(importId);   // only do if no studies (e.g. PubMed)
        }
        ftm.DropForeignMonTables();
        _monDataLayer.StoreImportEvent(import);
    } 
}

