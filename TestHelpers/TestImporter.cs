namespace MDR_Importer;

public class TestImporter
{
    private readonly ITestingDataLayer _testRepo;
    private readonly IMonDataLayer _monDataLayer;
    private readonly ILoggingHelper _loggingHelper;

    public TestImporter(IMonDataLayer monDataLayer, 
                        ITestingDataLayer testRepo, ILoggingHelper loggingHelper)
    {
        _monDataLayer = monDataLayer;
        _testRepo = testRepo;
        _loggingHelper = loggingHelper;
    }
    
    
    public void Run(Options opts)
    {    
        // one or both of -F, -G have been used
        // 'F' = is a test, If present, operates on the sd / ad tables in the test database
        // 'G' = test report, If present, compares and reports on adcomp versus expected tables
        // but does not recreate those tables first. -F and -G frequently used in combination
        
        _loggingHelper.OpenLogFile("test");

        if (opts.UsingTestData is true)
        {
            // First recreate the AD composite tables.
            // These will hold the AD data for all imported studies,
            // collected per source after each import, and 
            // thus available for comparison with the expected data

            _testRepo.SetUpADCompositeTables();

            // Go through and import each test source.
            // In test context, the small 'sd' test databases are
            // imported one at a time, into equally small ad tables.

            foreach (int sourceId in opts.SourceIds!)
            {
                Source? source = _monDataLayer.FetchSourceParameters(sourceId);
                if (source is not null)
                {
                    opts.RebuildAdTables = true;
                    ImportDataInTest(source, opts);
                    _loggingHelper.LogHeader("ENDING " + sourceId.ToString() + ": " + source.database_name +
                                              " first test pass");
                }
            }

            // make scripted changes to the ad tables to
            // create diffs between them.

            _testRepo.ApplyScriptedADChanges();

            // Go through each test source again,
            // this time keeping the ad tables.

            foreach (int sourceId in opts.SourceIds)
            {
                Source? source = _monDataLayer.FetchSourceParameters(sourceId);
                if (source is not null)
                {
                    opts.RebuildAdTables = false;
                    ImportDataInTest(source, opts);
                    _loggingHelper.LogHeader("ENDING " + sourceId.ToString() + ": " + source.database_name +
                                              " second test pass");
                }
            }
        }
        
        if (opts.CreateTestReport is true)
        {
            // construct a log detailing differences between the
            // expected and actual (composite ad) values.

            _testRepo.ConstructDiffReport();
        }
    }
    
    
    private void ImportDataInTest(Source source, Options opts)
    {
        // Obtain source details, augment with connection string for this database.

        source.db_conn = _monDataLayer.GetConnectionString(source.database_name!, true);
        _loggingHelper.LogStudyHeader(opts, "For source: " + source.id + ": " + source.database_name!);
        _loggingHelper.LogHeader("Setup");

        // First need to copy sd data back from composite
        // sd tables to the sd tables for this source.
        
        _testRepo.RetrieveSDData(source);
        
        // Recreate ad tables if necessary. If the second pass of a 
        // test loop will need to retrieve the ad data back from compad

        if (opts.RebuildAdTables is true)
        {
            AdBuilder adb = new AdBuilder(source, _loggingHelper);
            adb.BuildNewAdTables();
        }
        else
        {
            _testRepo.RetrieveADData(source);
        }
        
        // Create import event log record.
        // Create and fill temporary tables to hold ids and statuses  
        // of new or matched sd studies and sd data objects.
        
        // Establish top level builder classes and 
        // set up sf monitor tables as foreign tables, temporarily.
        
        _loggingHelper.LogHeader("Start Import Process");
        _loggingHelper.LogHeader("Create and fill diff tables");
        
        DataTransferManager dtm = new DataTransferManager(source, _loggingHelper);
        _loggingHelper.LogLine("Foreign (mon) tables established in database");
        int importId = _monDataLayer.GetNextImportEventId();

        // Start the data transfer.
        // Consider matched studies and objects - delete these first from the 
        // ad tables, re-assign ids on the ad tables, and then add ALL the 
        // sd data, matched and new, as new data.
        // (if rebuild all tables is true no need to delete any matched data first)

       _loggingHelper.LogLine("Foreign (mon) tables established in database");     

        if (source.has_study_tables is true)
        {
            dtm.DeleteMatchedStudyData(importId);
        }
        dtm.DeleteMatchedObjectData(importId);
        _loggingHelper.LogHeader("Matched data deleted from ad tables");
        
        if (source.has_study_tables is true)
        {
            dtm.AddStudyData(importId);
        }
        dtm.AddObjectData(importId);
        _loggingHelper.LogHeader("New data added to ad tables");

        // Copy ad data from ad tables to the compad tables...
        // Tidy up by removing monitoring tables
        
        _testRepo.TransferADDataToComp(source);        
    } 
}
 