using System.Reflection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MDR_Importer;

// Set up file based configuration environment.

string AssemblyLocation = Assembly.GetExecutingAssembly().Location;
string? BasePath = Path.GetDirectoryName(AssemblyLocation);
if (string.IsNullOrWhiteSpace(BasePath))
{
    return -1;
}

var configFiles = new ConfigurationBuilder()
    .SetBasePath(BasePath)
    .AddJsonFile("appsettings.json")
    .AddJsonFile($"appsettings.{Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")}.json", true)
    .Build();

// Set up the host for the app,
// adding the services used in the system to support DI
// and including Serilog

IHost host = Host.CreateDefaultBuilder()
    .UseContentRoot(BasePath)
    .ConfigureAppConfiguration(builder =>
    {
        builder.AddConfiguration(configFiles); 
        
    })
    .ConfigureServices((hostContext, services) =>
    {
        // Register services (or develop a comp root)
        services.AddSingleton<ICredentials, Credentials>();
        services.AddSingleton<ILoggingHelper, LoggingHelper>();
        services.AddSingleton<IMonDataLayer, MonDataLayer>();        
        services.AddSingleton<IImporter, Importer>();
        services.AddSingleton<ITestingDataLayer, TestingDataLayer>();
        services.AddTransient<ISource, Source>();
    })
    .Build();

// Establish logger, at this stage as an object reference
// because the log file(s) are yet to be opened.
// Establish a new credentials class, and use both to establish the monitor and test
// data (repository) layers. ALL of these classes are singletons.

LoggingHelper loggingHelper = ActivatorUtilities.CreateInstance<LoggingHelper>(host.Services);
Credentials credentials = ActivatorUtilities.CreateInstance<Credentials>(host.Services);
MonDataLayer monDataLayer = new(credentials, loggingHelper);
TestingDataLayer testDataLayer = new(credentials, loggingHelper);

// Establish the parameter checker, which first checks if the program's 
// arguments can be parsed and, if they can, then checks if they are valid.
// If both tests are passed the object returned includes both the
// original arguments and the 'source' object or ob jects being imported.

ParameterChecker paramChecker = new(monDataLayer, testDataLayer, loggingHelper);
ParamsCheckResult paramsCheck = paramChecker.CheckParams(args);
if (paramsCheck.ParseError || paramsCheck.ValidityError)
{
    // End program, parameter errors should have been logged
    // in a 'no source' file by the ParameterChecker class.

    return -1;
}
else
{
    // Should be able to proceed - (opts and srce are known to be non-null).
    // Open log file, create Harvester class and call the main harvest function

    try
    {
        var opts = paramsCheck.Pars!;
        if (opts.UsingTestData is not true && opts.CreateTestReport is not true)
        {
            Importer importer = new(monDataLayer, loggingHelper);
            importer.Run(opts);
        }
        else
        {
            TestImporter testImporter = new(monDataLayer, testDataLayer, loggingHelper);
            testImporter.Run(opts);
        }
        return 0;
    }
    catch (Exception e)
    {
        // If an error bubbles up to here there is an issue with the code.

        loggingHelper.LogHeader("UNHANDLED EXCEPTION");
        loggingHelper.LogCodeError("MDR_Harvester application aborted", e.Message, e.StackTrace);
        loggingHelper.CloseLog();
        return -1;
    }
}

