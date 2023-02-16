using Microsoft.Extensions.Configuration;
namespace MDR_Importer;

public class TestReportWriter
{
    private readonly string _logfileStartOfPath;
    private string? _logfile_path;
    private StreamWriter? _sw;
    
    public TestReportWriter()
    {
        IConfigurationRoot settings = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();

        _logfileStartOfPath = settings["logFilePath"] ?? "";
    }
  
    public void OpenLogFile()
    {
        string dtString = DateTime.Now.ToString("s", 
                           System.Globalization.CultureInfo.InvariantCulture)
                          .Replace(":", "").Replace("T", " ");
        
        _logfile_path = _logfileStartOfPath + "IMPORT TEST REPORT";
        _logfile_path += " " + dtString + ".log";
        _sw = new StreamWriter(_logfile_path, false, System.Text.Encoding.UTF8);

    }

    public void LogLine(string message, string identifier = "")
    {
        string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string feedback = dt_string + message + identifier;
        Transmit(feedback);
    }


    public void LogSimpleLine(string message, string identifier = "")
    {
        string feedback = message + identifier;
        Transmit(feedback);
    }


    public void BlankLine()
    {
        Transmit("");
    }

    public void LogHeader(string message)
    {
        string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string header = dt_string + "**** " + message + " ****";
        Transmit("");
        Transmit(header);
    }

    public void LogError(string message, string identifier = "")
    {
        string dt_string = DateTime.Now.ToShortDateString() + " : " + DateTime.Now.ToShortTimeString() + " :   ";
        string error_message = dt_string + "***ERROR*** " + message;
        Transmit("");
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit(error_message);
        Transmit("+++++++++++++++++++++++++++++++++++++++");
        Transmit("");
    }

    public void CloseLog()
    {
        LogHeader("Closing Log");
        _sw!.Flush();
        _sw!.Close();
    }


    private void Transmit(string message)
    {
        _sw!.WriteLine(message);
        Console.WriteLine(message);
    }

    
}