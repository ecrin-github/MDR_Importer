﻿
namespace MDR_Importer;

public interface ILoggingHelper
{
    string LogFilePath { get; }    
    
    void LogLine(string message, string identifier = "");    
    void LogHeader(string header_text);
    void LogStudyHeader(Options opts, string dbLine);   
    
    void LogError(string message);
    void LogCodeError(string header, string errorMessage, string? stackTrace);
    void LogParseError(string header, string errorNum, string errorType);   
    
    void LogCommandLineParameters(Options opts);
    
    void OpenLogFile(string database_name);
    void OpenNoSourceLogFile();    

    void CloseLog();   
    
    void LogTableStatistics(ISource s, string schema);
    void LogDiffs(ISource s);
    
    string GetTableRecordCount(string db_conn, string schema, string table_name);
}

