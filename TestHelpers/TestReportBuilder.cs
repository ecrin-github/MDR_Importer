using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Microsoft.Extensions.Configuration;
using Npgsql;

namespace MDR_Importer;

class TestReportBuilder
{
    private readonly string _dbConn;
    private string? _sqlString;
    private int _totalStudies;
    private int _totalObjects;

    readonly TestReportWriter _reporter;

    public TestReportBuilder(string dbConn)
    {
        _dbConn = dbConn;

        // set up report file
        _reporter = new TestReportWriter();

        IConfigurationRoot settings = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json")
            .Build();
        string logfileStartOfPath = settings["logfilepath"]!;

        _reporter.OpenLogFile(logfileStartOfPath!);
    }


    public int GetCount(string sqlString)
    {
        int res = 0;
        using (var conn = new NpgsqlConnection(_dbConn))
        {
            res = conn.ExecuteScalar<int>(sqlString);
        }
        return res;
    }



    public void ExecuteSql(string sqlString)
    {
        using var conn = new NpgsqlConnection(_dbConn);
        conn.Execute(sqlString);
    }


    public bool CompareStudyRecordCounts()
    {
        _reporter.LogHeader("Study record counts");

        _sqlString = "select count(*) from expected.studies;";
        int e = GetCount(_sqlString);
        _reporter.LogLine("Number of expected studies: " + e.ToString());

        _sqlString = "select count(*) from adcomp.studies;";
        int a = GetCount(_sqlString);
        _reporter.LogLine("Number of actual studies: " + a.ToString());

        _sqlString = @"select count(*) from 
                     expected.studies e
                     inner join adcomp.studies a
                     on e.sd_sid = a.sd_sid;";
        int m = GetCount(_sqlString);
        _reporter.LogLine("Number of matched study ids: " + m.ToString());

        if (a != e)
        {
            _reporter.LogError("Number of actual and expected studies do not match");
            _reporter.LogError("Cannot proceed with testing studies until this error resolved");
            return false;
        }
        else if (a != m)
        {
            _reporter.LogError("Number of matched study IDs not equal to total number");
            _reporter.LogError("Cannot proceed with testing studies until this error resolved");
            return false;
        }
        else
        {
            _reporter.LogLine("All OK!");
            _reporter.LogLine("");
            _totalStudies = m;
            return true;
        }
    }

    public void CompareStudyRecords()
    {
        CompareFields("studies", "display_title", "string");
        CompareFields("studies", "brief_description", "string");
        CompareFields("studies", "data_sharing_statement", "string");
        CompareFields("studies", "study_start_year", "int");
        CompareFields("studies", "study_start_month", "int");
        CompareFields("studies", "study_type_id", "int");
        CompareFields("studies", "study_status_id", "int");
        CompareFields("studies", "study_enrolment", "string");
        CompareFields("studies", "study_gender_elig_id", "int");
        CompareFields("studies", "min_age", "int");
        CompareFields("studies", "min_age_units_id", "int");
        CompareFields("studies", "max_age", "int");
        CompareFields("studies", "max_age_units_id", "int");
        CompareFields("studies", "record_hash", "string");
    }


    public void CompareStudyAttributes()
    {
        // Compares numbers of attribute records of each type

        CompareAttributeNumbers("studies", "data_objects");

        CompareAttributeNumbers("studies", "study_identifiers");
        CompareAttributeNumbers("studies", "study_titles");
        CompareAttributeNumbers("studies", "study_contributors");
        CompareAttributeNumbers("studies", "study_topics");
        CompareAttributeNumbers("studies", "study_features");
        CompareAttributeNumbers("studies", "study_ipd_available");
        CompareAttributeNumbers("studies", "study_references");
        CompareAttributeNumbers("studies", "study_links");
        CompareAttributeNumbers("studies", "study_countries");
        CompareAttributeNumbers("studies", "study_locationss");
        CompareAttributeNumbers("studies", "study_relationships");
    }


    public void CompareStudyHashes()
    {
        CompareHashes("studies", "study_identifiers", 11);
        CompareHashes("studies", "study_titles", 12);
        CompareHashes("studies", "study_contributors", 15);
        CompareHashes("studies", "study_topics", 14);
        CompareHashes("studies", "study_features", 13);
        CompareHashes("studies", "study_ipd_available", 19);
        CompareHashes("studies", "study_references", 17);
        CompareHashes("studies", "study_links", 18);
        CompareHashes("studies", "study_countries", 21);
        CompareHashes("studies", "study_locations", 20);
        CompareHashes("studies", "study_relartionships", 16);
    }


    public bool CompareObjectRecordCounts()
    {
        _reporter.LogHeader("Object record counts");

        _sqlString = "select count(*) from expected.data_objects;";
        int e = GetCount(_sqlString);
        _reporter.LogLine("Number of expected data objects: " + e.ToString());

        _sqlString = "select count(*) from adcomp.data_objects;";
        int a = GetCount(_sqlString);
        _reporter.LogLine("Number of actual data objects: " + a.ToString());

        _sqlString = @"select count(*) from 
                     expected.data_objects e
                     inner join adcomp.data_objects a
                     on e.sd_oid = a.sd_oid;";
        int m = GetCount(_sqlString);
        _reporter.LogLine("Number of matched data objects: " + m.ToString());

        if (a != e)
        {
            _reporter.LogError("Number of actual and expected data objects do not match");
            _reporter.LogError("Cannot proceed with testing studies until this error resolved");
            return false;
        }
        else if (a != m)
        {
            _reporter.LogError("Number of matched data object IDs not equal to total number");
            _reporter.LogError("Cannot proceed with testing studies until this error resolved");
            return false;
        }
        else
        {
            _reporter.LogLine("All OK!");
            _reporter.LogLine("");
            _totalObjects = m;
            return true;
        }
    }

    public void CompareObjectRecords()
    {
        CompareFields("data_objects", "display_title", "string");
        CompareFields("data_objects", "version", "string");
        CompareFields("data_objects", "doi", "string");
        CompareFields("data_objects", "doi_status_id", "int");
        CompareFields("data_objects", "publication_year", "int");
        CompareFields("data_objects", "object_class_id", "int");
        CompareFields("data_objects", "object_type_id", "int");
        CompareFields("data_objects", "managing_org_id", "int");
        CompareFields("data_objects", "managing_org", "string");
        CompareFields("data_objects", "managing_org_ror_id", "string");
        CompareFields("data_objects", "lang_code", "string");
        CompareFields("data_objects", "access_type_id", "int");
        CompareFields("data_objects", "access_details", "string");
        CompareFields("data_objects", "access_details_url", "string");
        CompareFields("data_objects", "url_last_checked", "date");
        CompareFields("data_objects", "eosc_category", "int");
        CompareFields("data_objects", "add_study_contribs", "boolean");
        CompareFields("data_objects", "add_study_topics", "boolean");
        CompareFields("data_objects", "record_hash", "string");
    }


    public void CompareObjectAttributes()
    {
        // Compares numbers of attribute records of each type

        CompareAttributeNumbers("data_objects", "object_datasets");
        CompareAttributeNumbers("data_objects", "object_instances");
        CompareAttributeNumbers("data_objects", "object_titles");
        CompareAttributeNumbers("data_objects", "object_dates");
        CompareAttributeNumbers("data_objects", "object_topics");
        CompareAttributeNumbers("data_objects", "object_contributors");
        CompareAttributeNumbers("data_objects", "object_relationships");
        CompareAttributeNumbers("data_objects", "object_descriptions");
        CompareAttributeNumbers("data_objects", "object_rights");
        CompareAttributeNumbers("data_objects", "object_db_links");
        CompareAttributeNumbers("data_objects", "object_comments");
        CompareAttributeNumbers("data_objects", "object_publication_types");
        CompareAttributeNumbers("data_objects", "object_identifiers");
    }


    public void CompareObjectHashes()
    {
        CompareHashes("data_objects", "object_datasets", 50);
        CompareHashes("data_objects", "object_instances", 51);
        CompareHashes("data_objects", "object_titles", 52);
        CompareHashes("data_objects", "object_dates", 53);
        CompareHashes("data_objects", "object_topics", 54);
        CompareHashes("data_objects", "object_contributors", 55);
        CompareHashes("data_objects", "object_relationships", 56);
        CompareHashes("data_objects", "object_descriptions", 57);
        CompareHashes("data_objects", "object_rights", 59);
        CompareHashes("data_objects", "object_db_links", 60);
        CompareHashes("data_objects", "object_comments", 61);
        CompareHashes("data_objects", "object_publication_types", 62);
        CompareHashes("data_objects", "object_identifiers", 63);
    }

    public void CompareFullHashes()
    {
        CompareFields("studies", "study_full_hash", "string"); 
        CompareFields("data_objects", "object_full_hash", "string");
    }

    private void DoComparisonHeader(string table, string field)
    {
        _reporter.LogHeader("Comparing " + field + " field in " + table + " table");
    }

    private void DoAttributeComparisonHeader(string table)
    {
        _reporter.LogHeader("Comparing Attribute Numbers in " + table);
    }

    private void DoHashComparisonHeader(string table)
    {
        _reporter.LogHeader("Comparing Hash values for " + table);
    }

    private void DoComparisonFooter()
    {
        _reporter.BlankLine();
    }


    private void CompareFields(string table, string field, string fieldtype)
    {
        int total = table == "studies" ? _totalStudies : _totalObjects;

        DoComparisonHeader(table, field);
        int m = GetMatchingIds(table, field);
        int n = GetNonMatchingIds(table, field);
        int en = GetNullsInExpectedIds(table, field);
        int an = GetNullsInActualIds(table, field);
        int b = GetBothNulls(table, field);

        if (n == 0 && en == 0 && an == 0 && (m + b) == total)
        {
            _reporter.LogLine("All OK!");
        }
        else
        {
            if (n > 0)
            {
                _reporter.BlankLine();
                string list_header = (n == 1) ? "Unmatched record:" : "Unmatched records:";
                _reporter.LogSimpleLine(list_header);
                _reporter.BlankLine();
                WriteOutNonMatchingValues(table, field, fieldtype, "n");
            }

            if (en > 0)
            {
                _reporter.BlankLine();
                string list_header = "Null in expected ";
                list_header += (n == 1) ? "record:" : "records:";
                _reporter.LogSimpleLine(list_header);
                _reporter.BlankLine();
                WriteOutNonMatchingValues(table, field, fieldtype, "en");
            }

            if (an > 0)
            {
                _reporter.BlankLine();
                string list_header = "Null in actual ";
                list_header += (n == 1) ? "record:" : "records:";
                _reporter.LogSimpleLine(list_header);
                _reporter.BlankLine();
                WriteOutNonMatchingValues(table, field, fieldtype, "an");
            }
        }

        DoComparisonFooter();

    }

    private int GetMatchingIds(string table, string field)
    {
        string sdid = table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*)
        from expected." + table + @" e
        inner join adcomp." + table + @" a
        on e." + sdid + @" = a." + sdid + @"
        where e." + field + " = a." + field + ";";

        int m = GetCount(_sqlString);
        _reporter.LogLine("Number of matched field values: " + m.ToString());
        return m;
    }

    private int GetNonMatchingIds(string table, string field)
    {
        string sdid = table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*)
        from expected." + table + @" e
        inner join adcomp." + table + @" a
        on e." + sdid + @" = a." + sdid + @"
        where e." + field + " <> a." + field + ";";

        int n = GetCount(_sqlString);
        _reporter.LogLine("Number of unmatched field values: " + n.ToString());
        return n;
    }

    private int GetBothNulls(string table, string field)
    {
        string sdid = table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*)
        from expected." + table + @" e
        inner join adcomp." + table + @" a
        on e." + sdid + @" = a." + sdid + @"
        where e." + field + @" is null
        and a. " + field + " is null;";

        int b = GetCount(_sqlString);
        _reporter.LogLine("Number of records where both records null: " + b.ToString());

        return b;
    }


    private int GetNullsInExpectedIds(string table, string field)
    {
        string sdid = table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*)
        from expected." + table + @" e
        inner join adcomp." + table + @" a
        on e." + sdid + @" = a." + sdid + @"
        where e." + field + @" is null
        and a." + field + " is not null;";

        int en = GetCount(_sqlString);
        _reporter.LogLine("Number of unmatched nulls in expected: " + en.ToString());
        return en;
    }

    private int GetNullsInActualIds(string table, string field)
    {
        string sdid = table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*)
        from expected." + table + @" e
        inner join adcomp." + table + @" a
        on e." + sdid + @" = a." + sdid + @"
        where a." + field + @" is null
        and e." + field + " is not null;";

        int an = GetCount(_sqlString);
        _reporter.LogLine("Number of unmatched nulls in actual: " + an.ToString());
        return an;
    }


    private void CompareAttributeNumbers(string main_table, string table)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";

        DoAttributeComparisonHeader(table);
        int m = GetMatchingAttributeNumbers(main_table, table);

        int n = GetNonMatchingAttributeNumbers(main_table, table);
        if (n > 0)
        {
            _reporter.BlankLine();
            _reporter.LogSimpleLine("Non matching details");
            _reporter.BlankLine();
            WriteOutAttributesWithNonMatchingValues(main_table, table);
        }

        int e = GetExpectedOnlyAttributeNumbers(main_table, table);
        if (e > 0)
        {
            _reporter.BlankLine();
            string list_header = (e == 1) ? "Entity " : "Entities ";
            list_header += "with attributes in expected data only:";
            _reporter.LogSimpleLine("Non matching details");
            _reporter.BlankLine();
            WriteOutAttributesinExpectedDataOnly(main_table, table);
        }

        int a = GetActualOnlyAttributeNumbers(main_table, table);
        if (a > 0)
        {
            _reporter.BlankLine();
            string list_header = (a == 1) ? "Entity " : "Entities ";
            list_header += "with attributes in actual data only:";
            _reporter.LogSimpleLine("Non matching details");
            _reporter.BlankLine();
            WriteOutAttributesinActualDataOnly(main_table, table);
        }
        int b = GetNumbersWhereNoAttributes(main_table, table);

        if (n == 0 && a == 0 && e == 0)
        {
            _reporter.LogLine("All OK!");
        }

        DoComparisonFooter();
    }


    private int GetMatchingAttributeNumbers(string main_table, string table)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*) from
                (select " + sdid + @", count(id) as num from expected." + table + @"
                 where " + sdid + @" is not null
                 group by " + sdid + @") e
                inner join
                (select " + sdid + @", count(id) as num from adcomp." + table + @"
                 where " + sdid + @" is not null
                 group by " + sdid + @") a
                on e." + sdid + @" = a." + sdid + @"
                where e.num = a.num;";

        int m = GetCount(_sqlString);
        _reporter.LogLine("Number of matching attribute numbers: " + m.ToString());
        return m;
    }

    private int GetNonMatchingAttributeNumbers(string main_table, string table)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*) from
                (select " + sdid + @", count(id) as num from expected." + table + @"
                where " + sdid + @" is not null
                group by " + sdid + @") e
                inner join
                (select " + sdid + @", count(id) as num from adcomp." + table + @"
                where " + sdid + @" is not null
                group by " + sdid + @") a
                on e." + sdid + @" = a." + sdid + @"
                where e.num <> a.num;";

        int n = GetCount(_sqlString);
        _reporter.LogLine("Number of non-matching attribute numbers: " + n.ToString());
        return n;
    }

    private int GetExpectedOnlyAttributeNumbers(string main_table, string table)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*) from
                (select " + sdid + @", count(id) as num from expected." + table + @"
                 where " + sdid + @" is not null
                 group by " + sdid + @") e
                left join
                (select " + sdid + @", count(id) as num from adcomp." + table + @"
                 where " + sdid + @" is not null
                 group by " + sdid + @") a
                on e." + sdid + @" = a." + sdid + @"
                where a." + sdid + @" is null;";

        int e = GetCount(_sqlString);
        _reporter.LogLine("Numbers with attributes only in expected data: " + e.ToString());
        return e;
    }

    private int GetActualOnlyAttributeNumbers(string main_table, string table)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*) from 
                (select " + sdid + @", count(id) as num from expected." + table + @"
                 where " + sdid + @" is not null
                 group by " + sdid + @") e
                right join
                (select " + sdid + @", count(id) as num from adcomp." + table + @"
                 where " + sdid + @" is not null
                 group by " + sdid + @") a
                on e." + sdid + @" = a." + sdid + @"
                where e." + sdid + @" is null;";

        int a = GetCount(_sqlString);
        _reporter.LogLine("Numbers with attributes only in actual data: " + a.ToString());
        return a;
    }

    private int GetNumbersWhereNoAttributes(string main_table, string table)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";

        _sqlString = @"select count(*) from 
        (select am." + sdid + @", count(t.id) as anum 
         from adcomp." + main_table + @" am left join adcomp." + table + @" t 
         on am." + sdid + @" = t." + sdid + @"
         where am." + sdid + @" is not null
	     group by am." + sdid + @") a
         inner join
         (select em." + sdid + @", count(et.id) as enum 
         from expected." + main_table + @" em left join expected." + table + @" et 
         on em." + sdid + @" = et." + sdid + @"
         where em." + sdid + @" is not null
	     group by em." + sdid + @") e
         on a." + sdid + @" = e." + sdid + @"
         where anum = 0 and enum = 0;";

        int a = GetCount(_sqlString);
        _reporter.LogLine("Numbers with no attributes of this type in expected and actual: " + a.ToString());
        return a;
    }

    private void WriteOutAttributesWithNonMatchingValues(string main_table, string table)
    {
        if (main_table == "studies")
        {
            _sqlString = @"select a.sd_sid as id, 
            e.num as expected, a.num as actual from
            (select sd_sid, count(id) as num from expected." + table + @"
            group by sd_sid) e
            inner join
            (select sd_sid, count(id) as num from adcomp." + table + @"
            group by sd_sid) a
            on e.sd_sid = a.sd_sid
            where e.num <> a.num;";
        }
        else
        {
            _sqlString = @"select b.sd_sid as id, a.sd_oid as oid, 
            e.num as expected, a.num as actual from
            (select sd_oid, count(id) as num from expected." + table + @"
            group by sd_oid) e
            inner join
            (select sd_oid, count(id) as num from adcomp." + table + @"
            group by sd_oid) a
            on e.sd_oid = a.sd_oid
            inner join expected.data_objects b
            on a.sd_oid = b.sd_oid
            where e.num <> a.num;";
        }

        WriteOutNonMatchingAttributes(_sqlString, main_table);
    }

    private void WriteOutAttributesinExpectedDataOnly(string main_table, string table)
    {
        if (main_table == "studies")
        {
            _sqlString = @"select e.sd_sid as id, 
                e.num as number from
                (select sd_sid, count(id) as num from expected." + table + @"
                group by sd_sid) e
                left join
                (select sd_sid, count(id) as num from adcomp." + table + @"
                group by sd_sid) a
                on e.sd_sid = a.sd_sid
                where a.sd_sid is null;";
        }
        else
        {
            _sqlString = _sqlString = @"select b.sd_sid as id, e.sd_oid as oid, 
                e.num as number from
                (select sd_oid, count(id) as num from expected." + table + @"
                group by sd_oid) e
                left join
                (select sd_oid, count(id) as num from adcomp." + table + @"
                group by sd_oid) a
                on e.sd_oid = a.sd_oid
                inner join adcomp.data_objects b
                on e.sd_oid = b.sd_oid
                where a.sd_oid is null;";

        }

        WriteOutAttributesOnlyInOneDataset(_sqlString, main_table);
    }

    private void WriteOutAttributesinActualDataOnly(string main_table, string table)
    {
        if (main_table == "studies")
        {
            _sqlString = @"select a.sd_sid as id, 
            a.num as number from
            (select sd_sid, count(id) as num from expected." + table + @"
            group by sd_sid) e
            right join
            (select sd_sid, count(id) as num from adcomp." + table + @"
            group by sd_sid) a
            on e.sd_sid = a.sd_sid
            where e.sd_sid is null;";
        }
        else
        {
            _sqlString = _sqlString = @"select b.sd_sid as id, a.sd_oid as oid, 
            a.num as number from
            (select sd_oid, count(id) as num from expected." + table + @"
            group by sd_oid) e
            right join
            (select sd_oid, count(id) as num from adcomp." + table + @"
            group by sd_oid) a
            on e.sd_oid = a.sd_oid
            inner join expected.data_objects b
            on a.sd_oid = b.sd_oid
            where e.sd_oid is null;";
        }

        WriteOutAttributesOnlyInOneDataset(_sqlString, main_table);
    }


    private void CompareHashes(string main_table, string table, int hashtype)
    {
        DoHashComparisonHeader(table);

        int m = GetMatchingHashes(main_table, hashtype);
        int n = GetNonMatchingHashes(main_table, hashtype);
        if (n > 0)
        {
            _reporter.BlankLine();
            _reporter.LogSimpleLine("Non matching details");
            _reporter.BlankLine();
            WriteOutHashesWithNonMatchingValues(main_table, hashtype);
        }

        int e = GetExpectedOnlyHashes(main_table, hashtype);
        if (e > 0)
        {
            _reporter.BlankLine();
            string list_header = (e == 1) ? "Entity " : "Entities ";
            list_header += "with hashes in expected data only:";
            _reporter.LogSimpleLine("Non matching details");
            _reporter.BlankLine();
            WriteOutHashesinExpectedDataOnly(main_table, hashtype);
        }

        int a = GetActualOnlyHashes(main_table, hashtype);
        if (a > 0)
        {
            _reporter.BlankLine();
            string list_header = (a == 1) ? "Entity " : "Entities ";
            list_header += "with hashes in actual data only:";
            _reporter.LogSimpleLine("Non matching details");
            _reporter.BlankLine();
            WriteOutHashesinActualDataOnly(main_table, hashtype);
        }

        int b = GetNumbersWhereNoHashes(main_table, hashtype);
        if (n == 0 && a == 0 && e == 0)
        {
            _reporter.LogLine("All OK!");
        }
        DoComparisonFooter();

    }

    private int GetMatchingHashes(string main_table, int hashtype)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";
        string hash_table = main_table == "studies" ? "study_hashes" : "object_hashes";

        _sqlString = @"select count(*) from
                (select " + sdid + @", composite_hash from expected." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") e
                inner join
                (select " + sdid + @", composite_hash from adcomp." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") a
                on e." + sdid + @" = a." + sdid + @"
                where e.composite_hash = a.composite_hash;";

        int m = GetCount(_sqlString);
        _reporter.LogLine("Number of matching composite hashes: " + m.ToString());
        return m;
    }

    private int GetNonMatchingHashes(string main_table, int hashtype)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";
        string hash_table = main_table == "studies" ? "study_hashes" : "object_hashes";

        _sqlString = @"select count(*) from
                (select " + sdid + @", composite_hash from expected." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") e
                inner join
                (select " + sdid + @", composite_hash from adcomp." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") a
                on e." + sdid + @" = a." + sdid + @"
                where e.composite_hash <> a.composite_hash;";

        int n = GetCount(_sqlString);
        _reporter.LogLine("Number of non-matching composite hashes: " + n.ToString());
        return n;
    }

    private int GetExpectedOnlyHashes(string main_table, int hashtype)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";
        string hash_table = main_table == "studies" ? "study_hashes" : "object_hashes";

        _sqlString = @"select count(*) from
                (select " + sdid + @", composite_hash from expected." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") e
                left join
                (select " + sdid + @", composite_hash from adcomp." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") a
                on e." + sdid + @" = a." + sdid + @"
                where a." + sdid + @" is null;";

        int e = GetCount(_sqlString);
        _reporter.LogLine("Numbers of composite hashes only in expected data: " + e.ToString());
        return e;
    }


    private int GetActualOnlyHashes(string main_table, int hashtype)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";
        string hash_table = main_table == "studies" ? "study_hashes" : "object_hashes";

        _sqlString = @"select count(*) from 
                (select " + sdid + @", composite_hash from expected." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") e
                right join
                (select " + sdid + @", composite_hash from adcomp." + hash_table + @"
                where hash_type_id = " + hashtype.ToString() + @") a
                on e." + sdid + @" = a." + sdid + @"
                where e." + sdid + @" is null;";

        int a = GetCount(_sqlString);
        _reporter.LogLine("Numbers of composite hashes only in actual data: " + a.ToString());
        return a;
    }

    private int GetNumbersWhereNoHashes(string main_table, int hashtype)
    {
        string sdid = main_table == "studies" ? "sd_sid" : "sd_oid";
        string hash_table = main_table == "studies" ? "study_hashes" : "object_hashes";

        _sqlString = @"select count(*) 
        from 
            (select am." + sdid + @"
             from adcomp." + main_table + @" am 
             left join
                 (select " + sdid + @"
                 from adcomp." + hash_table + @"
                 where hash_type_id = " + hashtype.ToString() + @") ah
             on am." + sdid + @" = ah." + sdid + @"
             where ah." + sdid + @" is null) a
         inner join
             (select em." + sdid + @"
             from expected." + main_table + @" em 
             left join
                 (select " + sdid + @"
                 from expected." + hash_table + @"
                 where hash_type_id = " + hashtype.ToString() + @") eh
             on em." + sdid + @" = eh." + sdid + @"
             where eh." + sdid + @" is null) e
         on a." + sdid + @" = e." + sdid;

        int a = GetCount(_sqlString);
        _reporter.LogLine("Numbers with no composite hashes of this type in expected and actual: " + a.ToString());
        return a;
    }


    private void WriteOutHashesWithNonMatchingValues(string main_table, int hashtype)
    {
        if (main_table == "studies")
        {
            _sqlString = @"select a.sd_sid as id, e.composite_hash as expected, 
                a.composite_hash as actual from
                (select sd_sid, composite_hash from expected.study_hashes
                where hash_type_id = " + hashtype.ToString() + @") e
                inner join
                (select sd_sid, composite_hash from adcomp.study_hashes
                where hash_type_id = " + hashtype.ToString() + @") a
                on e.sd_sid = a.sd_sid
                where e.composite_hash <> a.composite_hash;";
        }
        else
        {
            _sqlString = @"select b.sd_sid as id, a.sd_oid as oid, e.composite_hash as expected, 
                a.composite_hash as actual from
                (select sd_oid, composite_hash from expected.object_hashes
                where hash_type_id = " + hashtype.ToString() + @") e
                inner join
                (select sd_oid, composite_hash from adcomp.object_hashes
                where hash_type_id = " + hashtype.ToString() + @") a
                on e.sd_oid = a.sd_oid
                inner join adcomp.data_objects b
                on a.sd_oid = b.sd_oid
                where e.composite_hash <> a.composite_hash;";
        }
        WriteOutNonMatchingHashes(_sqlString, main_table);
    }


    private void WriteOutHashesinExpectedDataOnly(string main_table, int hashtype)
    {
        if (main_table == "studies")
        {
            _sqlString = @"select e.sd_sid as id, e.composite_hash as hash from
                (select sd_sid, composite_hash from expected.study_hashes
                where hash_type_id = " + hashtype.ToString() + @") e
                left join
                (select sd_sid, composite_hash from adcomp.study_hashes
                where hash_type_id = " + hashtype.ToString() + @") a
                on e.sd_sid = a.sd_sid
                where a.sd_sid is null;";
        }
        else
        {
            _sqlString = @"select b.sd_sid as id, e.sd_oid as oid, e.composite_hash as hash from
                (select sd_oid, composite_hash from expected.object_hashes
                where hash_type_id = " + hashtype.ToString() + @") e
                left join
                (select sd_oid, composite_hash from adcomp.object_hashes
                where hash_type_id = " + hashtype.ToString() + @") a
                on e.sd_oid = a.sd_oid
                inner join expected.data_objects b
                on e.sd_oid = b.sd_oid
                where a.sd_oid is null;";
        }
        WriteOutHashesOnlyInOneDataset(_sqlString, main_table);
    }

    private void WriteOutHashesinActualDataOnly(string main_table, int hashtype)
    {
        if (main_table == "studies")
        {
            _sqlString = @"select a.sd_sid as id, a.composite_hash as hash from
                    (select sd_sid, composite_hash from expected.study_hashes
                    where hash_type_id = " + hashtype.ToString() + @") e
                    right join
                    (select sd_sid, composite_hash from adcomp.study_hashes
                    where hash_type_id = " + hashtype.ToString() + @") a
                    on e.sd_sid = a.sd_sid
                    where e.sd_sid is null;";
        }
        else
        {
            _sqlString = @"select b.sd_sid as id, a.sd_oid as oid, a.composite_hash as hash from
                    (select sd_oid, composite_hash from expected.object_hashes
                    where hash_type_id = " + hashtype.ToString() + @") e
                    right join
                    (select sd_oid, composite_hash from adcomp.object_hashes
                    where hash_type_id = " + hashtype.ToString() + @") a
                    on e.sd_oid = a.sd_oid
                    inner join adcomp.data_objects b
                    on a.sd_oid = b.sd_oid
                    where e.sd_oid is null;";
        }

        WriteOutHashesOnlyInOneDataset(_sqlString, main_table);
    }


    public void Close()
    {
        _reporter.CloseLog();
    }


    private class DBValue
    { 
        public string? id { get; set; }
        public string? expected { get; set; }
        public string? actual { get; set; }
    }

    private class DBObjValue
    {
        public string? id { get; set; }
        public string? oid { get; set; }
        public string? expected { get; set; }
        public string? actual { get; set; }
    }

    
    private class AttNum
    {
        public string? id { get; set; }
        public int? expected { get; set; }
        public int? actual { get; set; }
    }

    private class SingleAttNum
    {
        public string? id { get; set; }
        public int? number { get; set; }
    }

    private class HashValue
    {
        public string? id { get; set; }
        public string? expected { get; set; }
        public string? actual { get; set; }
    }

    private class SingleHashValue
    {
        public string? id { get; set; }
        public string? hash { get; set; }
    }

    private class ObjAttNum
    {
        public string? id { get; set; }
        public string? oid { get; set; }
        public int? expected { get; set; }
        public int? actual { get; set; }
    }

    private class ObjSingleAttNum
    {
        public string? id { get; set; }
        public string? oid { get; set; }
        public int? number { get; set; }
    }

    private class ObjHashValue
    {
        public string? id { get; set; }
        public string? oid { get; set; }
        public string? expected { get; set; }
        public string? actual { get; set; }
    }

    private class ObjSingleHashValue
    {
        public string? id { get; set; }
        public string? oid { get; set; }
        public string? hash { get; set; }
    }


    private string ObtainSQLForNonMatchingValues(string table, string field, string fieldtype, string comptype)
    {
        string sdid = table == "studies" ? "sd_sid" : "sd_oid";

        string sql_string = table == "studies"
                              ? "select a.sd_sid as id, "
                              : "select a.sd_sid as id, a.sd_oid as oid,";

        if (fieldtype == "string")
        {
            sql_string += "e." + field + " as expected, a." + field + " as actual ";
        }
        else if (fieldtype == "int" || fieldtype == "date")
        {
            sql_string += "cast(e." + field + " as varchar) as expected, cast(a." + field + " as varchar) as actual ";
        }
        else if (fieldtype == "boolean")
        {
            sql_string += @" case
                              when e." + field + @" = true then 'true' 
                              when e." + field + @" = false then 'false' 
                              else 'null' end as expected, 
                              case
                              when a." + field + @" = true then 'true' 
                              when a." + field + @" = false then 'false' 
                              else 'null' end, as actual ";
        }

        sql_string += @" 
            from expected." + table + @" e
            inner join adcomp." + table + @" a
            on e." + sdid + @" = a." + sdid;

        if (comptype == "n")
        {
            sql_string += " where e." + field + " <> a." + field + "; ";
        }
        else if (comptype == "en")
        {
            sql_string += "  where e." + field + " IS DISTINCT FROM a." + field + " and e. " + field + " is null;";
        }
        else if (comptype == "an")
        {
            sql_string += "  where e." + field + " IS DISTINCT FROM a." + field + " and a. " + field + " is null;";
        }

        return sql_string;
    }


    private void WriteOutNonMatchingValues(string table, string field, string fieldtype, string comptype)
    {
        // should log out the 'offending' records
        // into a simple object (but object defn will depend on field type)

        _sqlString = ObtainSQLForNonMatchingValues(table, field, fieldtype, comptype);
        using (var conn = new NpgsqlConnection(_dbConn))
        {
            int res_count = 0;
            if (table == "studies")
            {
                List<DBValue> res = conn.Query<DBValue>(_sqlString).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (DBValue v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + ", \nexpected: " + v.expected
                                          + ", \nactual: " + v.actual + "\n");
                    }
                }
            }
            else
            {
                List<DBObjValue> res = conn.Query<DBObjValue>(_sqlString).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (DBObjValue v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + ", \noid: " + v.oid
                                          + ", \nexpected: " + v.expected
                                          + ", \nactual: " + v.actual + "\n");
                    }
                }
            }
            if (res_count == 0)
            {
                _reporter.LogSimpleLine("Odd - could not find the non-matching records in the DB");
            }
        }

    }

    private void WriteOutNonMatchingAttributes(string sql_string, string main_table)
    {
        using (var conn = new NpgsqlConnection(_dbConn))
        {
            int res_count = 0;
            if (main_table == "studies")
            {
                List<AttNum> res = conn.Query<AttNum>(sql_string).ToList();
                res_count = res.Count;
                if (res.Count > 0)
                {
                    foreach (AttNum v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + ", expected: " + v.expected
                                          + ", actual: " + v.actual + "\n");
                    }
                }
            }
            else
            {
                List<ObjAttNum> res = conn.Query<ObjAttNum>(sql_string).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (ObjAttNum v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + "\noid: " + v.oid + ", expected: " + v.expected
                                          + ", actual: " + v.actual + "\n");
                    }
                }
            }
            if (res_count == 0)
            {
                _reporter.LogSimpleLine("Odd - could not find the non-matching records in the DB");
            }
        }
    }

    private void WriteOutAttributesOnlyInOneDataset(string sql_string, string main_table)
    {
        using (var conn = new NpgsqlConnection(_dbConn))
        {
            int res_count = 0;
            if (main_table == "studies")
            {
                List<SingleAttNum> res = conn.Query<SingleAttNum>(sql_string).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (SingleAttNum v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + ", number: " + v.number + "\n");
                    }
                }
            }
            else
            {
                List<ObjSingleAttNum> res = conn.Query<ObjSingleAttNum>(sql_string).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (ObjSingleAttNum v in res)
                    { 
                        _reporter.LogSimpleLine("id: " + v.id + "\noid: " + v.oid + ", number: " + v.number + "\n");
                    }
                }
            }
            if (res_count == 0)
            {
                _reporter.LogSimpleLine("Odd - could not find the non-matching records in the DB");
            }
        }
    }


    private void WriteOutNonMatchingHashes(string sql_string, string main_table)
    {
        using (var conn = new NpgsqlConnection(_dbConn))
        {
            int res_count = 0;
            if (main_table == "studies")
            {
                List<HashValue> res = conn.Query<HashValue>(sql_string).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (HashValue v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + ", expected: " + v.expected
                                          + ", actual: " + v.actual + "\n");
                    }
                }
            }
            else
            {
                List<ObjHashValue> res = conn.Query<ObjHashValue>(sql_string).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (ObjHashValue v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + "\noid: " + v.oid + ", expected: " + v.expected
                                          + ", actual: " + v.actual + "\n");
                    }
                }
            }
            if (res_count == 0)
            {
                _reporter.LogSimpleLine("Odd - could not find the non-matching records in the DB");
            }
        }
    }

    private void WriteOutHashesOnlyInOneDataset(string sql_string, string main_table)
    {
        using (var conn = new NpgsqlConnection(_dbConn))
        {
            int res_count = 0;
            if (main_table == "studies")
            {
                List<SingleHashValue> res = conn.Query<SingleHashValue>(sql_string).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (SingleHashValue v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + ", number: " + v.hash + "\n");
                    }
                }
            }
            else
            { 
                List<ObjSingleHashValue> res = conn.Query<ObjSingleHashValue>(sql_string).ToList();
                res_count = res.Count;
                if (res_count > 0)
                {
                    foreach (ObjSingleHashValue v in res)
                    {
                        _reporter.LogSimpleLine("id: " + v.id + "\noid: " + v.oid + ", number: " + v.hash + "\n");
                    }
                }
            }
            if (res_count == 0)
            {
                _reporter.LogSimpleLine("Odd - could not find the non-matching records in the DB");
            }
        }
    }

}