#r "AuditLog.dll"

using System;
using System.Configuration;
using O365ETL;

public static void Run(TimerInfo pbiTimer, TraceWriter log)
{
	ConsoleWriter.Writer = log;
    ConsoleWriter.IsSilent = false;

    string productKey = ConfigurationManager.ConnectionStrings["ProductKey"]?.ConnectionString;
    string clientId = ConfigurationManager.ConnectionStrings["ClientId"]?.ConnectionString;
    string clientSecret = ConfigurationManager.ConnectionStrings["ClientSecret"]?.ConnectionString;
    string tenantId = ConfigurationManager.ConnectionStrings["Tenant"]?.ConnectionString;
    string sqlConnectionString = ConfigurationManager.ConnectionStrings["AuditDb"]?.ConnectionString;
    string schema = ConfigurationManager.ConnectionStrings["Schema"]?.ConnectionString;
    int daysToRetrieve = int.Parse(ConfigurationManager.ConnectionStrings["DaysToRetrieve"]?.ConnectionString ?? "2");
	
    var start = DateTime.Now;

    for (int i = daysToRetrieve - 1; i >= 0; i--)
    {
        DateTime currentProcessDate = DateTime.UtcNow.AddDays(-1 * i);
        DateTime dateToProcess = new DateTime(currentProcessDate.Year, currentProcessDate.Month, currentProcessDate.Day, 0, 0, 0);

        try
        {
            log.Info($"Processing: {dateToProcess}");
            bool result = Processor.Process(clientId, clientSecret, tenantId, dateToProcess, sqlConnectionString, schema, productKey).Result;
        }
        catch (Exception ex)
        {
            log.Info(ex.Message);
            throw (ex);
        }
        finally
        {
            Processor.Commit();
            log.Info($"Processing: {dateToProcess} completed.");
        }        
    }
}
