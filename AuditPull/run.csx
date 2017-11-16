#r "O365ETL.dll"

using System;


public static void Run(TimerInfo myTimer, TraceWriter log)
{
    string connstring =  System.Configuration.ConfigurationManager.ConnectionStrings["AuditDb"].ConnectionString;
    string schema = System.Configuration.ConfigurationManager.ConnectionStrings["Schema"].ConnectionString;
    string clientSecret = System.Configuration.ConfigurationManager.ConnectionStrings["ClientSecret"].ConnectionString;
    string tenant = System.Configuration.ConfigurationManager.ConnectionStrings["Tenant"].ConnectionString;
    string clientId = System.Configuration.ConfigurationManager.ConnectionStrings["ClientId"].ConnectionString;
	int daysToRetrieve;
	daysToRetrieve = 7;
	
    for (int i = 0; i < daysToRetrieve; i++)
	{
		DateTime dateToProcess = DateTime.UtcNow.AddDays(-1*i);
		try
		{
			
			var result =
				O365ETL.GetOfficeData.Process(clientId, clientSecret, tenant, dateToProcess, connstring, schema).Result;

		}
		catch (Exception ex)
		{
			throw(ex)
		}
	}
	
	O365ETL.SQLOperations.CreateSP(schema);
	O365ETL.SQLOperations.RunStoredProc(connstring, schema + ".uspMoveStaging");
}
