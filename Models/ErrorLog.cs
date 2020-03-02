using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Dapper;
using System.Data;
using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.IO;

namespace CallDataReader
{
  public class ErrorLog
  {
    public int AppId { get; set; } = 20042;
    public string ApplicationName { get; set; } = "Call Data Reader";
    public string ErrorText { get; set; }
    public string ErrorMessage { get; set; }
    public string ErrorStacktrace { get; set; }
    public string ErrorSource { get; set; }
    public string Query { get; set; }

    public ErrorLog(string text,
      string message,
      string stacktrace,
      string source,
      string errorQuery,
      bool is_prod)
    {
      ErrorText = text;
      ErrorMessage = message;
      ErrorStacktrace = stacktrace;
      ErrorSource = source;
      Query = errorQuery;
      SaveLog(is_prod);
    }

    public ErrorLog(Exception ex, string errorQuery = "", bool is_prod = true)
    {
      ErrorText = ex.ToString();
      ErrorMessage = ex.Message;
      ErrorStacktrace = ex.StackTrace;
      ErrorSource = ex.Source;
      Query = errorQuery;
      SaveLog(is_prod);
    }

    private void SaveLog(bool is_prod)
    {
      string sql = @"
          INSERT INTO ErrorData 
          (applicationName, errorText, errorMessage, 
          errorStacktrace, errorSource, query)  
          VALUES (@applicationName, @errorText, @errorMessage,
            @errorStacktrace, @errorSource, @query);";

      string cs = ErrorLog.GetLogCS(is_prod);

      try
      {
        using (IDbConnection db = new SqlConnection(cs))
        {
          db.Execute(sql, this);
        }
      }
      catch
      {
        // we only want to attempt a second save if we just failed to save to production
        // this stops an infinite loop if there is no connectivity to production or the back up.
        if (is_prod) SaveLog(false); 
      }
    }

    public static void SaveEmail(string to, string subject, string body)
    {
      string sql = @"
          INSERT INTO EmailList 
          (EmailTo, EmailSubject, EmailBody)  
          VALUES (@To, @Subject, @Body);";

      try
      {
        var dbArgs = new Dapper.DynamicParameters();
        dbArgs.Add("@To", to);
        dbArgs.Add("@Subject", subject);
        dbArgs.Add("@Body", body);

        var cs = ErrorLog.GetLogCS(true);

        using (IDbConnection db = new SqlConnection(cs))
        {
          db.Execute(sql, dbArgs);
        }
      }
      catch (Exception ex)
      {
        // if we fail to save an email to the production server,
        // let's save it to the backup DB server.
        new ErrorLog(ex, sql, false);
      }
    }

    private static string GetLogCS(bool is_prod)
    {
      var builder = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

      var config = builder.Build();
      return is_prod ? config.GetConnectionString("LogProd") : config.GetConnectionString("LogBackup");      
    }


  }
}