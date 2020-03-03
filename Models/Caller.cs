using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using System.Data.SqlClient;
using Dapper;

namespace CallDataReader.Models
{
  public class Caller
  {
    public string session_id { get; set; }
    public long call_id { get; set; }
    public DateTime call_origin_time { get; set; }
    public string call_type { get; set; } = "";
    public string raw_phone_number { get; set; } = "";
    public string phone_number
    {
      get
      {
        if (raw_phone_number.Length == 0) return "";
        return Regex.Replace(raw_phone_number, @"(\(|\)|-|[ ]+)", "");
      }
    }
    public string business_name { get; set; } = "";

    public Caller(BaseData data)
    {
      session_id = data.session_id.Trim();
      call_id = data.call_id;
      call_origin_time = data.call_origin_time;
      call_type = data.call_type.Trim();
      raw_phone_number = data.phone_number.Trim();

      switch (data.call_location_type)
      {
        case "BUSN":
        case "PBXB":
          business_name = data.caller_name;
          break;

        default:
          business_name = "";
          break;
      }
    }

    public static bool IsValidCaller(BaseData data)
    {
      // here we look for things that would make this an invalid caller
      if (data.session_id.Length == 0) return false;
      if (data.call_id == -1) return false;
      return true;

    }

    public static void Save(List<Caller> callers, string cs)
    {
      if (callers.Count == 0) return;
      // this will need to be a merge statement
      // we will only want each session id to be in this table once.
      var dt = CreateDataTable();
      foreach(Caller c in callers)
      {
        try
        {
          dt.Rows.Add(
            c.session_id,
            c.call_id,
            c.call_origin_time,
            c.call_type,
            c.raw_phone_number,
            c.phone_number,
            c.business_name
            );

        }
        catch(Exception ex)
        {
          new ErrorLog(ex);
        }
        
      }
      // this query might need to be modified later to update a "last seen on" column.
      string query = @"
        SET NOCOUNT, XACT_ABORT ON;
        USE Tracking;

        DECLARE @Now DATETIME = GETDATE();

        MERGE Tracking.dbo.[911_callers] WITH (HOLDLOCK) AS C

        USING @CallerData AS CD ON C.session_id = CD.session_id

        WHEN NOT MATCHED THEN

          INSERT 
            (
              session_id
              ,call_id
              ,call_origin_time
              ,call_type
              ,raw_phone_number
              ,phone_number
              ,business_name
              ,added_on
            )
          VALUES (
              CD.session_id
              ,CD.call_id
              ,CD.call_origin_time
              ,CD.call_type
              ,CD.raw_phone_number
              ,CD.phone_number
              ,CD.business_name              
              ,@Now
          );";

      try
      {
        using (IDbConnection db = new SqlConnection(cs))
        {
          db.Execute(query, new { CallerData = dt.AsTableValuedParameter("CallerData") });
        }

      }
      catch (Exception ex)
      {
        new ErrorLog(ex);
      }
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("CallerData");
      dt.Columns.Add("session_id", typeof(string));
      dt.Columns.Add("call_id", typeof(long));
      dt.Columns.Add("call_origin_time", typeof(DateTime));
      dt.Columns.Add("call_type", typeof(string));
      dt.Columns.Add("raw_phone_number", typeof(string));
      dt.Columns.Add("phone_number", typeof(string));
      dt.Columns.Add("business_name", typeof(string));
      return dt;
    }

  }
}
