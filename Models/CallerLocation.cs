using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using System.Data;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace CallDataReader.Models
{
  public class CallerLocation
  {
    public long location_id { get; set; }
    public string session_id { get; set; }
    public DateTime location_timestamp { get; set; }    
    public string call_location_type { get; set; }
    public int location_type { get; set; } = 0; // 0 = from Solacom, 1 = from geocoding
    public string agency { get; set; } = "";
    public string raw_location { get; set; }
    public decimal latitude { get; set; } = 0;
    public decimal longitude { get; set; } = 0;
    public string confidence { get; set; }

    public CallerLocation(BaseData data) 
    {
      location_id = data.location_id;
      session_id = data.session_id;
      location_timestamp = data.call_origin_time;
      call_location_type = data.call_location_type;
      raw_location = Regex.Replace(data.caller_location, @"\s+", " ");
      confidence = data.location_confidence;
      agency = data.agency_name;
      ProcessLocation();
    }

    public static bool IsValidLocation(BaseData data)
    {
      if (data.session_id.Length == 0) return false;
      if (data.call_id == -1) return false;
      if (data.location_id == -1) return false;

      bool valid_call_type = false;
      switch (data.call_location_type.Trim())
      {
        case "WPH1":
        case "WPH2":
        case "VOIP":
          valid_call_type = true;
          break;

      }
      if (!valid_call_type) return false;

      if (data.caller_location.Trim().Length == 0) return false;

      return true;
    }

    public static void Save(List<CallerLocation> locations, string cs)
    {
      if (locations.Count == 0) return;
      // this will need to be a merge statement
      // we will only want each session id to be in this table once.
      var dt = CreateDataTable();
      foreach (CallerLocation cl in locations)
      {
        try
        {
          if(cl.latitude != 0)
          {
            dt.Rows.Add(
              cl.location_id,
              cl.session_id,
              cl.location_type,
              cl.agency,
              cl.location_timestamp,
              cl.call_location_type,
              cl.raw_location,
              cl.latitude,
              cl.longitude,
              cl.confidence
              );
          }


        }
        catch (Exception ex)
        {
          new ErrorLog(ex);
        }

      }
      // this query might need to be modified later to update a "last seen on" column.
      string query = @"
        SET NOCOUNT, XACT_ABORT ON;
        USE Tracking;

        DECLARE @Now DATETIME = GETDATE();

        MERGE Tracking.dbo.[911_caller_locations] WITH (HOLDLOCK) AS CL

        USING @CallerLocationData AS CLD ON CL.location_id = CLD.location_id

        WHEN NOT MATCHED THEN

          INSERT 
            (
              location_id
              ,session_id
              ,location_type
              ,location_timestamp
              ,agency
              ,call_type
              ,raw_location
              ,latitude
              ,longitude
              ,confidence
              ,added_on
            )
          VALUES (
              CLD.location_id
              ,CLD.session_id
              ,CLD.location_type
              ,CLD.location_timestamp
              ,CLD.agency
              ,CLD.call_type
              ,CLD.raw_location
              ,CLD.latitude
              ,CLD.longitude
              ,CLD.confidence              
              ,@Now
          );";

      try
      {
        using (IDbConnection db = new SqlConnection(cs))
        {
          db.Execute(query, new { CallerLocationData = dt.AsTableValuedParameter("CallerLocationData") });
        }

      }
      catch (Exception ex)
      {
        new ErrorLog(ex);
      }
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("CallerLocationData");
      dt.Columns.Add("location_id", typeof(long));
      dt.Columns.Add("session_id", typeof(string));
      dt.Columns.Add("location_type", typeof(Int16));
      dt.Columns.Add("agency", typeof(string));
      dt.Columns.Add("location_timestamp", typeof(DateTime));
      dt.Columns.Add("call_type", typeof(string));
      dt.Columns.Add("raw_location", typeof(string));
      dt.Columns.Add("latitude", typeof(decimal));
      dt.Columns.Add("longitude", typeof(decimal));      
      dt.Columns.Add("confidence", typeof(string));
      return dt;
    }

    private void ProcessLocation()
    {
      var location = raw_location.Trim();
      if (location.Length == 0)
      {
        return;
      }
      var raw = location.Split(" ");
      if (raw.Length != 2)
      {
        return;
      }
      try
      {
        if (Decimal.TryParse(raw[0], out decimal tmp_lat))
        {
          latitude = tmp_lat;
        }
        if (Decimal.TryParse(raw[1], out decimal tmp_long))
        {
          longitude = tmp_long;
        }
      }
      catch
      {
        return;
      }
    }

  }
}
