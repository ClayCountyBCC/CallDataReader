using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Data;
using System.Data.SqlClient;
using Dapper;

namespace CallDataReader.Models
{
  public class CallerAddress
  {
    public enum geocoding_status: int
    {
      geocoding_not_performed = 0,
      geocoding_successful = 1,
      geocoding_error = 2
    }

    public string session_id { get; set; }
    public geocoding_status geocode_status { get; set; } = 0;
    public string call_location_type { get; set; }
    public string raw_street_number { get; set; }
    public string raw_street_name { get; set; }
    public string street_number
    {
      get
      {
        // if there is a space and then a dash followed by something, let's get rid of it and replace it with a space.
        // this should hopefully match the whole_address column in the Address_Site table.
        return Regex.Replace(raw_street_number, "([ ]{2,}-|[ ]+)", " ").Trim();
      }
    }    
    public string street_name
    {
      get
      {
        // get rid of the apartment / suite numbers that appear sometimes in the address.
        return Regex.Replace(raw_street_name, "[  ]{2,}.+", "");         
      }
    }
    public string city { get; set; } = "";
    public string state { get; set; } = "";

    public CallerAddress(BaseData data) 
    {
      session_id = data.session_id;
      geocode_status = geocoding_status.geocoding_not_performed;
      call_location_type = data.call_location_type;
      raw_street_number = data.street_number;
      raw_street_name = data.street_name;
      city = data.city;
      state = data.caller_state;
    }

    public static bool IsValidAddress(BaseData data)
    {
      switch (data.call_location_type)
      {
        case "WPH2":
        case "WPH1":
        case "VOIP":
          return false;
      }
      if (data.street_name.Length == 0 || data.street_number.Length == 0 || data.city.Length == 0) return false;
      return true;
    }

    public static void Save(List<CallerAddress> addresses, string cs)
    {
      if (addresses.Count == 0) return;
      // this will need to be a merge statement
      // we will only want each session id to be in this table once.
      var dt = CreateDataTable();
      foreach (CallerAddress ca in addresses)
      {
        try
        {
          dt.Rows.Add(
            ca.session_id,
            ca.geocode_status,
            ca.call_location_type,
            ca.raw_street_number,
            ca.raw_street_name,
            ca.street_number,
            ca.street_name,
            ca.city,
            ca.state          
            );
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

        MERGE Tracking.dbo.[911_caller_addresses] WITH (HOLDLOCK) AS CA

        USING @CallerAddressData AS CAD ON CA.session_id = CAD.session_id

        WHEN NOT MATCHED THEN

          INSERT 
            ( 
              session_id
              ,geocode_status
              ,call_type
              ,raw_street_number
              ,raw_street_name
              ,street_number
              ,street_name
              ,city
              ,state
              ,added_on
            )
          VALUES (
              CAD.session_id
              ,CAD.geocode_status
              ,CAD.call_type
              ,CAD.raw_street_number
              ,CAD.raw_street_name
              ,CAD.street_number
              ,CAD.street_name
              ,CAD.city
              ,CAD.state
              ,@Now
          );";

      try
      {
        using (IDbConnection db = new SqlConnection(cs))
        {
          db.Execute(query, new { CallerAddressData = dt.AsTableValuedParameter("CallerAddressData") });
        }

      }
      catch (Exception ex)
      {
        new ErrorLog(ex);
      }
    }

    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("CallerAddressData");
      dt.Columns.Add("session_id", typeof(string));
      dt.Columns.Add("geocode_status", typeof(Int16));
      dt.Columns.Add("call_type", typeof(string));
      dt.Columns.Add("raw_street_number", typeof(string));
      dt.Columns.Add("raw_street_name", typeof(string));
      dt.Columns.Add("street_number", typeof(string));
      dt.Columns.Add("street_name", typeof(string));
      dt.Columns.Add("city", typeof(string));
      dt.Columns.Add("state", typeof(string));
      return dt;
    }

  }
}
