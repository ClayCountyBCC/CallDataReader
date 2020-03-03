using System;
using System.Collections.Generic;
using System.Text;

namespace CallDataReader.Models
{
  public class CallerLocation
  {
    public long location_id { get; set; }
    public string session_id { get; set; }
    public DateTime location_timestamp { get; set; }
    public string call_type { get; set; }
    public string raw_location { get; set; }
    public decimal latitude 
    { 
      get
      {
        var location = raw_location.Trim();
        if (location.Length == 0) return 0;
        var raw = location.Split(" ");
        if (raw.Length != 2) return 0;
        try
        {
          if (Decimal.TryParse(raw[0], out decimal v))
          {
            return v;
          }
          return 0;
        }
        catch
        {
          return 0;
        }
        
      } 
    }
    public decimal longitude
    {
      get
      {
        var location = raw_location.Trim();
        if (location.Length == 0) return 0;
        var raw = location.Split(" ");
        if (raw.Length != 2) return 0;
        try
        {
          if (Decimal.TryParse(raw[1], out decimal v))
          {
            return v;
          }
          return 0;
        }
        catch
        {
          return 0;
        }
      }
    }
    public string confidence { get; set; }

    public CallerLocation(BaseData data) 
    {
      location_id = data.location_id;
      session_id = data.session_id;
      location_timestamp = data.call_origin_time;
      call_type = data.call_type;
      raw_location = data.caller_location;
      confidence = data.location_confidence;
    }

    public static bool IsValidLocation(BaseData data)
    {
      if (data.session_id.Length == 0) return false;
      if (data.call_id == -1) return false;
      if (data.location_id == -1) return false;

      bool valid_call_type = false;
      switch (data.call_type.Trim())
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

    public static void Save(List<CallerLocation> locations)
    {

    }

  }
}
