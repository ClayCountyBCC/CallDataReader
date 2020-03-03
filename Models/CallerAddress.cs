using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

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
    public string call_type { get; set; }
    public string raw_street_number { get; set; }
    public string raw_street_name { get; set; }
    public string street_number
    {
      get
      {
        // if there is a space and then a dash followed by something, let's get rid of it and replace it with a space.
        // this should hopefully match the whole_address column in the Address_Site table.
        return Regex.Replace(raw_street_number, "[ ]{2,}-", " ").Trim();
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

    public CallerAddress(BaseData data) 
    {
      session_id = data.session_id;
      geocode_status = geocoding_status.geocoding_not_performed;
      call_type = data.call_type;
      raw_street_number = data.street_number;
      raw_street_name = data.street_name;
    }


    public static bool IsValidAddress(BaseData data)
    {
      switch (data.call_type)
      {
        case "WPH2":
        case "WPH1":
        case "VOIP":
          return false;
      }
      if (data.street_name.Length == 0 || data.street_number.Length == 0 || data.city.Length == 0) return false;
      return true;
    }

    public static void Save(List<CallerAddress> addresses)
    {

    }

  }
}
