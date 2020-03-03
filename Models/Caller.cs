using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;

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

      switch (call_type)
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

    public static void Save(List<Caller> callers)
    {

    }

  }
}
