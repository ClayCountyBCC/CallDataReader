using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using Npgsql;
using System.Data;

namespace CallDataReader.Models
{
  public class BaseData
  {
    public long call_id { get; set; } = -1;
    public string session_id { get; set; } = "";
    public DateTime call_origin_time { get; set; } = DateTime.MinValue;
    public string call_type { get; set; } = "";
    public string agency_name { get; set; } = "";
    public long location_id { get; set; } = -1;
    public string phone_number { get; set; } = "";
    public string street_number { get; set; } = "";
    public string street_name { get; set; } = "";
    public string city { get; set; } = "";
    public string caller_state { get; set; } = "";
    public string call_location_type { get; set; } = "";
    public string caller_name { get; set; } = "";
    public string caller_location { get; set; } = "";
    public string location_confidence { get; set; } = "";

    public BaseData() { }

    public static List<BaseData> Get(string cs, long previous_max_id)
    {
      var param = new DynamicParameters();
      param.Add(":previous_max_id", previous_max_id);

      string query = @"
        SELECT 
          S.""SourceRecordID"" call_id
          ,S.""solacom-session-id"" session_id
          ,S.""solacom-origin-time"" call_origin_time
          ,S.""solacom-callType"" call_type
          ,S.""solacom-agencyOrElement"" agency_name
          ,A.""SourceRecordID"" location_id
          ,A.""Attach1"" phone_number
          ,A.""Attach2"" street_number
          ,A.""Attach3"" street_name
          ,A.""Attach4"" city
          ,A.""Attach5"" caller_state
          ,A.""Attach7"" call_location_type
          ,A.""Attach8"" caller_name
          ,A.""Attach16"" caller_location
          ,A.""Attach17"" location_confidence
        FROM PUBLIC.""SolacomCDRv10"" S
        INNER JOIN PUBLIC.""Attachments"" A ON S.""FileID"" = A.""FileID""
        WHERE
          S.""SourceRecordID"" > :previous_max_id
          AND S.""solacom-eventType"" = 'ALIresponse'
        ORDER BY S.""solacom-session-id"" ASC, S.""SourceRecordID"" DESC";

      try
      {
        using (IDbConnection db = new NpgsqlConnection(cs))
        {
          return (List<BaseData>)db.Query<BaseData>(query, param);
        }
      }
      catch(Exception ex)
      {
        new ErrorLog(ex);
        return null;
      }
      

    }



  }
}
