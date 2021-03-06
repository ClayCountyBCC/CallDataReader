﻿using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System;
using System.Linq;
using System.IO;
using System.Threading;
using CallDataReader.Models;
using System.Data.SqlClient;
using System.Data;
using Dapper;

namespace CallDataReader
{
  class Program
  {
    static void Main()
    {
      var builder = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

      var config = builder.Build();

      string call_db_cs = config.GetConnectionString("CallDB");
      string tracking_db_cs = config.GetConnectionString("TrackingDB");

      long previous_max_id = Get_Previous_Max_Id(tracking_db_cs);

      
      DateTime end_time = DateTime.Today.AddHours(5).AddMinutes(-1);
      if (DateTime.Now.Hour > 4) end_time = DateTime.Today.AddDays(1).AddHours(5).AddMinutes(-1);

      int error_count = 0;
      //Console.WriteLine($"Program is scheduled to end on: { end_time.ToString() }");
      //new ErrorLog("Program running", end_time.ToString(), "", "", "", true);
      // process steps
      // main loop
      while (DateTime.Now < end_time)
      {
        try
        {
          // I'm aware that DateTime.Now's accuracy is bad, but it's good enough for this.
          // plus or minus 15 milliseconds is fine.
          DateTime loopend = DateTime.Now.AddSeconds(5);
          // read the calls that have been added since we last ran
          var data = BaseData.Get(call_db_cs, previous_max_id);
          if (data != null)
          {
            data = (from bd in data
                    orderby bd.call_origin_time ascending
                    select bd).ToList();


            if (data.Count() > 0)
            {
              long new_max = (from d in data select d.call_id).Max();
              previous_max_id = Math.Max(new_max, previous_max_id);

              List<Caller> callers = new List<Caller>();
              List<CallerAddress> addresses = new List<CallerAddress>();
              List<CallerLocation> locations = new List<CallerLocation>();
              HashSet<string> seen_sessions = new HashSet<string>();
              // break them dowm into their components
              foreach (BaseData d in data)
              {
                if (Caller.IsValidCaller(d))
                {
                  if (!seen_sessions.Contains(d.session_id))
                  {
                    seen_sessions.Add(d.session_id);
                    callers.Add(new Caller(d));
                    if (CallerAddress.IsValidAddress(d))
                    {
                      addresses.Add(new CallerAddress(d));
                    }
                  }

                  if (CallerLocation.IsValidLocation(d))
                  {
                    locations.Add(new CallerLocation(d));
                  }
                }
              }
              // save the components
              Caller.Save(callers, tracking_db_cs);
              CallerAddress.Save(addresses, tracking_db_cs);
              CallerLocation.Save(locations, tracking_db_cs);
              //Console.WriteLine($"Saved {callers.Count().ToString()} callers");
              //new ErrorLog($"Saved {callers.Count().ToString()} callers", callers.Count().ToString(), "", "", "", true);

            }
            // wait for at most 5 seconds
            var now = DateTime.Now;
            int current = (int)loopend.Subtract(now).TotalMilliseconds;
            if (current > 0)
            {
              Thread.Sleep(current);
            }
            error_count = 0;
          }
          else
          {
            Console.WriteLine("Data returned from the database is invalid, sleeping for 60 seconds.");
            new ErrorLog("Data returned is invalid", "sleeping for 60 seconds", "", "", "", true);
            Thread.Sleep(60000);
          }

        }
        catch(Exception ex)
        {
          new ErrorLog(ex);
          error_count++;
          if(error_count > 10)
          {
            Thread.Sleep(60000); // let's wait a minute
            error_count = 0;
          }
        }
      }

    }

    private static long Get_Previous_Max_Id(string cs)
    {
      string query = "SELECT ISNULL(MAX(call_id), 7145348) FROM Tracking.dbo.[911_callers]";
      try
      {
        using (IDbConnection db = new SqlConnection(cs))
        {
          return db.ExecuteScalar<long>(query);
        }
      }
      catch (Exception ex)
      {
        new ErrorLog(ex, query);
        return -1;
      }
      // this function will return the max value stored in the database once we have some data in there.
    }

  }
}
