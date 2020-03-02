using Microsoft.Extensions.Configuration;
using System;
using System.Linq;
using System.IO;
using CallDataReader.Models;

namespace CallDataReader
{
  class Program
  {
    static void Main(string[] args)
    {
      var builder = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

      var config = builder.Build();

      string call_db_cs = config.GetConnectionString("CallDB");
      string tracking_db_cs = config.GetConnectionString("TrackingDB");

      var data = BaseData.Get(call_db_cs);

      var max = (from d in data
                 select d.call_id).Max();

      Console.WriteLine(max.ToString());
      // don't store WPH1 / WPH2 addresses

    }
  }
}
