using System;
using System.Collections.Generic;
using System.Data;

namespace dslink_dotnet_sensolus
{
    public class FactTracker
    {
        public DateTime Ts { get; set; }
        public string Serial { get; set; }
        public long Trackerrecid { get; set; }
        public int? Battpercent { get; set; }
        public int? Estbattlife { get; set; }

        public static List<FactTracker> FromDataReader(IDataReader reader)
        {
            List<FactTracker> list = new List<FactTracker>();
            while(reader.Read())
            {
                FactTracker obj = new FactTracker();
                obj.Ts = (DateTime)reader["ts"];
                obj.Serial = (string)reader["serial"];
                obj.Trackerrecid = (long)reader["trackerrecid"];
                obj.Battpercent = (int?)reader["battpercent"];
                obj.Estbattlife = (int?)reader["estbattlife"];
                list.Add(obj);
            }
            return list;
        }
    }
}
