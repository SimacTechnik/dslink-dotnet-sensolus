using System;
using System.Collections.Generic;
using System.Data;

namespace dslink_dotnet_sensolus
{
    public partial class FactActivity
    {

        public long Id { get; set; }
        public DateTime Evttime { get; set; }
        public DateTime Inserttime { get; set; }
        public string Serial { get; set; }
        public string Evttype { get; set; }
        public double? Lat { get; set; }
        public double? Lon { get; set; }
        public string Src { get; set; }
        public int? Accuracy { get; set; }
        public string Address { get; set; }
        public string Geozones { get; set; }
        public long Trackerrecid { get; set; }

        public static List<FactActivity> FromDataReader(IDataReader reader)
        {
            List<FactActivity> list = new List<FactActivity>();
            while (reader.Read())
            {
                FactActivity obj = new FactActivity();
                obj.Id = (long)reader["id"];
                obj.Evttime = (DateTime)reader["evttime"];
                obj.Inserttime = (DateTime)reader["inserttime"];
                obj.Serial = (string)reader["serial"];
                obj.Evttype = (string)reader["evttype"];
                obj.Lat = (double?)reader["lat"];
                obj.Lon = (double?)reader["lon"];
                obj.Src = (string)reader["src"];
                obj.Accuracy = (int?)reader["accuracy"];
                obj.Address = (string)reader["address"];
                obj.Geozones = (string)reader["geozones"];
                obj.Trackerrecid = (long)reader["trackerrecid"];
                list.Add(obj);
            }
            return list;
        }
    }
}
