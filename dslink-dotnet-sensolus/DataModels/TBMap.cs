using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace dslink_dotnet_sensolus.DataModels
{
    public struct TBMap
    {
        public long? TBRef { get; set; }
        public string TBDevId { get; set; }
        public string TBDevName { get; set; }
        public string TBDevType { get; set; }
        public string TBAt { get; set; }
        public DateTime Created { get; set; }
    }


    public static class TBMapExtensions
    {
        public static List<TBMap> GetTBMaps(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM tb_map;";
            List<TBMap> data = new List<TBMap>();
            using (IDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    TBMap obj = new TBMap();
                    obj.TBRef = (long)reader["tbref"];
                    obj.TBDevId = (string)reader["tb_dev_id"];
                    obj.TBDevName = (string)reader["tb_dname"];
                    obj.TBDevType = (string)reader["tb_dev_type"];
                    obj.TBAt = (string)reader["tb_at"];
                    obj.Created = (DateTime)reader["created"];
                    data.Add(obj);
                }
            }
            return data;
        }

        public static TBMap? GetTBMap(this DatabaseWrapper conn, long tbref)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT * FROM tb_map WHERE tbref = {tbref};";
            using (IDataReader reader = cmd.ExecuteReader())
            {
                if(!reader.Read())
                {
                    return (TBMap?)null;
                }
                TBMap obj = new TBMap();
                obj.TBRef = (long)reader["tbref"];
                obj.TBDevId = (string)reader["tb_dev_id"];
                obj.TBDevName = (string)reader["tb_dname"];
                obj.TBDevType = (string)reader["tb_dev_type"];
                obj.TBAt = (string)reader["tb_at"];
                obj.Created = (DateTime)reader["created"];
                return (TBMap?)obj;
            }
        }
    }
}
