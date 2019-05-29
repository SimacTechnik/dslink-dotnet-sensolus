using dslink_dotnet_sensolus.DataModels;
using Newtonsoft.Json.Linq;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class DimRule : IEquatable<DimRule>, IKeyValue<long>
    {
        public long Recid { get; set; }
        public long Id { get; set; }
        public string Title { get; set; }
        public string Type { get; set; }
        public string Deviceserials { get; set; }
        public string Zonelst { get; set; }
        public DateTime Validfrom { get; set; }
        public DateTime? Validto { get; set; }


        public bool Equals(DimRule other)
        {
            return Id == other.Id &&
                Title == other.Title &&
                Type == other.Type &&
                Deviceserials == other.Deviceserials &&
                Zonelst == other.Zonelst;
        }

        public long GetKeyValue()
        {
            return Id;
        }
    }
    public static class DimRuleExtensions {

        public static void Insert(this DatabaseWrapper conn, List<DimRule> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("INSERT INTO Dim_Rule (id, title, type, deviceserials, zonelst) VALUES ");
            sb.Append(
                String.Join(", ", list.Select(x => $"({x.Id}, " +
                    $"{SqlConvert.Convert(x.Title)}, " +
                    $"{SqlConvert.Convert(x.Type)}, " +
                    $"{SqlConvert.Convert(x.Deviceserials)}, " +
                    $"{SqlConvert.Convert(x.Zonelst)})")
                )
            );
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static void Delete(this DatabaseWrapper conn, List<DimRule> list)
        {
            if(list.Count == 0)
            {
                return;
            }
            StringBuilder sb = new StringBuilder("UPDATE Dim_Zone SET validto = now() WHERE validto IS NULL AND id IN (");
            sb.Append(String.Join(", ", list.Select(x => $"{x.Id}")));
            sb.Append(')');
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }

        public static List<DimRule> GetRules(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM Dim_Rule WHERE validto IS NULL;";
            List<DimRule> data = new List<DimRule>();
            using (IDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    DimRule obj = new DimRule();
                    obj.Recid = (long)reader["recid"];
                    obj.Id = (long)reader["id"];
                    obj.Title = (DBNull.Value == reader["title"]) ? null : (string)reader["title"];
                    obj.Type = (string)reader["type"];
                    obj.Deviceserials = (DBNull.Value == reader["deviceserials"]) ? null : (string)reader["deviceserials"];
                    obj.Zonelst = (DBNull.Value == reader["zonelst"]) ? null : (string)reader["zonelst"];
                    obj.Validfrom = (DateTime)reader["validfrom"];
                    obj.Validto = (DBNull.Value == reader["validto"]) ? null : (DateTime?)reader["validto"];
                    data.Add(obj);
                }
            }
            return data;
        }

        public static List<DimRule> GetRules(this API api)
        {
            JArray jResponse = api.Call(@"/api/v1/alertrules", Method.GET, null);
            return jResponse.Select(x =>
                new DimRule
                {
                    Recid = 0,
                    Id = x["id"].Value<long>(),
                    Title = x["title"]?.Value<string>(),
                    Type = x["alertTypeName"]?.Value<string>(),
                    Deviceserials = x["deviceSerials"]?.ToString(Newtonsoft.Json.Formatting.None),
                    Zonelst = x["definitions"]?["selectedIds"]?.ToString(Newtonsoft.Json.Formatting.None),
                    Validfrom = DateTime.Now,
                    Validto = null
                })
                .ToList();
        }

        public static void ClearRules(this DatabaseWrapper conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = "DELETE FROM Dim_Rule;";
            cmd.ExecuteNonQuery();
        }
    }
}
