using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Linq;
using dslink_dotnet_sensolus.DataModels;
using System.Data.Common;

namespace dslink_dotnet_sensolus
{
    class DataProcessor
    {
        private SensolusCfg cfg;
        private API api;

        private static string GET_DIM_TRACKERS = "SELECT * FROM Dim_Tracker WHERE validto IS NULL;";
        private static string GET_DIM_ZONES = "SELECT * FROM Dim_Zone WHERE validto IS NULL;";
        private static string GET_DIM_RULES = "SELECT * FROM Dim_Rule WHERE validto IS NULL;";

        public DataProcessor(SensolusCfg cfg)
        {
            this.cfg = cfg;
            this.api = new API(cfg.ApiKey);
        }

        public void Run(DbProviderFactory factory)
        {
            IDbTransaction transaction = null;
            try
            {
                using (IDbConnection client = factory.CreateConnection())
                {
                    var connectionStringBuilder = factory.CreateConnectionStringBuilder();
                    connectionStringBuilder.ConnectionString = $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password={cfg.Password}";
                    client.ConnectionString = connectionStringBuilder.ToString();
                    client.Open();
                    transaction = client.BeginTransaction();
                    FirstPhase(client);
                    transaction.Commit();
                    transaction = null;
                }
            }
            catch(SqlException)
            {
                transaction?.Rollback();
            }
            catch(ArgumentException e)
            {
                // wrong SQL Connection string
            }
        }

        public void FirstPhase(IDbConnection conn)
        {
            ManageDim(conn, DimTracker.EmptyInstance, GET_DIM_TRACKERS, api.GetTrackers().ToDictionary(x => x.GetKeyValue(), x => x));
            ManageDim(conn, DimRule.EmptyInstance, GET_DIM_RULES, api.GetRules().ToDictionary(x => x.GetKeyValue(), x => x));
            ManageDim(conn, DimZone.EmptyInstance, GET_DIM_ZONES, api.GetZones().ToDictionary(x => x.GetKeyValue(), x => x));
        }

        public void ManageDim<T, TT>(IDbConnection conn, DataModel<T> obj, string sql, Dictionary<TT, T> sensolusObjs) where T : IKeyValue<TT>, IEquatable<T>
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            Dictionary<TT, T> dbObjs;
            using (IDataReader reader = cmd.ExecuteReader())
            {
                dbObjs = obj.FromDataReader(reader).ToDictionary(x => x.GetKeyValue(), x => x);
            }
            List<T> toAdd = new List<T>();
            List<T> toUpdate = new List<T>();
            List<T> toDelete = new List<T>();
            foreach (TT key in sensolusObjs.Keys)
            {
                if (dbObjs.ContainsKey(key))
                {
                    if (!EqualityComparer<T>.Default.Equals(dbObjs[key], sensolusObjs[key]))
                    {
                        toUpdate.Add(sensolusObjs[key]);
                    }
                }
                else
                {
                    toAdd.Add(sensolusObjs[key]);
                }
            }
            foreach (TT key in dbObjs.Keys)
            {
                if (!sensolusObjs.ContainsKey(key))
                {
                    toDelete.Add(dbObjs[key]);
                }
            }
            if(toAdd.Count != 0)
            {
                cmd.CommandText = obj.InsertSql(toAdd);
                cmd.ExecuteNonQuery();
            }
            if (toDelete.Count != 0)
            {
                cmd.CommandText = obj.DeleteSql(toDelete);
                cmd.ExecuteNonQuery();
            }
            if (toUpdate.Count != 0)
            {
                cmd.CommandText = obj.DeleteSql(toUpdate);
                cmd.ExecuteNonQuery();
                cmd.CommandText = obj.InsertSql(toUpdate);
                cmd.ExecuteNonQuery();
            }
        }
    }
}
