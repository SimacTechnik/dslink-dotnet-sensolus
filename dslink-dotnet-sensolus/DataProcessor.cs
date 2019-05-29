using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
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
        private static string GET_FACT_ALERT = "SELECT * FROM Fact_Alert;";
        private static string GET_FACT_TRACKER = "SELECT * FROM Fact_Tracker;";

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
                    SecondPhase(client, cfg.Interval, true);
                    ThirdPhase(client);
                    transaction.Commit();
                    transaction = null;
                }
            }
            catch(SqlException)
            {
                transaction?.Rollback();
            }
            catch (ArgumentException e)
            {
                //wrong SQL Connection string
            }
        }

        public void FirstPhase(IDbConnection conn)
        {
            ManageDim(conn, DimTracker.EmptyInstance, GET_DIM_TRACKERS, api.GetDimTrackers().ToDictionary(x => x.GetKeyValue(), x => x));
            ManageDim(conn, DimRule.EmptyInstance, GET_DIM_RULES, api.GetRules().ToDictionary(x => x.GetKeyValue(), x => x));
            ManageDim(conn, DimZone.EmptyInstance, GET_DIM_ZONES, api.GetZones().ToDictionary(x => x.GetKeyValue(), x => x));
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = GET_DIM_TRACKERS;
            Dictionary<string, DimTracker> dbTrackers;
            using (IDataReader reader = cmd.ExecuteReader())
            {
                dbTrackers = DimTracker.EmptyInstance.FromDataReader(reader).ToDictionary(x => x.GetKeyValue(), x => x);
            }
            FactTracker.EmptyInstance.SetTrackers(dbTrackers);
            cmd.CommandText = GET_FACT_TRACKER;
            Dictionary<string, FactTracker> factTrackers;
            using (IDataReader reader = cmd.ExecuteReader())
            {
                factTrackers = FactTracker.EmptyInstance.FromDataReader(reader).ToDictionary(x => x.GetKeyValue(), x => x);
            }
            var sensolusFactTrackers = api.GetFactTrackers();
            List<FactTracker> toAdd = new List<FactTracker>();
            foreach(var fact in sensolusFactTrackers)
            {
                if(!factTrackers.ContainsKey(fact.GetKeyValue()))
                {
                    toAdd.Add(fact);
                }
            }
            if (toAdd.Count != 0)
            {
                cmd.CommandText = FactTracker.EmptyInstance.InsertSql(toAdd);
                cmd.ExecuteNonQuery();
            }
        }

        public void SecondPhase(IDbConnection conn, int interval, bool import = false)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = GET_DIM_TRACKERS;
            Dictionary<string, DimTracker> trackers;
            using (IDataReader reader = cmd.ExecuteReader())
            {
                trackers = DimTracker.EmptyInstance.FromDataReader(reader).ToDictionary(x => x.GetKeyValue(), x => x);
            }
            FactActivity.EmptyInstance.SetTrackers(trackers);
            DateTime from = DateTime.MinValue;
            if (!import)
                from = DateTime.Now.AddMinutes(-interval);
            DateTime to = DateTime.Now;
            Dictionary<long, FactActivity> dbData;
            cmd.CommandText = $"SELECT * FROM Fact_Activity WHERE evttime BETWEEN '" +
                $"{from.ToString("yyyy-MM-dd HH:mm:ss")}' AND '" +
                $"{to.ToString("yyyy-MM-dd HH:mm:ss")}'";
            using (IDataReader reader = cmd.ExecuteReader())
            {
                dbData = FactActivity.EmptyInstance.FromDataReader(reader).ToDictionary(x => x.GetKeyValue(), x => x);
            }
            List<FactActivity> sensolusData = api.GetActivities(from, to, trackers.Select(x => x.Key).ToArray());
            List<FactActivity> toAdd = new List<FactActivity>();
            foreach(var obj in sensolusData)
            {
                if(!dbData.ContainsKey(obj.GetKeyValue()))
                {
                    toAdd.Add(obj);
                }
            }
            if (toAdd.Count != 0)
            {
                cmd.CommandText = FactActivity.EmptyInstance.InsertSql(toAdd);
                cmd.ExecuteNonQuery();
            }
        }

        public void ThirdPhase(IDbConnection conn)
        {
            IDbCommand cmd = conn.CreateCommand();
            cmd.CommandText = GET_DIM_RULES;
            Dictionary<long, DimRule> rules;
            using (IDataReader reader = cmd.ExecuteReader())
            {
                rules = DimRule.EmptyInstance.FromDataReader(reader).ToDictionary(x => x.GetKeyValue(), x => x);
            }
            FactAlert.EmptyInstance.SetRules(rules);

            cmd.CommandText = GET_DIM_TRACKERS;
            string[] serials;
            using (IDataReader reader = cmd.ExecuteReader())
            {
                serials = DimTracker.EmptyInstance.FromDataReader(reader).Select(x => x.Serial).ToArray();
            }

            DateTime from = DateTime.MinValue;
            DateTime to = DateTime.Now;
            Dictionary<string, FactActivity> activities;
            cmd.CommandText = $"SELECT * FROM Fact_Activity WHERE evttime BETWEEN '" +
                $"{from.ToString("yyyy-MM-dd HH:mm:ss")}' AND '" +
                $"{to.ToString("yyyy-MM-dd HH:mm:ss")}'";
            using (IDataReader reader = cmd.ExecuteReader())
            {
                activities = FactActivity.EmptyInstance.FromDataReader(reader)
                    .GroupBy(x => x.Evttime.ToString()+x.Serial)
                    .Select(x => {
                        var list = x.ToList();
                        if (list.Count == 1) return list.First();
                        if (list[0].Evttype == "ON_THE_MOVE" && list[1].Evttype == "STOP") return list[1];
                        else if (list[1].Evttype == "ON_THE_MOVE" && list[0].Evttype == "STOP") return list[0];
                        else if (list[0].Evttype == "START") return list[1];
                        else return list[0];
                    })
                    .ToDictionary(x => x.Evttime.ToString()+x.Serial, x => x);
            }
            FactAlert.EmptyInstance.SetActivities(activities);

            cmd.CommandText = GET_FACT_ALERT;
            Dictionary<string, FactAlert> dbObjs;
            using (IDataReader reader = cmd.ExecuteReader())
            {
                dbObjs = FactAlert.EmptyInstance.FromDataReader(reader).ToDictionary(x => x.GetKeyValue(), x => x);
            }

            List<FactAlert> sensolusData = api.GetAlerts(serials);
            List<FactAlert> toAdd = new List<FactAlert>();
            foreach(var alert in sensolusData)
            {
                if(!dbObjs.ContainsKey(alert.GetKeyValue()))
                {
                    toAdd.Add(alert);
                }
            }
            if (toAdd.Count != 0)
            {
                cmd.CommandText = FactAlert.EmptyInstance.InsertSql(toAdd);
                cmd.ExecuteNonQuery();
            }
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
