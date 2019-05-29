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
                var connectionStringBuilder = factory.CreateConnectionStringBuilder();
                connectionStringBuilder.ConnectionString = $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password={cfg.Password}";
                using (DatabaseWrapper client = new DatabaseWrapper(factory, connectionStringBuilder.ToString()))
                {
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
            //catch (ArgumentException e)
            //{
                //wrong SQL Connection string
            //}
        }

        public void FirstPhase(DatabaseWrapper conn)
        {
            CompareData(conn.GetDimTrackers().ToDictionary(x => x.GetKeyValue(), x => x), api.GetDimTrackers().ToDictionary(x => x.GetKeyValue(), x => x), conn.Insert, conn.Delete);
            CompareData(conn.GetRules().ToDictionary(x => x.GetKeyValue(), x => x), api.GetRules().ToDictionary(x => x.GetKeyValue(), x => x), conn.Insert, conn.Delete);
            CompareData(conn.GetZones().ToDictionary(x => x.GetKeyValue(), x => x), api.GetZones().ToDictionary(x => x.GetKeyValue(), x => x), conn.Insert, conn.Delete);
            Dictionary<string, DimTracker> trackers = conn.GetDimTrackers().ToDictionary(x => x.GetKeyValue(), x => x);
            Dictionary<string, FactTracker> dbFactTrackers = conn.GetFactTrackers().ToDictionary(x => x.GetKeyValue(), x => x); ;
            List<FactTracker> sensolusFactTrackers = api.GetFactTrackers(trackers);
            List<FactTracker> toAdd = new List<FactTracker>();
            foreach(var fact in sensolusFactTrackers)
            {
                if(!dbFactTrackers.ContainsKey(fact.GetKeyValue()))
                {
                    toAdd.Add(fact);
                }
            }
            conn.Insert(toAdd);
        }

        public void SecondPhase(DatabaseWrapper conn, int interval, bool import = false)
        {
            Dictionary<string, DimTracker> trackers = conn.GetDimTrackers().ToDictionary(x => x.GetKeyValue(), x => x);
            DateTime from = DateTime.MinValue;
            if (!import)
                from = DateTime.Now.AddMinutes(-interval);
            DateTime to = DateTime.Now;
            Dictionary<long, FactActivity> dbData = conn.GetActivities(from, to).ToDictionary(x => x.GetKeyValue(), x => x);
            List<FactActivity> sensolusData = api.GetActivities(from, to, trackers);
            List<FactActivity> toAdd = new List<FactActivity>();
            foreach(var obj in sensolusData)
            {
                if(!dbData.ContainsKey(obj.GetKeyValue()))
                {
                    toAdd.Add(obj);
                }
            }
            conn.Insert(toAdd);
        }

        public void ThirdPhase(DatabaseWrapper conn)
        {
            Dictionary<long, DimRule> rules = conn.GetRules().ToDictionary(x => x.GetKeyValue(), x => x);
            string[] serials = conn.GetDimTrackers().Select(x => x.Serial).ToArray();

            DateTime from = DateTime.MinValue;
            DateTime to = DateTime.Now;

            Dictionary<string, FactActivity> activities = conn.GetActivities(from, to)
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
            Dictionary<string, FactAlert> dbObjs = conn.GetAlerts().ToDictionary(x => x.GetKeyValue(), x => x);
            List<FactAlert> sensolusData = api.GetAlerts(serials, activities, rules);
            List<FactAlert> toAdd = new List<FactAlert>();
            foreach(var alert in sensolusData)
            {
                if(!dbObjs.ContainsKey(alert.GetKeyValue()))
                {
                    toAdd.Add(alert);
                }
            }
            conn.Insert(toAdd);
        }

        public void CompareData<T, TT>(Dictionary<TT, T> dbObjs, Dictionary<TT, T> sensolusObjs, Action<List<T>> addAction, Action<List<T>> deleteAction) where T : IKeyValue<TT>, IEquatable<T>
        {
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

            addAction(toAdd);
            deleteAction(toDelete);
            deleteAction(toUpdate);
            addAction(toUpdate);
        }
    }
}
