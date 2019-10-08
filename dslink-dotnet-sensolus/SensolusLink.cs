using CommandLine;
using DSLink;
using DSLink.Nodes;
using DSLink.Nodes.Actions;
using DSLink.Request;
using Newtonsoft.Json.Linq;
using Npgsql;
using Serilog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace dslink_dotnet_sensolus
{
    class SensolusLink : DSLinkContainer
    {

        private static Dictionary<string, SensolusCfg> connCfgs = new Dictionary<string, SensolusCfg>();

        private static string ThingsBoardURL = "";

        static void Main(string[] args)
        {
            List<string> argsList = new List<string>(args);
            int idx = argsList.FindIndex(x => x == "-t" || x == "--thingsboard");
            if(idx >= 0 && args.Length > idx+1)
            {
                ThingsBoardURL = args[idx + 1];
            }

            Parser.Default.ParseArguments<CommandLineArguments>(args)
                .WithParsed(cmdLineOptions =>
                {
                    cmdLineOptions = ProcessDSLinkJson(cmdLineOptions);

                    //Init the logging engine
                    InitializeLogging(cmdLineOptions);

                    //Construct a link Configuration
                    var config = new Configuration(cmdLineOptions.LinkName, false, true);

                    //Construct our custom link
                    var dslink = new SensolusLink(config, cmdLineOptions);

                    InitializeLink(dslink).Wait();
                })
                .WithNotParsed(errors => { Environment.Exit(-1); });
            while (true)
            {
                Thread.Sleep(1000);
            }
        }

        public static async Task InitializeLink(SensolusLink dsLink)
        {
            await dsLink.Connect();
            dsLink.LoadData();
            dsLink.InitializeDefaultNodes();
            await dsLink.SaveNodes();
        }

        public SensolusLink(Configuration config, CommandLineArguments cmdLineOptions) : base(config)
        {
            //Perform any configuration overrides from command line options
            if (cmdLineOptions.BrokerUrl != null)
            {
                config.BrokerUrl = cmdLineOptions.BrokerUrl;
            }

            if (cmdLineOptions.Token != null)
            {
                config.Token = cmdLineOptions.Token;
            }

            if (cmdLineOptions.NodesFileName != null)
            {
                config.NodesFilename = cmdLineOptions.NodesFileName;
            }

            if (cmdLineOptions.KeysFolder != null)
            {
                config.KeysFolder = cmdLineOptions.KeysFolder;
            }

            Responder.AddNodeClass("connNode", delegate (Node node)
            {
                node.CreateChild("Remove", "removeNode").BuildNode();
            });

            Responder.AddNodeClass("removeNode", delegate (Node node)
            {
                node.SetAction(new ActionHandler(Permission.Config, (InvokeRequest request) =>
                {
                    lock (connCfgs)
                    {
                        connCfgs.Remove(node.Parent.Name.ToLower());
                    }
                    SaveData();
                }));
            });

            Responder.AddNodeClass("handleActivity", delegate (Node node) {
                node.Configs.Set(ConfigType.DisplayName, new Value("Handle Activity Notification"));
                node.AddParameter(new Parameter {
                    Name = SensolusConstants.PATH,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter {
                    Name = SensolusConstants.SERIAL,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter {
                    Name = SensolusConstants.ACTIVITY_ID,
                    ValueType = DSLink.Nodes.ValueType.Number
                });
                node.AddParameter(new Parameter {
                    Name = SensolusConstants.ACTIVITY_TIME,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_INSERTTIME,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_STATE,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_LAT,
                    ValueType = DSLink.Nodes.ValueType.Number
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_LON,
                    ValueType = DSLink.Nodes.ValueType.Number
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_SRC,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_ACCURACY,
                    ValueType = DSLink.Nodes.ValueType.Number
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_ADDRESS,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.ACTIVITY_GEOZONE,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.SetAction(new ActionHandler(Permission.Config, _handleActivityNotification));
            });

            Responder.AddNodeClass("handleAlert", delegate (Node node) {
                node.Configs.Set(ConfigType.DisplayName, new Value("Handle Alert Notification"));
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.PATH,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = SensolusConstants.SERIAL,
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.SetAction(new ActionHandler(Permission.Config, _handleAlertNotification));
            });

            Responder.AddNodeClass("connAdd", delegate (Node node)
            {
                node.Configs.Set(ConfigType.DisplayName, new Value("Add Connection"));
                node.AddParameter(new Parameter
                {
                    Name = "Name",
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = "apiKey",
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = "DB Address",
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = "DB Port",
                    ValueType = DSLink.Nodes.ValueType.Number
                });
                node.AddParameter(new Parameter
                {
                    Name = "DB Name",
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = "DB User",
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.AddParameter(new Parameter
                {
                    Name = "DB Password",
                    ValueType = DSLink.Nodes.ValueType.String,
                    Editor = EditorType.Password
                });
                node.AddParameter(new Parameter
                {
                    Name = "Interval (minutes)",
                    ValueType = DSLink.Nodes.ValueType.Number
                });
                node.AddParameter(new Parameter
                {
                    Name = "Pool",
                    ValueType = DSLink.Nodes.ValueType.Boolean
                });
                node.AddParameter(new Parameter
                {
                    Name = "Old Data",
                    ValueType = DSLink.Nodes.ValueType.Boolean
                });
                node.AddParameter(new Parameter
                {
                    Name = "Context",
                    ValueType = DSLink.Nodes.ValueType.String
                });
                node.SetAction(new ActionHandler(Permission.Config, _createConnection));
            });

            Task.Run(() =>
            {
                long i = 0;
                while (true)
                {
                    lock (connCfgs)
                    {
                        foreach(var key in connCfgs.Keys)
                        {
                            var cfg = connCfgs[key];
                            if (i % cfg.Interval == 0)
                            {
                                Task.Run(() =>
                                {
                                    var copyCfg = cfg;
                                    copyCfg.Clean = false;
                                    DataProcessor dp = new DataProcessor(copyCfg);
                                    dp.Run(NpgsqlFactory.Instance);
                                    cfg.LastExecute = DateTime.Now;
                                    connCfgs[key] = cfg;
                                });
                            }
                        }
                    }
                    ++i;
                    Thread.Sleep(TimeSpan.FromMinutes(1));
                }
            });
        }
        private void _handleAlertNotification(InvokeRequest request)
        {
            Uri uri = new Uri("http://localhost" + request.Parameters[SensolusConstants.PATH].Value<string>());
            if (uri.Segments.Length <= 1) return;
            string context = uri.Segments[1].Replace("/", "");
            if (!connCfgs.ContainsKey(context)) return;
            SensolusCfg cfg = connCfgs[context];
            var query = System.Web.HttpUtility.ParseQueryString(uri.Query);

            DateTime start = DateTime.Now;
            DataProcessor dp = new DataProcessor(cfg);
            IDbTransaction transaction = null;

            string serial = "";

            try
            {
                var connectionStringBuilder = NpgsqlFactory.Instance.CreateConnectionStringBuilder();
                connectionStringBuilder.ConnectionString = $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password={cfg.Password}";
                using (DatabaseWrapper client = new DatabaseWrapper(NpgsqlFactory.Instance, connectionStringBuilder.ToString()))
                {
                    client.Open();
                    transaction = client.BeginTransaction();

                    dp.FirstPhase(client);

                    serial = request.Parameters[SensolusConstants.SERIAL]?.Value<string>();

                    FactActivity latestActivity = client.GetLatestActivity(serial);
                    int halfMinutes = (int)Math.Ceiling((DateTime.Now - latestActivity.Evttime).TotalMinutes / 2);
                    List<DimTracker> tracker = new List<DimTracker>();
                    tracker.Add(client.GetDimTracker(serial));
                    dp.SecondPhase(client, halfMinutes, tracker);
                    dp.ThirdPhase(client, new string[] { serial });

                    transaction.Commit();
                }
            }
            catch (SqlException e)
            {
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                transaction?.Rollback();
                return;
            }
            catch (ArgumentException e)
            {
                //wrong SQL Connection string
                Serilog.Log.Error("Wrong SQL Connection string: " + $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password=*****");
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                return;
            }
            catch (Exception e)
            {
                Serilog.Log.Error("Unexpected error");
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                return;
            }

            TimeSpan duration = DateTime.Now - start;

            try
            {
                var connectionStringBuilder = NpgsqlFactory.Instance.CreateConnectionStringBuilder();
                connectionStringBuilder.ConnectionString = $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password={cfg.Password}";
                using (DatabaseWrapper client = new DatabaseWrapper(NpgsqlFactory.Instance, connectionStringBuilder.ToString()))
                {
                    client.Open();
                    IDbCommand cmd = client.CreateCommand();
                    cmd.CommandText = $"INSERT INTO log (ts, duration, reqcount, initevent, serial) VALUES ({SqlConvert.Convert(DateTime.Now)}, {duration.TotalMilliseconds}, {dp.api.Count}, 'PN', {SqlConvert.Convert(serial)})";
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Serilog.Log.Error("Could not make log record");
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                return;
            }

            Serilog.Log.Information($"ts: {DateTime.Now}, duration: {duration.TotalMilliseconds}, reqCount: {dp.api.Count}, initevent: PN, serial: {serial}");
        }

            private async void _handleActivityNotification(InvokeRequest request)
        {
            Uri uri = new Uri("http://localhost" + request.Parameters[SensolusConstants.PATH].Value<string>());
            if (uri.Segments.Length <= 1) return;
            string context = uri.Segments[1].Replace("/", "");
            if (!connCfgs.ContainsKey(context)) return;
            SensolusCfg cfg = connCfgs[context];
            var query = System.Web.HttpUtility.ParseQueryString(uri.Query);

            DateTime start = DateTime.Now;
            DataProcessor dp = new DataProcessor(cfg);
            IDbTransaction transaction = null;

            string serial = "";

            try
            {
                var connectionStringBuilder = NpgsqlFactory.Instance.CreateConnectionStringBuilder();
                connectionStringBuilder.ConnectionString = $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password={cfg.Password}";
                using (DatabaseWrapper client = new DatabaseWrapper(NpgsqlFactory.Instance, connectionStringBuilder.ToString()))
                {
                    client.Open();
                    transaction = client.BeginTransaction();

                    dp.FirstPhase(client);
                    List<DimTracker> trackers = client.GetDimTrackers();

                    FactActivity activity = new FactActivity();
                    activity.Id = request.Parameters[SensolusConstants.ACTIVITY_ID].Value<long>();
                    activity.Evttime = DateTime.Parse(request.Parameters[SensolusConstants.ACTIVITY_TIME].Value<string>());
                    activity.Inserttime = DateTime.Parse(request.Parameters[SensolusConstants.ACTIVITY_INSERTTIME].Value<string>());
                    activity.Serial = request.Parameters[SensolusConstants.SERIAL].Value<string>();
                    activity.Evttype = request.Parameters[SensolusConstants.ACTIVITY_STATE]?.Value<string>();
                    activity.Lat = request.Parameters[SensolusConstants.ACTIVITY_LAT]?.Value<double?>();
                    activity.Lon = request.Parameters[SensolusConstants.ACTIVITY_LON]?.Value<double?>();
                    activity.Src = request.Parameters[SensolusConstants.ACTIVITY_SRC]?.Value<string>();
                    activity.Accuracy = request.Parameters[SensolusConstants.ACTIVITY_ACCURACY]?.Value<int?>();
                    activity.Address = request.Parameters[SensolusConstants.ACTIVITY_ADDRESS]?.Value<string>();
                    activity.Geozones = request.Parameters[SensolusConstants.ACTIVITY_GEOZONE]?.Value<string>();

                    serial = activity.Serial;

                    DimTracker tracker = trackers.Find(x => x.Serial == activity.Serial);

                    activity.Trackerrecid = tracker.Recid;

                    DateTime epoch = new DateTime(1970, 1, 1);

                    if (tracker.TBRef != null && ThingsBoardURL != "")
                    {
                        // send to TB
                        JObject data = new JObject();
                        if(activity.Evttype == "GeozoneInsideAlertType")
                        {
                            data["inGz"] = true;
                            JArray geozones = JArray.Parse(activity.Geozones);
                            data["lastGz"] = geozones[0].Value<string>();
                            data["lastGzEntry"] = (activity.Evttime - epoch).TotalMilliseconds;
                        }

                        else if (activity.Evttype == "GeozoneOutsideAlertType")
                        {
                            data["inGz"] = false;
                            data["lastGzExit"] = (activity.Evttime - epoch).TotalMilliseconds;
                        }

                        if(activity.Lat != null)
                        {
                            data["latitude"] = activity.Lat;
                        }
                        if (activity.Lon != null)
                        {
                            data["longitude"] = activity.Lon;
                        }
                        if (activity.Accuracy != null)
                        {
                            data["locAccuracy"] = activity.Accuracy;
                        }
                        if(activity.Lat != null || activity.Lon != null || activity.Accuracy != null)
                        {
                            data["lastLocTime"] = (activity.Evttime - epoch).TotalMilliseconds;
                        }
                        data["tracker"] = activity.Serial;

                        // send POST request to ThingsBoard
                        using (var httpClient = new HttpClient())
                        {
                            var response = await httpClient.PostAsync(
                                $"{ThingsBoardURL}/api/v1/{tracker.TBRef.Value.TBAt}/attributes",
                                 new StringContent(data.ToString(), Encoding.UTF8, "application/json"));
                        }
                    }

                    transaction.Commit();
                }
            }
            catch (SqlException e)
            {
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                transaction?.Rollback();
                return;
            }
            catch (ArgumentException e)
            {
                //wrong SQL Connection string
                Serilog.Log.Error("Wrong SQL Connection string: " + $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password=*****");
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                return;
            }
            catch (Exception e)
            {
                Serilog.Log.Error("Unexpected error");
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                return;
            }

            TimeSpan duration = DateTime.Now - start;

            try
            {
                var connectionStringBuilder = NpgsqlFactory.Instance.CreateConnectionStringBuilder();
                connectionStringBuilder.ConnectionString = $"Server={cfg.Host};Database={cfg.Database};User ID={cfg.User};Password={cfg.Password}";
                using (DatabaseWrapper client = new DatabaseWrapper(NpgsqlFactory.Instance, connectionStringBuilder.ToString()))
                {
                    client.Open();
                    IDbCommand cmd = client.CreateCommand();
                    cmd.CommandText = $"INSERT INTO log (ts, duration, reqcount, initevent, serial) VALUES ({SqlConvert.Convert(DateTime.Now)}, {duration.TotalMilliseconds}, {dp.api.Count}, 'PN', {SqlConvert.Convert(serial)})";
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception e)
            {
                Serilog.Log.Error("Could not make log record");
                Serilog.Log.Error(e.Message);
                Serilog.Log.Error(e.StackTrace);
                return;
            }

            Serilog.Log.Information($"ts: {DateTime.Now}, duration: {duration.TotalMilliseconds}, reqCount: {dp.api.Count}, initevent: PN, serial: {serial}");
        }


        private void _createConnection(InvokeRequest request)
        {
            SensolusCfg cfg = new SensolusCfg();
            string name = request.Parameters["Name"].Value<String>();
            if (connCfgs.ContainsKey(name.ToLower()))
            {
                // ERROR
                return;
            }
            cfg.ApiKey = request.Parameters["apiKey"].Value<String>();
            cfg.Host = request.Parameters["DB Address"].Value<String>();
            cfg.Port = request.Parameters["DB Port"].Value<int>();
            cfg.Database = request.Parameters["DB Name"].Value<String>();
            cfg.User = request.Parameters["DB User"].Value<String>();
            cfg.Password = request.Parameters["DB Password"].Value<String>();
            cfg.Interval = request.Parameters["Interval (minutes)"].Value<int>();
            cfg.Pool = request.Parameters["Pool"].Value<bool>();
            Task.Run(() =>
            {
                var copyCfg = cfg;
                if (request.Parameters["Old Data"].Value<bool>())
                {
                    copyCfg.Interval = (int)(DateTime.Now - new DateTime(1990, 1, 1)).TotalMinutes / 2;
                    copyCfg.Clean = true;
                }
                DataProcessor dp = new DataProcessor(copyCfg);
                dp.Run(NpgsqlFactory.Instance);
                cfg.LastExecute = DateTime.Now;
                lock (connCfgs)
                {
                    connCfgs.Add(name.ToLower(), cfg);
                }
                SaveData();
            });
            Responder.SuperRoot.CreateChild(name, "connNode").BuildNode();
        }


        public override void InitializeDefaultNodes()
        {
            Responder.SuperRoot.RemoveChild("handleActivityNotification");
            Responder.SuperRoot.RemoveChild("handleAlertNotification");
            Responder.SuperRoot.RemoveChild("addConnection");
            Responder.SuperRoot.CreateChild("handleActivityNotification", "handleActivity").BuildNode();
            Responder.SuperRoot.CreateChild("handleAlertNotification", "handleAlert").BuildNode();
            Responder.SuperRoot.CreateChild("addConnection", "connAdd").BuildNode();
        }

        #region Initialize Logging

        /// <summary>
        /// This method initializes the logging engine.  In this case Serilog is used, but you
        /// may use a variety of logging engines so long as they are compatible with
        /// Liblog (the interface used by the DSLink SDK)
        /// </summary>
        /// <param name="cmdLineOptions"></param>
        private static void InitializeLogging(CommandLineArguments cmdLineOptions)
        {
            if (cmdLineOptions.LogFileFolder != null &&
                !cmdLineOptions.LogFileFolder.EndsWith(Path.DirectorySeparatorChar))
            {
                throw new ArgumentException($"Specified LogFileFolder must end with '{Path.DirectorySeparatorChar}'");
            }

            var logConfig = new LoggerConfiguration();
            switch (cmdLineOptions.LogLevel)
            {
                case LogLevel.Debug:
                    logConfig.MinimumLevel.Debug();
                    break;

                case LogLevel.Unspecified:
                case LogLevel.Info:
                    logConfig.MinimumLevel.Information();
                    break;

                case LogLevel.Warning:
                    logConfig.MinimumLevel.Warning();
                    break;

                case LogLevel.Error:
                    logConfig.MinimumLevel.Error();
                    break;
            }
            logConfig.WriteTo.Console(
                outputTemplate:
                "{Timestamp:MM/dd/yyyy HH:mm:ss} {SourceContext} [{Level}] {Message}{NewLine}{Exception}");
            logConfig.WriteTo.Logger(lc =>
            {
                lc.WriteTo.RollingFile(cmdLineOptions.LogFileFolder + "log-{Date}.txt", retainedFileCountLimit: 3);
            });
            Log.Logger = logConfig.CreateLogger();
        }

        #endregion

        #region dslink-json file processing

        /// <summary>
        /// This method will return an instance of CommandLineArguments build with the following logic rules.
        /// The file dslink.json can be utilized to specifiy command line arguments.  These live within the config block
        /// of the file.  Here is an example:
        ///         ...
        ///         "configs" : {
        ///                 "broker" : {
        ///                     "type": "url",
        ///                     "value":  "mybroker",
        ///                     "default": "http:localhost:8080\conn"
        ///                 },
        ///              }
        /// 
        /// The code in this method considers only the attribute's name ("broker") and value ("mybroker") in this example).
        /// "type" and "default" are not used.
        /// 
        /// The receives an instance of CommandLineArguments previously built from the parser.  If the dslink-json paramater
        /// is not null the code will use the value specified rather than the default value of "dslink.json" for the file
        /// to read containing the information.
        /// 
        /// Options specified on the command line wins out over those specified in the file.
        /// 
        /// </summary>
        /// <param name="cmdLineOptions"></param>
        /// <returns></returns>
        private static CommandLineArguments ProcessDSLinkJson(CommandLineArguments cmdLineOptions)
        {
            bool errorIfNotFound = false;
            string fileName = "dslink.json";

            //If filename is specified then error if it is not found
            if (!String.IsNullOrEmpty(cmdLineOptions.DSLinkJsonFilename))
            {
                errorIfNotFound = true;
                fileName = cmdLineOptions.DSLinkJsonFilename;
            }

            string fileData = "";
            if (File.Exists(fileName))
            {
                fileData = File.ReadAllText(fileName);
                Console.WriteLine(
                    $"Will use a combination of options specified from the command line and those specified in {fileName}");
            }
            else
            {
                if (errorIfNotFound == true)
                {
                    throw new ArgumentException($"Specified dslink-json file <{fileName}> was not found");
                }
                else
                {
                    return cmdLineOptions;
                }
            }

            JObject dslinkJson = JObject.Parse(fileData);
            var dsLinkJsonConfig = dslinkJson["configs"];

            var cmdLineOptionsDslinkJson = new CommandLineArguments();

            cmdLineOptionsDslinkJson.BrokerUrl =
                GetDsLinkStringValueForAttributeName(dsLinkJsonConfig, "broker", cmdLineOptions.BrokerUrl);
            cmdLineOptionsDslinkJson.LinkName =
                GetDsLinkStringValueForAttributeName(dsLinkJsonConfig, "name", cmdLineOptions.LinkName);
            cmdLineOptionsDslinkJson.LogFileFolder =
                GetDsLinkStringValueForAttributeName(dsLinkJsonConfig, "log-file", cmdLineOptions.LogFileFolder);
            cmdLineOptionsDslinkJson.KeysFolder =
                GetDsLinkStringValueForAttributeName(dsLinkJsonConfig, "key", cmdLineOptions.KeysFolder);
            cmdLineOptionsDslinkJson.NodesFileName =
                GetDsLinkStringValueForAttributeName(dsLinkJsonConfig, "nodes", cmdLineOptions.NodesFileName);
            cmdLineOptionsDslinkJson.Token =
                GetDsLinkStringValueForAttributeName(dsLinkJsonConfig, "token", cmdLineOptions.Token);
            cmdLineOptionsDslinkJson.LogLevel = GetDsLinkLogLevel(dsLinkJsonConfig, cmdLineOptions.LogLevel);

            return cmdLineOptionsDslinkJson;
        }

        private static LogLevel GetDsLinkLogLevel(JToken configObj, LogLevel logLevel)
        {
            if (logLevel != LogLevel.Unspecified)
            {
                return logLevel;
            }

            string testString = "";
            try
            {
                testString = configObj["log"]["value"].ToString();
            }
            catch
            {
            }

            ;

            LogLevel useLogLevel = LogLevel.Info;
            if (!Enum.TryParse(testString, out useLogLevel))
            {
                throw new ArgumentException("Invalid 'value' specified for 'log' value in dslink-json file");
            }

            return useLogLevel;
        }

        private static string GetDsLinkStringValueForAttributeName(JToken configObj, string attributeName,
            string cmdLineValue)
        {
            //use cmdLineValue if specified else attempt to use the one from the dslink-json
            if (cmdLineValue != null)
            {
                return cmdLineValue;
            }

            try
            {
                return configObj[attributeName]["value"].ToString();
            }
            catch
            {
                return null;
            }
        }

        #endregion processing

        private void LoadData()
        {
            if (!File.Exists("data.json")) return;
            using (StreamReader r = new StreamReader("data.json"))
            {
                string s = r.ReadToEnd();
                JArray array = JArray.Parse(s);
                foreach (JObject obj in array)
                {
                    string name = obj["Name"].Value<string>();
                    SensolusCfg cfg = new SensolusCfg();
                    cfg.Interval = obj["Interval"].Value<int>();
                    cfg.Host = obj["Host"].Value<string>();
                    cfg.ApiKey = obj["ApiKey"].Value<string>();
                    cfg.Port = obj["Port"].Value<int>();
                    cfg.Database = obj["Database"].Value<string>();
                    cfg.User = obj["User"].Value<string>();
                    cfg.Password = obj["Password"].Value<string>();
                    cfg.Pool = obj["Pool"].Value<bool>();
                    cfg.Clean = obj["Clean"].Value<bool>();
                    cfg.LastExecute = obj["LastExecute"].Value<DateTime>();
                    Task.Run(() => {
                        SensolusCfg copyCfg = cfg;
                        copyCfg.Clean = false;
                        copyCfg.Interval = (int)(DateTime.Now - copyCfg.LastExecute).TotalMinutes;
                        DataProcessor dp = new DataProcessor(copyCfg);
                        dp.Run(NpgsqlFactory.Instance);
                        cfg.LastExecute = DateTime.Now;
                        lock (connCfgs) {
                            if(!connCfgs.ContainsKey(name.ToLower()))
                                connCfgs.Add(name.ToLower(), cfg);
                        }
                        Responder.SuperRoot.CreateChild(name, "connNode").BuildNode();
                    });
                }
            }
        }

        private void SaveData()
        {
            JArray array = new JArray();
            foreach (var obj in connCfgs)
            {
                JObject jobj = new JObject();
                jobj.Add("Name", obj.Key);
                jobj.Add("Interval", obj.Value.Interval);
                jobj.Add("Host", obj.Value.Host);
                jobj.Add("ApiKey", obj.Value.ApiKey);
                jobj.Add("Port", obj.Value.Port);
                jobj.Add("Database", obj.Value.Database);
                jobj.Add("User", obj.Value.User);
                jobj.Add("Password", obj.Value.Password);
                jobj.Add("Pool", obj.Value.Pool);
                jobj.Add("Clean", obj.Value.Clean);
                jobj.Add("LastExecute", obj.Value.LastExecute);
                array.Add(jobj);
            }
            using (StreamWriter sw = new StreamWriter("data.json", false)) {
                sw.Write(array.ToString(Newtonsoft.Json.Formatting.None));
            }
        }
    }
}