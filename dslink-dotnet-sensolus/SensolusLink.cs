﻿using CommandLine;
using DSLink;
using DSLink.Nodes;
using DSLink.Nodes.Actions;
using DSLink.Request;
using Newtonsoft.Json.Linq;
using Npgsql;
using Serilog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace dslink_dotnet_sensolus
{
    class SensolusLink : DSLinkContainer
    {
        private Dictionary<string, SensolusCfg> connCfgs = new Dictionary<string, SensolusCfg>();

        static void Main(string[] args)
        {
            SensolusCfg cfg = new SensolusCfg();
            cfg.ApiKey = "e45e5cfe125c44adad15e7602246700e";
            cfg.Database = "tracking";
            cfg.Host = "10.16.50.10";
            cfg.Port = 5432;
            cfg.User = "trkdbusr";
            cfg.Password = "9%b73%ZxD!)6";
            DataProcessor dp = new DataProcessor(cfg);
            dp.Run(NpgsqlFactory.Instance);
            //Parser.Default.ParseArguments<CommandLineArguments>(args)
            //    .WithParsed(cmdLineOptions =>
            //    {
            //        cmdLineOptions = ProcessDSLinkJson(cmdLineOptions);

            //        //Init the logging engine
            //        InitializeLogging(cmdLineOptions);

            //        //Construct a link Configuration
            //        var config = new Configuration(cmdLineOptions.LinkName, false, true);

            //        //Construct our custom link
            //        var dslink = new SensolusLink(config, cmdLineOptions);

            //        InitializeLink(dslink).Wait();
            //    })
            //    .WithNotParsed(errors => { Environment.Exit(-1); });
        }

        public static async Task InitializeLink(SensolusLink dsLink)
        {
            await dsLink.Connect();
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
                node.SetAction(new ActionHandler(Permission.Config, _createConnection));
            });
        }

        private void _createConnection(InvokeRequest request)
        {
            SensolusCfg cfg = new SensolusCfg();
            string name = request.Parameters["Name"].Value<String>();
            if(connCfgs.ContainsKey(name))
            {
                // ERROR
            }
            cfg.ApiKey = request.Parameters["apiKey"].Value<String>();
            cfg.Host = request.Parameters["DB Address"].Value<String>();
            cfg.Port = request.Parameters["DB Port"].Value<int>();
            cfg.Database = request.Parameters["DB Name"].Value<String>();
            cfg.User = request.Parameters["DB User"].Value<String>();
            cfg.Password = request.Parameters["DB Password"].Value<String>();
            cfg.Interval = request.Parameters["Interval (minutes)"].Value<int>();
            cfg.Pool = request.Parameters["Pool"].Value<bool>();
            connCfgs.Add(name, cfg);
            Responder.SuperRoot.CreateChild("name", "connNode").BuildNode();
        }


        public override void InitializeDefaultNodes()
        {
            Responder.SuperRoot.CreateChild("addConnection", "connAdd").BuildNode();
        }

        private void _updateRandomNumbers()
        {
            while (Thread.CurrentThread.IsAlive)
            {

                Thread.Sleep(1);
            }
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
    }
}
