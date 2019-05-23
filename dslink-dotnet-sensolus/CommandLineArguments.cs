﻿using CommandLine;

namespace dslink_dotnet_sensolus
{
    public enum LogLevel
    {
        Unspecified = -1,
        Info,
        Debug,
        Warning,
        Error
    }

    /// <summary>
    /// This class represents the complete of set of all possible command line options
    /// used not only by DSLink SDK but the calling program itself
    /// 
    /// It makes use of the CommandLineParser nuget package to supply attributes to the various
    /// class members.  Those attributes are then used by the parser.
    /// </summary>
    public class CommandLineArguments
    {
        [Option('b', "broker",
            Required = false,
            HelpText = "The connection to the DSA Broker")]
        public string BrokerUrl { get; set; }

        [Option('n', "name",
            Required = true,
            HelpText = "The display name of the DSLink within the Broker")]
        public string LinkName { get; set; }

        [Option('l', "log",
            Required = false,
            Default = LogLevel.Unspecified,
            HelpText = "The log level for log messages")]
        public LogLevel LogLevel { get; set; }

        [Option('f', "log-file",
            Required = false,
            HelpText =
                "The folder ending with the Path.DirectorySeparatorChar specifying the location for log files to be written")]
        public string LogFileFolder { get; set; }

        [Option('o', "nodes",
            Required = false,
            HelpText = "The filename containing the DSLink nodes")]
        public string NodesFileName { get; set; }

        [Option('t', "token",
            Required = false,
            HelpText = "The security token for the connection to the Broker")]
        public string Token { get; set; }

        [Option('k', "key",
            Required = false,
            HelpText = "The path for the stored key")]
        public string KeysFolder { get; set; }

        [Option('d', "dslink-json",
            Required = false,
            HelpText = "The alternate filename containing the contents of dslink.json")]
        public string DSLinkJsonFilename { get; set; }
    }
}