using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace dslink_dotnet_sensolus
{
    public class DatabaseWrapper : IDisposable, IDbConnection
    {
        private IDbConnection connection;

        public string ConnectionString { get => this.connection.ConnectionString; set => this.connection.ConnectionString = value; }

        public int ConnectionTimeout { get => this.connection.ConnectionTimeout; }

        public string Database { get => this.connection.Database; }

        public ConnectionState State { get => this.connection.State; }

        public DatabaseWrapper(DbProviderFactory factory, string connectionString)
        {
            this.connection = factory.CreateConnection();
            this.connection.ConnectionString = connectionString;
        }

        public void Open()
        {
            this.connection.Open();
        }

        public void Dispose()
        {
            this.connection.Dispose();
        }

        public IDbTransaction BeginTransaction()
        {
            return this.connection.BeginTransaction();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            return this.connection.BeginTransaction(il);
        }

        public void ChangeDatabase(string databaseName)
        {
            this.connection.ChangeDatabase(databaseName);
        }

        public void Close()
        {
            this.connection.Close();
        }

        public IDbCommand CreateCommand()
        {
            return this.connection.CreateCommand();
        }
    }
}
