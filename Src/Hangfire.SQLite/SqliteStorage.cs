namespace Hangfire.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SQLite;

    using Dapper;

    using Hangfire.Sqlite.Db;
    using Hangfire.Sqlite.Installer;
    using Hangfire.Sqlite.Queues;
    using Hangfire.Storage;

    /// <summary>
    /// Hangfire storage implementation using SQLite database.
    /// </summary>
    public sealed class SqliteStorage : JobStorage
    {
        private readonly Func<IDbConnection> connectionFactory;

        private readonly SqliteStorageOptions options;

        private readonly string description = string.Empty;

        private PersistentJobQueueProviderCollection queueProviders;

        public SqliteStorage(SQLiteConnectionStringBuilder connectionString, SqliteStorageOptions options)
            : this(
                () => new SQLiteConnection(connectionString.ToString()) { Flags = SQLiteConnectionFlags.MapIsolationLevels },
                options)
        {
            this.description = connectionString.ToString();
        }

        internal SqliteStorage(Func<IDbConnection> connectionFactory, SqliteStorageOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
            this.connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));

            this.Initialize();
        }

        /// <inheritdoc />
        public override IMonitoringApi GetMonitoringApi() => throw new System.NotImplementedException();

        /// <inheritdoc />
        public override IStorageConnection GetConnection() => new SqliteStorageConnection(new JobRepository(this));

        /// <inheritdoc />
        public override string ToString() => this.description;

        private void Initialize()
        {
            if (this.options.PrepareSchemaIfNecessary)
            {
                SqliteObjectsInstaller.Install(this.connectionFactory);
            }

            var defaultQueueProvider = new SqliteJobQueueProvider(new JobRepository(this));
            this.queueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        private IDbConnection CreateAndOpenConnection()
        {
            IDbConnection connection = this.connectionFactory();
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            return connection;
        }

        private sealed class JobRepository : IJobStorage, ITransaction
        {
            private readonly SqliteStorage owner;

            private IDbConnection dedicatedConnection;

            private IDbTransaction dedicatedTransaction;

            public JobRepository(SqliteStorage owner) => this.owner = owner;

            private int? CommandTimeout => (int?)this.owner.options.CommandTimeout?.TotalSeconds;

            /// <inheritdoc />
            public SqliteStorageOptions Options => this.owner.options;

            /// <inheritdoc />
            public PersistentJobQueueProviderCollection QueueProviders => this.owner.queueProviders;

            /// <inheritdoc />
            public IEnumerable<T> Query<T>(string query, object param) =>
                this.UseConnection(
                    connection => connection.Query<T>(
                        query,
                        param,
                        this.dedicatedTransaction,
                        commandTimeout: this.CommandTimeout));

            /// <inheritdoc />
            public T ExecuteScalar<T>(string query, object param) =>
                this.UseConnection(
                    connection => connection.ExecuteScalar<T>(query, param, this.dedicatedTransaction, this.CommandTimeout));

            /// <inheritdoc />
            public int Execute(string query, object param) =>
                this.UseConnection(
                    connection => connection.Execute(query, param, this.dedicatedTransaction, this.CommandTimeout));

            /// <inheritdoc />
            public ITransaction BeginTransaction(IsolationLevel? isolationLevel)
            {
                IDbConnection connection = this.dedicatedConnection ?? this.owner.CreateAndOpenConnection();
                IDbTransaction transaction = isolationLevel.HasValue
                                                 ? connection.BeginTransaction(isolationLevel.Value)
                                                 : connection.BeginTransaction();
                return new JobRepository(this.owner)
                {
                    dedicatedConnection = connection,
                    dedicatedTransaction = transaction
                };
            }

            /// <inheritdoc />
            public void Commit() => this.dedicatedTransaction?.Commit();

            /// <inheritdoc />
            public void Rollback() => this.dedicatedTransaction?.Rollback();

            /// <inheritdoc />
            public void Dispose()
            {
                if (this.dedicatedConnection == null)
                {
                    return;
                }

                this.dedicatedTransaction?.Dispose();
                this.dedicatedTransaction = null;
                this.dedicatedConnection.Dispose();
                this.dedicatedConnection = null;
            }

            private T UseConnection<T>(Func<IDbConnection, T> action)
            {
                IDbConnection connection = null;
                try
                {
                    connection = this.dedicatedConnection ?? this.owner.CreateAndOpenConnection();
                    return action(connection);
                }
                finally
                {
                    if (connection != null && this.dedicatedConnection == null)
                    {
                        connection.Dispose();
                    }
                }
            }
        }
    }
}