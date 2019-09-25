namespace Hangfire.Sqlite
{
    using System;
    using System.Data;

    using Hangfire.Sqlite.Installer;
    using Hangfire.Storage;

    /// <summary>
    /// Hangfire storage implementation using SQLite database.
    /// </summary>
    public sealed class SqliteStorage : JobStorage
    {
        private readonly Func<IDbConnection> connectionFactory;

        private readonly SqliteStorageOptions options;

        public SqliteStorage(Func<IDbConnection> connectionFactory, SqliteStorageOptions options)
        {
            this.connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            this.options = options ?? throw new ArgumentNullException(nameof(options));

            this.Initialize();
        }

        /// <inheritdoc />
        public override IMonitoringApi GetMonitoringApi() => throw new System.NotImplementedException();

        /// <inheritdoc />
        public override IStorageConnection GetConnection() => throw new System.NotImplementedException();

        private void Initialize()
        {
            if (this.options.PrepareSchemaIfNecessary)
            {
                SqliteObjectsInstaller.Install(this.connectionFactory);
            }
        }
    }
}
