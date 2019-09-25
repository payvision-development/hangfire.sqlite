namespace Hangfire.Sqlite
{
    using System;

    using Hangfire.Storage;

    /// <summary>
    /// Hangfire storage implementation using SQLite database.
    /// </summary>
    public sealed class SqliteStorage : JobStorage
    {
        private readonly SqliteStorageOptions options;

        public SqliteStorage(SqliteStorageOptions options)
        {
            this.options = options ?? throw new ArgumentNullException(nameof(options));
        }

        /// <inheritdoc />
        public override IMonitoringApi GetMonitoringApi() => throw new System.NotImplementedException();

        /// <inheritdoc />
        public override IStorageConnection GetConnection() => throw new System.NotImplementedException();
    }
}
