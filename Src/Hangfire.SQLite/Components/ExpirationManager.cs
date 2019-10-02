namespace Hangfire.Sqlite.Components
{
    using System;
    using System.Data;
    using System.Threading;

    using Hangfire.Common;
    using Hangfire.Logging;
    using Hangfire.Server;

    internal sealed class ExpirationManager : IServerComponent
    {
        private const string DistributedLockKey = "locks:expirationmanager";

        // This value should be high enough to optimize the deletion as much, as possible,
        // reducing the number of queries. But low enough to cause lock escalations (it
        // appears, when ~5000 locks were taken, but this number is a subject of version).
        // Note, that lock escalation may also happen during the cascade deletions for
        // State (3-5 rows/job usually) and JobParameters (2-3 rows/job usually) tables.
        private const int NumberOfRecordsInSinglePass = 1000;

        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromMinutes(5);

        private static readonly string[] ProcessedTables =
        {
            "AggregatedCounter",
            "Job",
            "List",
            "Set",
            "Hash",
        };

        private readonly ILog logger = LogProvider.For<ExpirationManager>();

        private readonly IJobStorage storage;

        private readonly TimeSpan checkInterval;

        public ExpirationManager(IJobStorage storage, TimeSpan checkInterval)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            this.checkInterval = checkInterval;
        }

        /// <inheritdoc />
        public void Execute(CancellationToken cancellationToken)
        {
            const string ExpireQuery = "DELETE FROM @table WHERE Id IN (" +
                                       "SELECT Id FROM @table WHERE ExpireAt < @expireAt LIMIT @limit";

            DateTime expireAt = DateTime.UtcNow;
            foreach (string table in ProcessedTables)
            {
                this.logger.Debug($"Removing outdated records from the '{table}' table...");

                using (this.storage.Lock(DistributedLockKey, DefaultLockTimeout))
                using (this.storage.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    int affected;

                    do
                    {
                        affected = this.storage.Execute(
                            ExpireQuery,
                            new
                            {
                                table,
                                expireAt,
                                limit = NumberOfRecordsInSinglePass
                            });
                    }
                    while (affected == NumberOfRecordsInSinglePass);

                    this.logger.Trace($"Outdated records removed from the '{table}' table.");
                }
            }

            cancellationToken.Wait(this.checkInterval);
        }

        /// <inheritdoc />
        public override string ToString() => this.GetType().ToString();
    }
}