namespace Hangfire.Sqlite.Queues
{
    using System;

    /// <summary>
    /// Job queue provider for SQLite persistence implementation.
    /// </summary>
    internal sealed class SqliteJobQueueProvider : IPersistentJobQueueProvider
    {
        public SqliteJobQueueProvider(IJobStorage storage)
        {
            if (storage == null)
            {
                throw new ArgumentNullException(nameof(storage));
            }

            this.JobQueue = new SqliteJobQueue(storage.Options.QueuePollInterval);
            this.JobQueueMonitoringApi = new SqliteJobQueueMonitoringApi(storage);
        }

        /// <inheritdoc />
        public IPersistentJobQueue JobQueue { get; }

        /// <inheritdoc />
        public IPersistentJobQueueMonitoringApi JobQueueMonitoringApi { get; }
    }
}
