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

            this.JobQueue = new SqliteJobQueue();
        }

        /// <inheritdoc />
        public IPersistentJobQueue JobQueue { get; }
    }
}
