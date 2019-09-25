namespace Hangfire.Sqlite
{

    using Hangfire.Sqlite.Db;
    using Hangfire.Sqlite.Queues;

    /// <summary>
    /// Abstraction of a Hangfire job storage.
    /// </summary>
    internal interface IJobStorage : ISession
    {
        /// <summary>
        /// Gets the configured options of the current job storage.
        /// </summary>
        SqliteStorageOptions Options { get; }

        /// <summary>
        /// Gets all the configured job queue persistence providers.
        /// </summary>
        PersistentJobQueueProviderCollection QueueProviders { get; }
    }
}
