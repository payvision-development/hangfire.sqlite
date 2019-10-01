namespace Hangfire.Sqlite.Queues
{
    /// <summary>
    /// Provides persistence storage for background jobs.
    /// </summary>
    internal interface IPersistentJobQueueProvider
    {
        /// <summary>
        /// Gets the persistent job queue.
        /// </summary>
        IPersistentJobQueue JobQueue { get; }

        /// <summary>
        /// Gets the job queue monitor.
        /// </summary>
        IPersistentJobQueueMonitoringApi JobQueueMonitoringApi { get; }
    }
}
