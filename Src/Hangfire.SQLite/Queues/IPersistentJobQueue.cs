namespace Hangfire.Sqlite.Queues
{
    using Hangfire.Sqlite.Db;

    /// <summary>
    /// Defines a queue to store background jobs.
    /// </summary>
    internal interface IPersistentJobQueue
    {
        /// <summary>
        /// Enqueues the specified job into the specified queue.
        /// </summary>
        /// <param name="session">The persistence session.</param>
        /// <param name="queue">The name of the queue.</param>
        /// <param name="jobId">The identifier of the job.</param>
        void Enqueue(ISession session, string queue, string jobId);
    }
}
