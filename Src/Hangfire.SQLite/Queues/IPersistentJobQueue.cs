namespace Hangfire.Sqlite.Queues
{
    using System.Threading;

    using Hangfire.Sqlite.Db;
    using Hangfire.Storage;

    /// <summary>
    /// Defines a queue to store background jobs.
    /// </summary>
    internal interface IPersistentJobQueue
    {
        /// <summary>
        /// Enqueue the specified job into the specified queue.
        /// </summary>
        /// <param name="session">The persistence session.</param>
        /// <param name="queue">The name of the queue.</param>
        /// <param name="jobId">The identifier of the job.</param>
        void Enqueue(ISession session, string queue, string jobId);

        /// <summary>
        /// Dequeue the next job from the queue.
        /// </summary>
        /// <param name="session">The persistence session.</param>
        /// <param name="queues">The queues to be read.</param>
        /// <param name="cancellationToken">The cancellation token used to notify any operation cancellation.</param>
        /// <returns>The next job.</returns>
        IFetchedJob Dequeue(ISession session, string[] queues, CancellationToken cancellationToken);
    }
}
