namespace Hangfire.Sqlite.Queues
{
    using System.Globalization;
    using System.Threading;

    using Hangfire.Sqlite.Db;

    /// <summary>
    /// Job queue using SQLite persistence.
    /// </summary>
    internal sealed class SqliteJobQueue : IPersistentJobQueue
    {
        // This is an optimization that helps to overcome the polling delay, when
        // both client and server reside in the same process. Everything is working
        // without this event, but it helps to reduce the delays in processing.
        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(false);

        /// <inheritdoc />
        public void Enqueue(ISession session, string queue, string jobId)
        {
            const string EnqueueJobSql = "INSERT INTO [JobQueue](JobId, Queue) VALUES (@jobId, @queue)";

            session.Execute(
                EnqueueJobSql,
                new
                {
                    jobId = long.Parse(jobId, CultureInfo.InvariantCulture),
                    queue
                });
        }
    }
}
