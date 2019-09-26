namespace Hangfire.Sqlite.Queues
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Threading;

    using Hangfire.Annotations;
    using Hangfire.Common;
    using Hangfire.Sqlite.Db;
    using Hangfire.Storage;

    using CancellationEvent = Common.CancellationTokenExtentions.CancellationEvent;

    /// <summary>
    /// Job queue using SQLite persistence.
    /// </summary>
    internal sealed class SqliteJobQueue : IPersistentJobQueue
    {
        // This is an optimization that helps to overcome the polling delay, when
        // both client and server reside in the same process. Everything is working
        // without this event, but it helps to reduce the delays in processing.
        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(false);

        private readonly TimeSpan pollInterval;

        public SqliteJobQueue(TimeSpan pollInterval) =>
            this.pollInterval = pollInterval > TimeSpan.Zero ? pollInterval : TimeSpan.FromSeconds(1);

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

        /// <inheritdoc />
        public IFetchedJob Dequeue(ISession session, string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            if (queues.Length == 0)
            {
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));
            }

            using (CancellationEvent cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                do
                {
                    FetchedJob fetchedJob = this.FetchNextJob(session, queues);
                    if (fetchedJob != null)
                    {
                        return new SqliteTransactionJob(
                            session,
                            fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                            fetchedJob.Id);
                    }

                    WaitHandle.WaitAny(
                        new WaitHandle[] { cancellationEvent.WaitHandle, NewItemInQueueEvent },
                        this.pollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                }
                while (true);
            }
        }

        private FetchedJob FetchNextJob(ISession session, string[] queues)
        {
            const string LockJobSql = "UPDATE [JobQueue] SET LockedBy=@lockId WHERE Id IN (" +
                                      "SELECT Id FROM [JobQueue] WHERE LockedBy IS NULL " +
                                      "AND Queue IN @queues AND (FetchedAt IS NULL OR FetchedAt < @fetchedAt)" + 
                                      " LIMIT 1)";
            const string FetchJobSql = "SELECT Id, JobId, Queue, FetchedAt FROM [JobQueue] WHERE LockedBy=@lockId";

            string lockId = Guid.NewGuid().ToString();
            if (session.Execute(
                    LockJobSql,
                    new
                    {
                        lockId,
                        queues,
                        fetchedAt = DateTime.UtcNow
                    }) <= 0)
            {
                return null;
            }

            return session.Query<FetchedJob>(FetchJobSql, new { lockId }).SingleOrDefault();
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private class FetchedJob
        {
            public long Id { get; set; }

            public long JobId { get; set; }

            public string Queue { get; set; }

            public DateTime? FetchedAt { get; set; }
        }
    }
}
