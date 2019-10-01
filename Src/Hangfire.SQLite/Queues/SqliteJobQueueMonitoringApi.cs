namespace Hangfire.Sqlite.Queues
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    using Hangfire.Annotations;
    using Hangfire.Sqlite.Entities;
    using Hangfire.Sqlite.Monitoring;

    internal sealed class SqliteJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);

        private readonly IJobStorage storage;

        private readonly object cacheLock = new object();

        private List<string> queuesCache = new List<string>();

        private DateTime cacheUpdated;

        public SqliteJobQueueMonitoringApi(IJobStorage storage) =>
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));

        /// <inheritdoc />
        public IEnumerable<string> GetQueues()
        {
            string sqlQuery = "SELECT DISTINCT(Queue) FROM [JobQueue]";

            lock (this.cacheLock)
            {
                if (this.queuesCache.Count == 0 || this.cacheUpdated.Add(QueuesCacheTimeout) < DateTime.UtcNow)
                {
                    List<string> result = this.storage.Query<JobQueue>(sqlQuery, null).Select(x => x.Queue).ToList();

                    this.queuesCache = result;
                    this.cacheUpdated = DateTime.UtcNow;
                }

                return this.queuesCache;
            }
        }

        /// <inheritdoc />
        public IEnumerable<long> GetEnqueuedJobIds(string queue, int @from, int pageSize)
        {
            const string EnqueuedJobsSql = "SELECT JobId FROM [JobQueue] WHERE Queue=@queue AND FetchedAt IS NULL " +
                                           "ORDER BY Id LIMIT @limit OFFSET @offset";

            return this.storage.Query<JobIdDto>(
                    EnqueuedJobsSql,
                    new
                    {
                        queue,
                        limit = pageSize,
                        offset = @from
                    })
                .Select(x => x.JobId)
                .ToList();
        }

        /// <inheritdoc />
        public IEnumerable<long> GetFetchedJobIds(string queue, int @from, int pageSize)
        {
            const string FetchedJobsSql = "SELECT JobId FROM [JobQueue] WHERE Queue=@queue AND FetchedAt IS NOT NULL " +
                                          "ORDER BY Id LIMIT @limit OFFSET @offset";
            return this.storage.Query<JobIdDto>(
                    FetchedJobsSql,
                    new
                    {
                        queue,
                        limit = pageSize,
                        offset = @from
                    })
                .Select(x => x.JobId).ToList();
        }

        /// <inheritdoc />
        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            const string SqlQuery = "SELECT SUM(Enqueued) as EnqueuedCount, SUM(Fetched) AS FetchedCount FROM (" +
                                    "SELECT CASE WHEN FetchedAt IS NULL THEN 1 ELSE 0 END AS Enqueued," +
                                    "CASE WHEN FetchedAt IS NOT NULL THEN 1 ELSE 0 END AS Fetched " +
                                    "FROM [JobQueue] WHERE Queue=@queue) q";

            return this.storage.Query<EnqueuedAndFetchedCountDto>(SqlQuery, new { queue }).Single();
        }

        private sealed class JobIdDto
        {
            [UsedImplicitly]
            public long JobId { get; set; }
        }
    }
}