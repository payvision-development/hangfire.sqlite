namespace Hangfire.Sqlite.Monitoring
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;

    using Hangfire.Common;
    using Hangfire.Sqlite.Db;
    using Hangfire.Sqlite.Entities;
    using Hangfire.Sqlite.Queues;
    using Hangfire.States;
    using Hangfire.Storage;
    using Hangfire.Storage.Monitoring;

    /// <summary>
    /// <see cref="IMonitoringApi"/> implementation for SQLite storages.
    /// </summary>
    internal sealed class SqliteMonitoringApi : IMonitoringApi
    {
        private readonly IJobStorage storage;

        private readonly int? jobListLimit;

        public SqliteMonitoringApi(IJobStorage storage, int? jobListLimit)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
            this.jobListLimit = jobListLimit;
        }

        /// <inheritdoc />
        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            IOrderedEnumerable<(IPersistentJobQueueMonitoringApi monitoring, string queue)> tuples =
                this.storage.QueueProviders.Select(x => x.JobQueueMonitoringApi)
                    .SelectMany(x => x.GetQueues(), (monitoring, queue) => (monitoring, queue))
                    .OrderBy(x => x.queue);

            return tuples.Select(
                    x =>
                    {
                        (IPersistentJobQueueMonitoringApi monitoring, string queue) = x;
                        EnqueuedAndFetchedCountDto counters = monitoring.GetEnqueuedAndFetchedCount(queue);
                        return new QueueWithTopEnqueuedJobsDto
                        {
                            Name = queue,
                            Length = counters.EnqueuedCount ?? 0,
                            Fetched = counters.FetchedCount,
                            FirstJobs = this.EnqueuedJobs(monitoring.GetEnqueuedJobIds(queue, 0, 5).ToArray())
                        };
                    })
                .ToList();
        }

        /// <inheritdoc />
        public IList<ServerDto> Servers()
        {
            const string ServersSql = "SELECT Id, Data, LastHeartbeat FROM [Server]";

            return this.storage.Query<Server>(ServersSql, null)
                .Select(
                    server =>
                    {
                        var data = SerializationHelper.Deserialize<ServerData>(server.Data);
                        if (data.Queues == null && data.StartedAt == null && data.WorkerCount == 0)
                        {
                            data = SerializationHelper.Deserialize<ServerData>(server.Data, SerializationOption.User);
                        }

                        return new ServerDto
                        {
                            Name = server.Id,
                            Heartbeat = server.LastHeartbeat,
                            Queues = data.Queues,
                            StartedAt = data.StartedAt?.DateTime ?? DateTime.MinValue,
                            WorkersCount = data.WorkerCount
                        };
                    })
                .ToList();
        }

        /// <inheritdoc />
        public JobDetailsDto JobDetails(string jobId) => throw new NotImplementedException();

        /// <inheritdoc />
        public StatisticsDto GetStatistics()
        {
            const string StatisticsSql = "SELECT COUNT(Id) FROM [Job] WHERE StateName='Enqueued';" +
                                         "SELECT COUNT(Id) FROM [Job] WHERE StateName='Failed';" +
                                         "SELECT COUNT(Id) FROM [Job] WHERE StateName='Processing';" +
                                         "SELECT COUNT(Id) FROM [Job] WHERE StateName='Scheduled';" +
                                         "SELECT COUNT(Id) FROM [Server];" + "SELECT SUM(s.[Value]) FROM (" +
                                         "SELECT SUM([Value]) AS [Value] FROM [Counter] WHERE [Key]='stats:succeeded' " +
                                         "UNION ALL " +
                                         "SELECT [Value] FROM [AggregatedCounter] WHERE [Key]='stats:succeeded') AS s;" +
                                         "SELECT SUM(s.[Value]) FROM (" +
                                         "SELECT SUM([Value]) AS [Value] FROM [Counter] WHERE [Key]='stats:deleted' " +
                                         "UNION ALL " +
                                         "SELECT [Value] FROM [AggregatedCounter] WHERE [Key]='stats:deleted') AS s;" +
                                         "SELECT COUNT(*) FROM [Set] WHERE [Key]='recurring-jobs';";

            using (IGridReader grid = this.storage.QueryMultiple(StatisticsSql, null))
            {
                return new StatisticsDto
                {
                    Enqueued = grid.ReadSingle<int>(),
                    Failed = grid.ReadSingle<int>(),
                    Processing = grid.ReadSingle<int>(),
                    Scheduled = grid.ReadSingle<int>(),
                    Servers = grid.ReadSingle<int>(),
                    Succeeded = grid.ReadSingleOrDefault<long?>() ?? 0,
                    Deleted = grid.ReadSingleOrDefault<long?>() ?? 0,
                    Recurring = grid.ReadSingle<int>(),
                    Queues = this.storage.QueueProviders.SelectMany(x => x.JobQueueMonitoringApi.GetQueues()).Count()
                };
            }
        }

        /// <inheritdoc />
        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
        {
            IPersistentJobQueueMonitoringApi queueApi = this.GetQueueApi(queue);
            IEnumerable<long> fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

            return this.EnqueuedJobs(fetchedJobIds.ToArray());
        }

        /// <inheritdoc />
        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
            IPersistentJobQueueMonitoringApi queueApi = this.GetQueueApi(queue);
            IEnumerable<long> fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

            return this.FetchedJobs(fetchedJobIds.ToArray());
        }

        /// <inheritdoc />
        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count) =>
            this.GetJobs(
                from,
                count,
                ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    InProcessingState = ProcessingState.StateName.Equals(
                        sqlJob.StateName,
                        StringComparison.OrdinalIgnoreCase),
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = sqlJob.StateChanged
                });

        /// <inheritdoc />
        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count) =>
            this.GetJobs(
                from,
                count,
                ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    InScheduledState = ScheduledState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    EnqueueAt = JobHelper.DeserializeNullableDateTime(stateData["EnqueueAt"]) ?? DateTime.MinValue,
                    ScheduledAt = sqlJob.StateChanged
                });

        /// <inheritdoc />
        public JobList<SucceededJobDto> SucceededJobs(int @from, int count) =>
            this.GetJobs(
                from,
                count,
                SucceededState.StateName,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    InSucceededState = SucceededState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    Result = stateData["Result"],
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                                        ? long.Parse(stateData["PerformanceDuration"], CultureInfo.InvariantCulture) +
                                          long.Parse(
                                              stateData["Latency"],
                                              CultureInfo.InvariantCulture)
                                        : (long?)null,
                    SucceededAt = sqlJob.StateChanged
                });

        /// <inheritdoc />
        public JobList<FailedJobDto> FailedJobs(int @from, int count) =>
            this.GetJobs(
                from,
                count,
                FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    InFailedState = FailedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = sqlJob.StateChanged
                });

        /// <inheritdoc />
        public JobList<DeletedJobDto> DeletedJobs(int @from, int count) =>
            this.GetJobs(
                from,
                count,
                DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    InDeletedState = DeletedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    DeletedAt = sqlJob.StateChanged
                });

        /// <inheritdoc />
        public long ScheduledCount() => this.GetNumberOfJobsByStateName(ScheduledState.StateName);

        /// <inheritdoc />
        public long EnqueuedCount(string queue)
        {
            IPersistentJobQueueMonitoringApi queueApi = this.GetQueueApi(queue);
            EnqueuedAndFetchedCountDto counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount ?? 0;
        }

        /// <inheritdoc />
        public long FetchedCount(string queue)
        {
            IPersistentJobQueueMonitoringApi queueApi = this.GetQueueApi(queue);
            EnqueuedAndFetchedCountDto counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount ?? 0;
        }

        /// <inheritdoc />
        public long FailedCount() => this.GetNumberOfJobsByStateName(FailedState.StateName);

        /// <inheritdoc />
        public long ProcessingCount() => this.GetNumberOfJobsByStateName(ProcessingState.StateName);

        /// <inheritdoc />
        public long SucceededListCount() => this.GetNumberOfJobsByStateName(SucceededState.StateName);

        /// <inheritdoc />
        public long DeletedListCount() => this.GetNumberOfJobsByStateName(DeletedState.StateName);

        /// <inheritdoc />
        public IDictionary<DateTime, long> SucceededByDatesCount() => this.GetTimelineStats("succeeded");

        /// <inheritdoc />
        public IDictionary<DateTime, long> FailedByDatesCount() => this.GetHourlyTimelineStats("failed");

        /// <inheritdoc />
        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            const string Type = "succeeded";
            return this.GetHourlyTimelineStats(Type);
        }

        /// <inheritdoc />
        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            const string Type = "failed";
            return this.GetHourlyTimelineStats(Type);
        }

        private static JobList<T> DeserializeJobs<T>(
            ICollection<SqliteJob> jobs,
            Func<SqliteJob, Job, SafeDictionary<string, string>, T> selector)
        {
            var result = new List<KeyValuePair<string, T>>(jobs.Count);

            foreach (SqliteJob job in jobs)
            {
                T dto = default(T);
                if (job.InvocationData != null)
                {
                    var deserializedData = SerializationHelper.Deserialize<Dictionary<string, string>>(job.StateData);
                    SafeDictionary<string, string> stateData = deserializedData != null
                                                                   ? new SafeDictionary<string, string>(
                                                                       deserializedData,
                                                                       StringComparer.OrdinalIgnoreCase)
                                                                   : null;
                    dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);
                }

                result.Add(new KeyValuePair<string, T>(job.Id.ToString(CultureInfo.InvariantCulture), dto));
            }

            return new JobList<T>(result);
        }

        private JobList<EnqueuedJobDto> EnqueuedJobs(long[] jobIds)
        {
            string EnqueuedJobsSql = "SELECT j.Id, j.StateId, j.StateName, j.InvocationData, j.Arguments, j.CreatedAt, " +
                                     "j.ExpireAt, s.Reason as StateReason, s.Data as StateData, s.CreatedAt as StateChanged " +
                                     "FROM [Job] j LEFT JOIN [State] s ON s.Id=j.StateId AND s.JobId=j.Id " +
                                     "WHERE j.Id in @jobIds";

            Dictionary<long, SqliteJob> jobs = this.storage.Query<SqliteJob>(EnqueuedJobsSql, new { jobIds = jobIds })
                .ToDictionary(x => x.Id, x => x);
            List<SqliteJob> sortedJobs = jobIds
                .Select(x => jobs.TryGetValue(x, out SqliteJob job) ? job : new SqliteJob { Id = x })
                .ToList();

            return DeserializeJobs(
                sortedJobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    InEnqueuedState = EnqueuedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    EnqueuedAt = EnqueuedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase)
                                     ? sqlJob.StateChanged
                                     : null
                });
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            InvocationData data = InvocationData.DeserializePayload(invocationData);
            if (!string.IsNullOrWhiteSpace(arguments))
            {
                data.Arguments = arguments;
            }

            try
            {
                return data.DeserializeJob();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private JobList<T> GetJobs<T>(
            int from,
            int count,
            string stateName,
            Func<SqliteJob, Job, SafeDictionary<string, string>, T> selector)
        {
            const string JobsSql =
                "SELECT j.Id, j.InvocationData, j.Arguments, s.Reason as StateReason, s.Data as StateData " +
                "FROM [Job] j LEFT JOIN [State] s ON j.StateId=s.Id " + "WHERE j.StateName=@stateName ORDER BY j.Id DESC " +
                "LIMIT @limit OFFSET @offset";

            List<SqliteJob> jobs = this.storage.Query<SqliteJob>(
                    JobsSql,
                    new
                    {
                        stateName,
                        limit = count,
                        offset = from
                    })
                .ToList();
            return DeserializeJobs(jobs, selector);
        }

        private long GetNumberOfJobsByStateName(string stateName)
        {
            const string Query = "SELECT COUNT(Id) FROM [Job] WHERE StateName=@state";
            const string QueryWithLimit = "SELECT COUNT(j.Id) FROM (SELECT Id FROM [Job] " +
                                          "WHERE StateName=@state LIMIT @limit) AS j";

            return this.storage.ExecuteScalar<int>(
                this.jobListLimit.HasValue ? QueryWithLimit : Query,
                new
                {
                    state = stateName,
                    limit = this.jobListLimit
                });
        }

        private JobList<FetchedJobDto> FetchedJobs(IEnumerable<long> jobIds)
        {
            const string FetchedJobsSql = "SELECT j.*, s.Reason as StateReason, s.Data as StateData FROM [Job] " +
                                          "LEFT JOIN [State] s ON s.Id=j.StateId AND s.JobId=j.Id " +
                                          "WHERE j.Id IN @jobIds";

            IEnumerable<SqliteJob> jobs = this.storage.Query<SqliteJob>(FetchedJobsSql, new { jobIds });
            return new JobList<FetchedJobDto>(
                jobs.Select(
                    job => new KeyValuePair<string, FetchedJobDto>(
                        job.Id.ToString(CultureInfo.InvariantCulture),
                        new FetchedJobDto
                        {
                            Job = DeserializeJob(job.InvocationData, job.Arguments),
                            State = job.StateName
                        })));
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName) =>
            this.storage.QueueProviders[queueName].JobQueueMonitoringApi;

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            const string DateFormat = "yyyy-MM-dd-HH";

            DateTime endDate = DateTime.UtcNow;
            IEnumerable<(string, DateTime current)> keyMaps = Enumerable.Repeat(0, 24)
                .Select(
                    _ =>
                    {
                        (string, DateTime) current = ($"stats:{type}:{endDate.ToString(DateFormat)}", endDate);
                        endDate = endDate.AddHours(-1);
                        return current;
                    });
            
            return this.GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            const string DateFormat = "yyyy-MM-dd";

            DateTime endDate = DateTime.UtcNow.Date;
            IEnumerable<(string, DateTime)> keyMaps = Enumerable.Repeat(0, 7)
                .Select(
                    _ =>
                    {
                        (string, DateTime) current = ($"stats:{type}:{endDate.ToString(DateFormat)}", endDate);
                        endDate = endDate.AddDays(-1);
                        return current;
                    });

            return this.GetTimelineStats(keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(IEnumerable<(string, DateTime)> timeline)
        {
            const string GetCountSql = "SELECT [Key], [Value] as [Count] FROM [AggregatedCounter] WHERE [Key] IN @keys";

            (string key, DateTime value)[] keyMaps = timeline.ToArray();
            Dictionary<string, long> valuesMap = this.storage
                .Query<(string Key, long Count)>(GetCountSql, new { keys = keyMaps.Select(x => x.key) })
                .ToDictionary(x => x.Key, x => x.Count);
            return keyMaps.ToDictionary(x => x.value, x => valuesMap.TryGetValue(x.key, out long value) ? value : 0L);
        }

        /// <summary>
        /// Overloaded dictionary that doesn't throw if given an invalid key
        /// Fixes issues such as https://github.com/HangfireIO/Hangfire/issues/871
        /// </summary>
        private class SafeDictionary<TKey, TValue> : Dictionary<TKey, TValue>
        {
            public SafeDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer) 
                : base(dictionary, comparer)
            {
            }

            public new TValue this[TKey i]
            {
                get { return ContainsKey(i) ? base[i] : default(TValue); }
                set { base[i] = value; }
            }
        }
    }
}
