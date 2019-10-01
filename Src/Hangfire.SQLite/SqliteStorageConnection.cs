namespace Hangfire.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;

    using Hangfire.Common;
    using Hangfire.Server;
    using Hangfire.Sqlite.Concurrency;
    using Hangfire.Sqlite.Db;
    using Hangfire.Sqlite.Entities;
    using Hangfire.Sqlite.Queues;
    using Hangfire.Storage;

    internal sealed class SqliteStorageConnection : JobStorageConnection
    {
        private readonly IJobStorage storage;

        private LockedResources lockedResources = new LockedResources();

        public SqliteStorageConnection(IJobStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        /// <inheritdoc />
        public override IWriteOnlyTransaction CreateWriteTransaction() =>
            new SqliteWriteOnlyTransaction(this.storage, this.lockedResources);

        /// <inheritdoc />
        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            if (string.IsNullOrWhiteSpace(resource))
            {
                throw new ArgumentNullException(nameof(resource));
            }

            return this.lockedResources.Lock(resource, timeout);
        }

        /// <inheritdoc />
        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null)
            {
                throw new ArgumentNullException(nameof(job));
            }

            if (parameters == null)
            {
                throw new ArgumentNullException(nameof(parameters));
            }

            const string CreateJobSql = "INSERT INTO [Job](InvocationData, Arguments, CreatedAt, ExpireAt) " +
                                        "VALUES (@invocationData, @arguments, @createdAt, @expireAt);" +
                                        "SELECT last_insert_rowid();";
            const string CreateParametersSql = "INSERT INTO [JobParameter](JobId, Name, Value) " +
                                               "VALUES (@jobId, @name, @value);";

            using (ITransaction transaction = this.storage.BeginTransaction())
            {
                InvocationData invocationData = InvocationData.SerializeJob(job);
                string payload = invocationData.SerializePayload(true);
                var jobId = transaction.ExecuteScalar<long>(
                    CreateJobSql,
                    new
                    {
                        invocationData = payload,
                        arguments = invocationData.Arguments,
                        createdAt,
                        expireAt = createdAt.Add(expireIn)
                    });
                if (parameters.Count > 0)
                {
                    transaction.Execute(
                        CreateParametersSql,
                        parameters.Select(
                                p => new
                                {
                                    jobId,
                                    name = p.Key,
                                    value = p.Value
                                })
                            .ToArray());
                }

                transaction.Commit();
                return jobId.ToString(CultureInfo.InvariantCulture);
            }
        }

        /// <inheritdoc />
        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
            {
                throw new ArgumentNullException(nameof(queues));
            }

            IPersistentJobQueueProvider[] providers = queues
                .Select(queue => this.storage.QueueProviders[queue])
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {string.Join(", ", queues)}." +
                    " Only one type of persistent queue per server instance should be chosen.");
            }

            return providers[0].JobQueue.Dequeue(this.storage, queues, cancellationToken);
        }

        /// <inheritdoc />
        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            const string InsertJobParameterSql = "REPLACE INTO [JobParameter](JobId, Name, Value) " +
                                                 "VALUES (@jobId, @name, @value)";
            this.storage.Execute(
                InsertJobParameterSql,
                new
                {
                    jobId = long.Parse(id, CultureInfo.InvariantCulture),
                    name,
                    value
                });
        }

        /// <inheritdoc />
        public override string GetJobParameter(string id, string name)
        {
            if (id == null)
            {
                throw new ArgumentNullException(nameof(id));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            const string GetJobParameterSql = "SELECT Value FROM [JobParameter] WHERE JobId=@id AND Name=@name LIMIT 1";
            return this.storage.ExecuteScalar<string>(
                GetJobParameterSql,
                new
                {
                    id = long.Parse(id, CultureInfo.InvariantCulture),
                    name
                });
        }

        /// <inheritdoc />
        public override JobData GetJobData(string jobId)
        {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            const string GetJobDataSql = "SELECT InvocationData, StateName, Arguments, CreatedAt " +
                                         "FROM [Job] WHERE Id=@id";

            SqliteJob storedJobData = this.storage.Query<SqliteJob>(
                    GetJobDataSql,
                    new { id = long.Parse(jobId, CultureInfo.InvariantCulture) })
                .SingleOrDefault();
            if (storedJobData == null)
            {
                return null;
            }

            // TODO: conversion exception could be thrown. (From HangFire)
            InvocationData invocationData = InvocationData.DeserializePayload(storedJobData.InvocationData);
            if (!string.IsNullOrEmpty(storedJobData.Arguments))
            {
                invocationData.Arguments = storedJobData.Arguments;
            }

            var jobData = new JobData
            {
                State = storedJobData.StateName,
                CreatedAt = storedJobData.CreatedAt
            };
            try
            {
                jobData.Job = invocationData.DeserializeJob();
            }
            catch (JobLoadException exception)
            {
                jobData.LoadException = exception;
            }

            return jobData;
        }

        /// <inheritdoc />
        public override StateData GetStateData(string jobId)
        {
            if (jobId == null)
            {
                throw new ArgumentNullException(nameof(jobId));
            }

            const string GetDataSql = "SELECT s.Name, s.Reason, s.Data FROM [State] s " +
                                      "INNER JOIN [Job] j ON j.StateId=s.Id AND j.Id=s.JobId WHERE j.Id=@jobId";

            SqliteState sqlState = this.storage.Query<SqliteState>(
                    GetDataSql,
                    new { jobId = long.Parse(jobId, CultureInfo.InvariantCulture) })
                .SingleOrDefault();
            if (sqlState == null)
            {
                return null;
            }

            var data = new Dictionary<string, string>(
                SerializationHelper.Deserialize<Dictionary<string, string>>(sqlState.Data),
                StringComparer.OrdinalIgnoreCase);
            return new StateData
            {
                Name = sqlState.Name,
                Reason = sqlState.Reason,
                Data = data
            };
        }

        /// <inheritdoc />
        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            if (context == null)
            {
                throw new ArgumentNullException(nameof(context));
            }

            const string AddServerSql = "REPLACE INTO [Server](Id, Data, LastHeartbeat) VALUES (@id, @data, @heartbeat)";

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTimeOffset.UtcNow
            };

            this.storage.Execute(
                AddServerSql,
                new
                {
                    id = serverId,
                    data = SerializationHelper.Serialize(data),
                    heartbeat = data.StartedAt.Value.DateTime
                });
        }

        /// <inheritdoc />
        public override void RemoveServer(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            const string RemoveServerSql = "DELETE FROM [Server] WHERE Id=@id";
            this.storage.Execute(RemoveServerSql, new { id = serverId });
        }

        /// <inheritdoc />
        public override void Heartbeat(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            const string RenovateServerSql = "UPDATE [Server] SET LastHeartbeat=@now WHERE Id=@id";
            int affected = this.storage.Execute(
                RenovateServerSql,
                new
                {
                    now = DateTime.UtcNow,
                    id = serverId
                });
            if (affected == 0)
            {
                throw new BackgroundServerGoneException();
            }
        }

        /// <inheritdoc />
        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException($"The `{nameof(timeOut)}`value must be positive.", nameof(timeOut));
            }

            const string RemoveServerSql = "DELETE FROM [Server] WHERE LastHeartbeat < @timeOutAt";

            return this.storage.Execute(RemoveServerSql, new { timeOutAt = DateTime.UtcNow.Add(timeOut.Negate()) });
        }

        /// <inheritdoc />
        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string GetSetSql = "SELECT Value FROM [Set] WHERE [Key] = @key";
            return new HashSet<string>(this.storage.Query<string>(GetSetSql, new { key }));
        }

        /// <inheritdoc />
        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore) =>
            this.GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();

        /// <inheritdoc />
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (keyValuePairs == null)
            {
                throw new ArgumentNullException(nameof(keyValuePairs));
            }

            const string MergeHashSql = "REPLACE INTO [Hash]([Key], [Field], [Value]) VALUES (@key, @field, @value)";

            using (this.lockedResources.AcquireHashLock())
            using (ITransaction transaction = this.storage.BeginTransaction())
            {
                foreach (KeyValuePair<string, string> pair in keyValuePairs)
                {
                    transaction.Execute(
                        MergeHashSql,
                        new
                        {
                            key,
                            field = pair.Key,
                            value = pair.Value
                        });
                }

                transaction.Commit();
            }
        }

        /// <inheritdoc />
        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string GetHashesSql = "SELECT Field, Value FROM [Hash] WHERE [Key]=@key";
            return this.storage.Query<SqliteHash>(GetHashesSql, new { key }).ToDictionary(x => x.Field, x => x.Value);
        }

        /// <inheritdoc />
        public override void Dispose()
        {
            if (this.lockedResources == null)
            {
                return;
            }

            base.Dispose();
            this.lockedResources.Dispose();
            this.lockedResources = null;
        }

        public override List<string> GetFirstByLowestScoreFromSet(
            string key,
            double fromScore,
            double toScore,
            int count)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (count <= 0)
            {
                throw new ArgumentException($"The `{nameof(count)}`value must be a positive number.");
            }

            if (toScore < fromScore)
            {
                throw new ArgumentException(
                    $"The `{nameof(toScore)}`value must be higher or equal than the `{nameof(fromScore)}` value.");
            }

            const string GetScoreSetSql =
                "SELECT Value FROM [Set] WHERE [Key]=@key AND Score BETWEEN @from AND @to ORDER BY Score LIMIT 1";

            return this.storage.Query<string>(
                GetScoreSetSql,
                new
                {
                    key,
                    from = fromScore,
                    to = toScore
                }).ToList();
        }

        /// <inheritdoc />
        public override long GetSetCount(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string CountSql = "SELECT COUNT(*) FROM [Set] WHERE [Key]=@key";

            return this.storage.ExecuteScalar<int>(CountSql, new { key });
        }

        /// <inheritdoc />
        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string RangeSql = "SELECT [Value] FROM [Set] WHERE [Key]=@key ORDER BY Id ASC " +
                                    "LIMIT @limit @OFFSET @offset";
            return this.storage.Query<string>(
                    RangeSql,
                    new
                    {
                        key,
                        limit = endingAt - startingFrom + 1,
                        offset = startingFrom
                    })
                .ToList();
        }

        /// <inheritdoc />
        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string QuerySql = "SELECT MIN([ExpireAt]) FROM [Set] WHERE [Key]=@key";
            var result = this.storage.ExecuteScalar<DateTime?>(QuerySql, new { key });
            if (!result.HasValue)
            {
                return TimeSpan.FromSeconds(-1);
            }

            return result.Value - DateTime.UtcNow;
        }

        /// <inheritdoc />
        public override string GetValueFromHash(string key, string name)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (name == null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            const string ValueQuery = "SELECT [Value] FROM [Hash] WHERE [Key]=@key AND [Field]=@field";
            return this.storage.ExecuteScalar<string>(
                ValueQuery,
                new
                {
                    key,
                    field = name
                });
        }

        /// <inheritdoc />
        public override long GetHashCount(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string CountSql = "SELECT COUNT(*) FROM [Hash] WHERE [Key]=@key";
            return this.storage.ExecuteScalar<long>(CountSql, new { key });
        }

        /// <inheritdoc />
        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string Query = "SELECT MIN([ExpireAt]) FROM [Hash] WHERE [Key]=@key";
            DateTime? result = this.storage.ExecuteScalar<DateTime?>(Query, new { key });
            if (!result.HasValue)
            {
                return TimeSpan.FromSeconds(-1);
            }

            return result.Value - DateTime.UtcNow;
        }

        /// <inheritdoc />
        public override long GetListCount(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string CountQuery = "SELECT COUNT(*) FROM [List] WHERE [Key]=@key";
            return this.storage.ExecuteScalar<long>(CountQuery, new { key });
        }

        /// <inheritdoc />
        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string Query = "SELECT [Value] FROM [List] WHERE [Key]=@key ORDER BY [Id] DESC";
            return this.storage.Query<string>(Query, new { key }).ToList();
        }

        /// <inheritdoc />
        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string Query = "SELECT [Value] FROM [List] WHERE [Key]=@key ORDER BY Id DESC" +
                                 " LIMIT @limit OFFSET @offset";
            return this.storage.Query<string>(
                    Query,
                    new
                    {
                        key,
                        limit = endingAt - startingFrom + 1,
                        offset = startingFrom
                    })
                .ToList();
        }

        /// <inheritdoc />
        public override TimeSpan GetListTtl(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string ListSql = "SELECT MIN([ExpireAt]) FROM [List] WHERE [Key]=@key";
            DateTime? result = this.storage.ExecuteScalar<DateTime?>(ListSql, new { key });
            if (!result.HasValue)
            {
                return TimeSpan.FromSeconds(-1);
            }

            return result.Value - DateTime.UtcNow;
        }

        /// <inheritdoc />
        public override long GetCounter(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string CounterSql = "SELECT SUM(s.[Value]) FROM (SELECT SUM([Value]) AS [Value] " +
                                      "FROM [Counter] WHERE [Key]=@key UNION ALL " +
                                      "SELECT [Value] FROM [AggregatedCounter] WHERE [Key]=@key) AS s";

            return this.storage.ExecuteScalar<long?>(CounterSql, new { key }) ?? 0;
        }
    }
}
