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

    internal sealed class SqliteStorageConnection : IStorageConnection
    {
        private readonly IJobStorage storage;

        private LockedResources lockedResources = new LockedResources();

        public SqliteStorageConnection(IJobStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        /// <inheritdoc />
        public IWriteOnlyTransaction CreateWriteTransaction() =>
            new SqliteWriteOnlyTransaction(this.storage, this.lockedResources);

        /// <inheritdoc />
        public IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            if (string.IsNullOrWhiteSpace(resource))
            {
                throw new ArgumentNullException(nameof(resource));
            }

            return this.lockedResources.Lock(resource, timeout);
        }

        /// <inheritdoc />
        public string CreateExpiredJob(
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
        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
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
        public void SetJobParameter(string id, string name, string value)
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
        public string GetJobParameter(string id, string name)
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
        public JobData GetJobData(string jobId)
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
        public StateData GetStateData(string jobId) => throw new NotImplementedException();

        /// <inheritdoc />
        public void AnnounceServer(string serverId, ServerContext context)
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
        public void RemoveServer(string serverId)
        {
            if (serverId == null)
            {
                throw new ArgumentNullException(nameof(serverId));
            }

            const string RemoveServerSql = "DELETE FROM [Server] WHERE Id=@id";
            this.storage.Execute(RemoveServerSql, new { id = serverId });
        }

        /// <inheritdoc />
        public void Heartbeat(string serverId)
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
        public int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException($"The `{nameof(timeOut)}`value must be positive.", nameof(timeOut));
            }

            const string RemoveServerSql = "DELETE FROM [Server] WHERE LastHeartbeat < @timeOutAt";

            return this.storage.Execute(RemoveServerSql, new { timeOutAt = DateTime.UtcNow.Add(timeOut.Negate()) });
        }

        /// <inheritdoc />
        public HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string GetSetSql = "SELECT Value FROM [Set] WHERE [Key] = @key";
            return new HashSet<string>(this.storage.Query<string>(GetSetSql, new { key }));
        }

        /// <inheritdoc />
        public string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore) =>
            this.GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();

        /// <inheritdoc />
        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public Dictionary<string, string> GetAllEntriesFromHash(string key) => throw new NotImplementedException();

        /// <inheritdoc />
        public void Dispose()
        {
            if (this.lockedResources == null)
            {
                return;
            }

            this.lockedResources.Dispose();
            this.lockedResources = null;
        }

        private IEnumerable<string> GetFirstByLowestScoreFromSet(
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
                });
        }
    }
}
