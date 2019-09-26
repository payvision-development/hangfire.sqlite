namespace Hangfire.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;

    using Hangfire.Common;
    using Hangfire.Server;
    using Hangfire.Sqlite.Db;
    using Hangfire.Sqlite.Entities;
    using Hangfire.Sqlite.Queues;
    using Hangfire.Storage;

    internal sealed class SqliteStorageConnection : IStorageConnection
    {
        private readonly IJobStorage storage;

        public SqliteStorageConnection(IJobStorage storage)
        {
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        /// <inheritdoc />
        public IWriteOnlyTransaction CreateWriteTransaction() => new SqliteWriteOnlyTransaction(this.storage);

        /// <inheritdoc />
        public IDisposable AcquireDistributedLock(string resource, TimeSpan timeout) => throw new NotImplementedException();

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
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public string GetJobParameter(string id, string name) => throw new NotImplementedException();

        /// <inheritdoc />
        public JobData GetJobData(string jobId) => throw new NotImplementedException();

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
        public HashSet<string> GetAllItemsFromSet(string key) => throw new NotImplementedException();

        /// <inheritdoc />
        public string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore) => throw new NotImplementedException();

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
        }
    }
}
