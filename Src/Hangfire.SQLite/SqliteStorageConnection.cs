﻿namespace Hangfire.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading;

    using Hangfire.Common;
    using Hangfire.Server;
    using Hangfire.Sqlite.Db;
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
        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken) => throw new NotImplementedException();

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
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void RemoveServer(string serverId)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void Heartbeat(string serverId)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public int RemoveTimedOutServers(TimeSpan timeOut) => throw new NotImplementedException();

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
