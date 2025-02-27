﻿namespace Hangfire.Sqlite
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    using Hangfire.Common;
    using Hangfire.Sqlite.Db;
    using Hangfire.Sqlite.Queues;
    using Hangfire.States;
    using Hangfire.Storage;

    /// <summary>
    /// <see cref="JobStorageTransaction"/> implementation using a <see cref="IJobStorage"/>.
    /// </summary>
    internal sealed class SqliteWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<ISession>> commandQueue = new Queue<Action<ISession>>();

        private readonly Queue<Action> afterCommitCommandQueue = new Queue<Action>();

        private readonly Queue<Func<ISession, IDisposable>> pendingLocks =
            new Queue<Func<ISession, IDisposable>>();

        private readonly IJobStorage storage;

        public SqliteWriteOnlyTransaction(IJobStorage storage) =>
            this.storage = storage ?? throw new ArgumentNullException(nameof(storage));

        /// <inheritdoc />
        public override void Commit()
        {
            using (this.storage.LockAll(this.pendingLocks))
            {
                using (ITransaction transaction = this.storage.BeginTransaction())
                {
                    foreach (Action<ISession> command in this.commandQueue)
                    {
                        command(transaction);
                    }

                    transaction.Commit();
                }

                foreach (Action command in this.afterCommitCommandQueue)
                {
                    command();
                }
            }
        }

        /// <inheritdoc />
        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            const string ExpireJobSql = "UPDATE [Job] SET ExpireAt=@expireAt WHERE Id=@id";
            this.EnqueueCommand(
                session => session.Execute(
                    ExpireJobSql,
                    new
                    {
                        id = long.Parse(jobId, CultureInfo.InvariantCulture),
                        expireAt = DateTime.UtcNow.Add(expireIn)
                    }));
        }

        /// <inheritdoc />
        public override void PersistJob(string jobId)
        {
            const string RenovateJobSql = "UPDATE [Job] SET ExpireAt=NULL WHERE Id=@id";
            this.EnqueueCommand(x => x.Execute(RenovateJobSql, new { id = long.Parse(jobId) }));
        }

        /// <inheritdoc />
        public override void SetJobState(string jobId, IState state)
        {
            const string AddAndSetStateSql = "INSERT INTO [State](JobId, Name, Reason, CreatedAt, Data) " +
                                             "VALUES (@jobId, @name, @reason, @createdAt, @data);" +
                                             "UPDATE Job SET StateId=last_insert_rowid(), StateName=@name WHERE ID=@jobId;";
            long id = long.Parse(jobId, CultureInfo.InvariantCulture);
            this.EnqueueCommand(
                repository => repository.Execute(
                    AddAndSetStateSql,
                    new
                    {
                        jobId = id,
                        name = state.Name,
                        reason = state.Reason,
                        createdAt = DateTime.UtcNow,
                        data = SerializationHelper.Serialize(state.SerializeData())
                    }));
        }

        /// <inheritdoc />
        public override void AddJobState(string jobId, IState state)
        {
            const string AddStateSql = "INSERT INTO [State](JobId, Name, Reason, CreatedAt, Data) " +
                                       "VALUES (@jobId, @name, @reason, @createdAt, @data)";

            this.EnqueueCommand(
                x => x.Execute(
                    AddStateSql,
                    new
                    {
                        jobId = long.Parse(jobId, CultureInfo.InvariantCulture),
                        name = state.Name,
                        reason = state.Reason?.Substring(0, Math.Min(99, state.Reason.Length)),
                        createdAt = DateTime.UtcNow,
                        data = SerializationHelper.Serialize(state.SerializeData())
                    }));
        }

        /// <inheritdoc />
        public override void AddToQueue(string queue, string jobId)
        {
            IPersistentJobQueue jobQueue = this.storage.QueueProviders[queue].JobQueue;
            this.EnqueueCommand(x => jobQueue.Enqueue(x, queue, jobId));
            if (jobQueue is SqliteJobQueue)
            {
                this.afterCommitCommandQueue.Enqueue(() => SqliteJobQueue.NewItemInQueueEvent.Set());
            }
        }

        /// <inheritdoc />
        public override void IncrementCounter(string key) => this.SetCounter(key, +1, null);

        /// <inheritdoc />
        public override void IncrementCounter(string key, TimeSpan expireIn) => this.SetCounter(key, +1, expireIn);

        /// <inheritdoc />
        public override void DecrementCounter(string key) => this.SetCounter(key, -1, null);

        /// <inheritdoc />
        public override void DecrementCounter(string key, TimeSpan expireIn) => this.SetCounter(key, -1, expireIn);

        /// <inheritdoc />
        public override void AddToSet(string key, string value) => this.AddToSet(key, value, 0d);

        /// <inheritdoc />
        public override void AddToSet(string key, string value, double score)
        {
            const string AddSetSql = "REPLACE INTO [Set]([Key], Value, Score) VALUES (@key, @value, @score)";

            this.AcquireSetLock();
            this.EnqueueCommand(
                x => x.Execute(
                    AddSetSql,
                    new
                    {
                        key,
                        value,
                        score
                    }));
        }

        /// <inheritdoc />
        public override void RemoveFromSet(string key, string value)
        {
            const string DeleteSetSql = "DELETE FROM [Set] WHERE [Key]=@key AND [Value]=@value";

            this.AcquireSetLock();
            this.EnqueueCommand(
                session => session.Execute(
                    DeleteSetSql,
                    new
                    {
                        key,
                        value
                    }));
        }

        /// <inheritdoc />
        public override void InsertToList(string key, string value)
        {
            const string InsertListSql = "INSERT INTO [List]([Key], [Value]) VALUES (@key, @value)";

            this.AcquireListLock();
            this.EnqueueCommand(
                session => session.Execute(
                    InsertListSql,
                    new
                    {
                        key,
                        value
                    }));
        }

        /// <inheritdoc />
        public override void RemoveFromList(string key, string value)
        {
            const string DeleteListSql = "DELETE FROM [List] WHERE [Key]=@key AND [Value]=@value";

            this.AcquireListLock();
            this.EnqueueCommand(
                session => session.Execute(
                    DeleteListSql,
                    new
                    {
                        key,
                        value
                    }));
        }

        /// <inheritdoc />
        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            const string TrimSql = "DELETE FROM [List] WHERE [Key]=@key AND Id NOT IN (" +
                                   "SELECT Id FROM [List] WHERE [Key]=@key ORDER BY Id DESC " +
                                   "LIMIT @limit OFFSET @offset";

            this.AcquireListLock();
            this.EnqueueCommand(
                session => session.Execute(
                    TrimSql,
                    new
                    {
                        key,
                        limit = keepEndingAt - keepStartingFrom + 1,
                        offset = keepStartingFrom
                    }));
        }

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

            const string MergeHashSql = "REPLACE INTO [Hash](Key, Field, Value) VALUES (@key, @field, @value)";

            this.AcquireHashLock();
            foreach (KeyValuePair<string, string> pair in keyValuePairs)
            {
                this.EnqueueCommand(
                    session => session.Execute(
                        MergeHashSql,
                        new
                        {
                            key,
                            field = pair.Key,
                            value = pair.Value
                        }));
            }
        }

        /// <inheritdoc />
        public override void RemoveHash(string key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            const string RemoveHashSql = "DELETE FROM [Hash] WHERE [Key]=@key";
            this.AcquireHashLock();
            this.EnqueueCommand(session => session.Execute(RemoveHashSql, new { key }));
        }

        private void EnqueueCommand(Action<ISession> command) => this.commandQueue.Enqueue(command);

        private void SetCounter(string key, int value, TimeSpan? expireIn)
        {
            const string IncrementCounterSql = "INSERT INTO [Counter]([Key], [Value], [ExpireAt])" +
                                               " VALUES (@key, @value, @expireAt)";
            this.EnqueueCommand(
                session => session.Execute(
                    IncrementCounterSql,
                    new
                    {
                        key,
                        value,
                        expireAt = expireIn.HasValue ? DateTime.UtcNow.Add(expireIn.Value) : (DateTime?)null
                    }));
        }

        private void AcquireSetLock() => this.pendingLocks.Enqueue(x => x.AcquireSetLock());

        private void AcquireListLock() => this.pendingLocks.Enqueue(x => x.AcquireListLock());

        private void AcquireHashLock() => this.pendingLocks.Enqueue(x => x.AcquireHashLock());
    }
}