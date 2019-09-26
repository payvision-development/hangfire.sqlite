namespace Hangfire.Sqlite.Concurrency
{
    using System;
    using System.Collections.Generic;
    using System.Threading;

    /// <summary>
    /// Handles the multi-process locking of some resources.
    /// </summary>
    /// <remarks>
    /// <see cref="Mutex"/> is used due to a SQLite database can be opened by different
    /// processes, for example, if the application is running on an ASP.Net server with
    /// multiple processes.
    /// </remarks>
    internal sealed class LockedResources : IDisposable
    {
        private readonly Dictionary<string, (Mutex, HashSet<Guid>)>
            lockedResources = new Dictionary<string, (Mutex, HashSet<Guid>)>();

        private ReaderWriterLockSlim locker = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);

        ~LockedResources() => this.ReleaseUnmanagedResources();

        /// <inheritdoc />
        public void Dispose()
        {
            this.ReleaseUnmanagedResources();
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Gets a lock of the specified resource.
        /// </summary>
        /// <remarks>The lock will wait until is available so long as the specified <paramref name="timeout"/>.</remarks>
        /// <param name="resource">The resource to be locked.</param>
        /// <param name="timeout">The maximum waiting time allowed in order to get the lock.</param>
        /// <returns>The lock instance release when called <see cref="IDisposable.Dispose()"/>.</returns>
        /// <exception cref="TimeoutException">If the lock is not available before than the specified timeout.</exception>
        public IDisposable Lock(string resource, TimeSpan timeout)
        {
            if (resource == null)
            {
                throw new ArgumentNullException(nameof(resource));
            }

            Guid lockId = Guid.NewGuid();
            this.locker.EnterUpgradeableReadLock();
            try
            {
                if (!this.lockedResources.TryGetValue(resource, out (Mutex Mutex, HashSet<Guid> Pending) lockedResource))
                {
                    lockedResource = (new Mutex(false, resource), new HashSet<Guid>());
                    if (!lockedResource.Mutex.WaitOne(timeout))
                    {
                        throw new TimeoutException();
                    }

                    this.locker.EnterWriteLock();
                    try
                    {
                        this.lockedResources.Add(resource, lockedResource);
                    }
                    finally
                    {
                        this.locker.ExitWriteLock();
                    }
                }

                this.locker.EnterWriteLock();
                try
                {
                    lockedResource.Pending.Add(lockId);
                }
                finally
                {
                    this.locker.ExitWriteLock();
                }

                return new ReleaseDisposable(this, resource, lockId);
            }
            finally
            {
                this.locker.ExitUpgradeableReadLock();
            }
        }

        private void ReleaseUnmanagedResources()
        {
            if (this.locker == null)
            {
                return;
            }

            this.locker.EnterWriteLock();
            try
            {
                foreach ((Mutex mutex, HashSet<Guid> pending) in this.lockedResources.Values)
                {
                    pending.Clear();
                    mutex.Dispose();
                }

                this.lockedResources.Clear();
            }
            finally
            {
                this.locker.ExitWriteLock();
            }

            this.locker.Dispose();
            this.locker = null;
        }

        private sealed class ReleaseDisposable : IDisposable
        {
            private readonly LockedResources owner;

            private readonly string resource;

            private readonly Guid lockId;

            public ReleaseDisposable(LockedResources owner, string resource, Guid lockId)
            {
                this.owner = owner;
                this.resource = resource;
                this.lockId = lockId;
            }

            /// <inheritdoc />
            public void Dispose()
            {
                this.owner.locker.EnterUpgradeableReadLock();
                try
                {
                    if (!this.owner.lockedResources.TryGetValue(
                            this.resource,
                            out (Mutex Mutex, HashSet<Guid> Pending) lockedResource))
                    {
                        return;
                    }

                    this.owner.locker.EnterWriteLock();
                    try
                    {
                        if (lockedResource.Pending.Remove(this.lockId) && 
                            lockedResource.Pending.Count == 0 &&
                            this.owner.lockedResources.Remove(this.resource))
                        {
                            lockedResource.Mutex.ReleaseMutex();
                        }
                    }
                    finally
                    {
                        this.owner.locker.ExitWriteLock();
                    }
                }
                finally
                {
                    this.owner.locker.ExitUpgradeableReadLock();
                }
            }
        }
    }
}
