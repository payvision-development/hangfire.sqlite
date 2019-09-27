namespace Hangfire.Sqlite.Concurrency
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// <see cref="ILockedResources"/> extension methods.
    /// </summary>
    internal static class LockedResourcesExtensions
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Set</c> resource.
        /// </summary>
        /// <param name="lockedResources">The locked resources instance to perform the lock.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireSetLock(this ILockedResources lockedResources) =>
            lockedResources.AcquireSetLock(DefaultTimeout);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Set</c> resource.
        /// </summary>
        /// <param name="lockedResources">The locked resources instance to perform the lock.</param>
        /// <param name="timeout">The lock timeout.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireSetLock(this ILockedResources lockedResources, TimeSpan timeout)
        {
            const string Resource = "Set:Lock";
            return lockedResources.Lock(Resource, timeout);
        }

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Lis</c> resource.
        /// </summary>
        /// <param name="lockedResources">The locked resources instance to perform the lock.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireListLock(this ILockedResources lockedResources) =>
            lockedResources.AcquireListLock(DefaultTimeout);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Lis</c> resource.
        /// </summary>
        /// <param name="lockedResources">The locked resources instance to perform the lock.</param>
        /// <param name="timeout">The lock timeout.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireListLock(this ILockedResources lockedResources, TimeSpan timeout)
        {
            const string Resource = "List:Lock";
            return lockedResources.Lock(Resource, timeout);
        }

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Hash</c> resource.
        /// </summary>
        /// <param name="lockedResources">The locked resources instance to perform the lock.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireHashLock(this ILockedResources lockedResources) =>
            lockedResources.AcquireHashLock(DefaultTimeout);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Hash</c> resource.
        /// </summary>
        /// <param name="lockedResources">The locked resources instance to perform the lock.</param>
        /// <param name="timeout">The lock timeout.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireHashLock(this ILockedResources lockedResources, TimeSpan timeout)
        {
            const string Resource = "Hash:Lock";
            return lockedResources.Lock(Resource, timeout);
        }

        /// <summary>
        /// Performs all the specified locks bundle them on a single lock handler.
        /// </summary>
        /// <param name="lockedResources">The <see cref="ILockedResources"/> to perform the locks.</param>
        /// <param name="locks">The lock actions to be performed.</param>
        /// <returns>The bundled lock handler to dispose all the locks at the same time.</returns>
        public static IDisposable LockAll(
            this ILockedResources lockedResources,
            IEnumerable<Func<ILockedResources, IDisposable>> locks)
        {
            if (lockedResources == null)
            {
                throw new ArgumentNullException(nameof(lockedResources));
            }

            if (locks == null)
            {
                throw new ArgumentNullException(nameof(locks));
            }

            return new LockBundle(locks.Select(@lock => @lock(lockedResources)));
        }

        private sealed class LockBundle : IDisposable
        {
            private readonly IDisposable[] lockHandlers;

            public LockBundle(IEnumerable<IDisposable> lockHandlers) => this.lockHandlers = lockHandlers.ToArray();

            /// <inheritdoc />
            public void Dispose()
            {
                foreach (IDisposable lockHandler in this.lockHandlers)
                {
                    lockHandler.Dispose();
                }
            }
        }
    }
}
