namespace Hangfire.Sqlite.Db
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// <see cref="ISession"/> extension methods.
    /// </summary>
    internal static class SessionExtensions
    {
        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(15);

        /// <summary>
        /// Begins a new transaction on the current session as default isolation level.
        /// </summary>
        /// <returns>The transaction session.</returns>
        public static ITransaction BeginTransaction(this ISession session) => session.BeginTransaction(null);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Set</c> resource.
        /// </summary>
        /// <param name="session">The session instance to perform the lock.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireSetLock(this ISession session) =>
            session.AcquireSetLock(DefaultTimeout);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Set</c> resource.
        /// </summary>
        /// <param name="session">The session instance to perform the lock.</param>
        /// <param name="timeout">The lock timeout.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireSetLock(this ISession session, TimeSpan timeout)
        {
            const string Resource = "Set:Lock";
            return session.Lock(Resource, timeout);
        }

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Lis</c> resource.
        /// </summary>
        /// <param name="session">The session instance to perform the lock.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireListLock(this ISession session) =>
            session.AcquireListLock(DefaultTimeout);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Lis</c> resource.
        /// </summary>
        /// <param name="session">The session instance to perform the lock.</param>
        /// <param name="timeout">The lock timeout.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireListLock(this ISession session, TimeSpan timeout)
        {
            const string Resource = "List:Lock";
            return session.Lock(Resource, timeout);
        }

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Hash</c> resource.
        /// </summary>
        /// <param name="session">The session instance to perform the lock.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireHashLock(this ISession session) =>
            session.AcquireHashLock(DefaultTimeout);

        /// <summary>
        /// Acquires a lock on the HangFire's <c>Hash</c> resource.
        /// </summary>
        /// <param name="session">The session instance to perform the lock.</param>
        /// <param name="timeout">The lock timeout.</param>
        /// <returns>The handler to release the lock.</returns>
        public static IDisposable AcquireHashLock(this ISession session, TimeSpan timeout)
        {
            const string Resource = "Hash:Lock";
            return session.Lock(Resource, timeout);
        }

        /// <summary>
        /// Performs all the specified locks bundle them on a single lock handler.
        /// </summary>
        /// <param name="session">The <see cref="ISession"/> to perform the locks.</param>
        /// <param name="locks">The lock actions to be performed.</param>
        /// <returns>The bundled lock handler to dispose all the locks at the same time.</returns>
        public static IDisposable LockAll(
            this ISession session,
            IEnumerable<Func<ISession, IDisposable>> locks)
        {
            if (session == null)
            {
                throw new ArgumentNullException(nameof(session));
            }

            if (locks == null)
            {
                throw new ArgumentNullException(nameof(locks));
            }

            return new LockBundle(locks.Select(@lock => @lock(session)));
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
