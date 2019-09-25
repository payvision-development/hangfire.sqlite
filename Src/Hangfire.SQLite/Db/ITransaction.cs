namespace Hangfire.Sqlite.Db
{
    using System;

    internal interface ITransaction : ISession, IDisposable
    {
        /// <summary>
        /// Commit all pending changes succeeded on the current session.
        /// </summary>
        void Commit();

        /// <summary>
        /// Rollback all pending changes on the current session.
        /// </summary>
        void Rollback();
    }
}
