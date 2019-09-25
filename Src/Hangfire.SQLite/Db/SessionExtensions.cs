namespace Hangfire.Sqlite.Db
{
    /// <summary>
    /// Database session extension methods.
    /// </summary>
    internal static class SessionExtensions
    {
        /// <summary>
        /// Begins a new transaction on the current session as default isolation level.
        /// </summary>
        /// <returns>The transaction session.</returns>
        public static ITransaction BeginTransaction(this ISession session) => session.BeginTransaction(null);
    }
}
