namespace Hangfire.Sqlite.Db
{
    using System;

    /// <summary>
    /// Allows the reading of multiple fetch.
    /// </summary>
    internal interface IGridReader : IDisposable
    {
        T ReadSingle<T>();

        T ReadSingleOrDefault<T>();
    }
}
