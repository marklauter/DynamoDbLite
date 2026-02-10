namespace DynamoDbLite.SqlteStores;

internal sealed class AsyncReaderWriterLock
    : IDisposable
{
    private readonly SemaphoreSlim readerSemaphore = new(1, 1);
    private readonly SemaphoreSlim writerSemaphore = new(1, 1);
    private int readerCount;

    internal async ValueTask<IDisposable?> AcquireReadLockAsync(CancellationToken cancellationToken = default)
    {
        await readerSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (++readerCount == 1)
                await writerSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = readerSemaphore.Release();
        }

        return new ReadReleaser(this);
    }

    internal async ValueTask<IDisposable?> AcquireWriteLockAsync(CancellationToken cancellationToken = default)
    {
        await writerSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        return new WriteReleaser(this);
    }

    private void ReleaseReadLock()
    {
        readerSemaphore.Wait();
        try
        {
            if (--readerCount == 0)
                _ = writerSemaphore.Release();
        }
        finally
        {
            _ = readerSemaphore.Release();
        }
    }

    private void ReleaseWriteLock() => _ = writerSemaphore.Release();

    public void Dispose()
    {
        readerSemaphore.Dispose();
        writerSemaphore.Dispose();
    }

    private sealed class ReadReleaser(AsyncReaderWriterLock rwLock)
        : IDisposable
    {
        public void Dispose() => rwLock.ReleaseReadLock();
    }

    private sealed class WriteReleaser(AsyncReaderWriterLock rwLock)
        : IDisposable
    {
        public void Dispose() => rwLock.ReleaseWriteLock();
    }
}
