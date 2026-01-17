using MareSynchronosShared.Metrics;

namespace MareSynchronosServer.Services;

public class OnlineSyncedPairCacheService
{
    private readonly Dictionary<string, PairCache> _userCaches = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _cacheModificationSemaphore = new(1);
    private readonly ILogger<OnlineSyncedPairCacheService> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly MareMetrics _mareMetrics;

    public OnlineSyncedPairCacheService(
        ILogger<OnlineSyncedPairCacheService> logger,
        ILoggerFactory loggerFactory,
        MareMetrics mareMetrics)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _mareMetrics = mareMetrics;
    }

    public async Task InitPlayer(string userUid)
    {
        if (_userCaches.ContainsKey(userUid)) return;

        await _cacheModificationSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_userCaches.ContainsKey(userUid)) return;

            _logger.LogDebug("PairCache:Init {UserUid}", userUid);
            _userCaches[userUid] = new PairCache(
                _loggerFactory.CreateLogger<PairCache>(),
                userUid,
                _mareMetrics);

            _mareMetrics.IncGauge(MetricsAPI.GaugePairCacheUsers);
        }
        finally
        {
            _cacheModificationSemaphore.Release();
        }
    }

    public async Task DisposePlayer(string userUid)
    {
        if (!_userCaches.ContainsKey(userUid)) return;

        await _cacheModificationSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_userCaches.Remove(userUid, out var pairCache))
            {
                _logger.LogDebug("PairCache:Dispose {UserUid}", userUid);
                pairCache.Dispose();
                _mareMetrics.DecGauge(MetricsAPI.GaugePairCacheUsers);
            }
        }
        finally
        {
            _cacheModificationSemaphore.Release();
        }
    }

    public async Task<bool> AreAllPlayersCached(string senderUid, List<string> recipientUids, CancellationToken ct)
    {
        if (!_userCaches.ContainsKey(senderUid))
        {
            await InitPlayer(senderUid).ConfigureAwait(false);
        }

        if (_userCaches.TryGetValue(senderUid, out var pairCache))
        {
            return await pairCache.AreAllPlayersCached(recipientUids, ct).ConfigureAwait(false);
        }

        return false;
    }

    public async Task CachePlayers(string senderUid, List<string> validPairUids, CancellationToken ct)
    {
        if (!_userCaches.ContainsKey(senderUid))
        {
            await InitPlayer(senderUid).ConfigureAwait(false);
        }

        if (_userCaches.TryGetValue(senderUid, out var pairCache))
        {
            await pairCache.CachePlayers(validPairUids, ct).ConfigureAwait(false);
        }
    }

    public int GetCachedUserCount() => _userCaches.Count;

    private sealed class PairCache : IDisposable
    {
        private readonly ILogger<PairCache> _logger;
        private readonly string _ownerUid;
        private readonly MareMetrics _metrics;
        private readonly Dictionary<string, DateTime> _cachedPairs = new(StringComparer.Ordinal);
        private readonly SemaphoreSlim _lock = new(1);

        private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(60);

        public PairCache(ILogger<PairCache> logger, string ownerUid, MareMetrics metrics)
        {
            _logger = logger;
            _ownerUid = ownerUid;
            _metrics = metrics;
        }

        public async Task<bool> AreAllPlayersCached(List<string> uids, CancellationToken ct)
        {
            await _lock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var now = DateTime.UtcNow;
                var allCached = uids.TrueForAll(uid =>
                    _cachedPairs.TryGetValue(uid, out var expiry) && expiry > now);

                _logger.LogDebug("PairCache:Check {Owner} recipients={Count} cached={Cached}",
                    _ownerUid, uids.Count, allCached);

                if (allCached)
                    _metrics.IncCounter(MetricsAPI.CounterPairCacheHit);
                else
                    _metrics.IncCounter(MetricsAPI.CounterPairCacheMiss);

                return allCached;
            }
            finally
            {
                _lock.Release();
            }
        }

        public async Task CachePlayers(List<string> uids, CancellationToken ct)
        {
            await _lock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var expiry = DateTime.UtcNow.Add(CacheDuration);
                var newEntries = 0;

                foreach (var uid in uids)
                {
                    if (!_cachedPairs.ContainsKey(uid))
                        newEntries++;

                    _cachedPairs[uid] = expiry;
                }

                _logger.LogDebug("PairCache:Update {Owner} total={Total} new={New}",
                    _ownerUid, uids.Count, newEntries);

                _metrics.IncGauge(MetricsAPI.GaugePairCacheEntries, newEntries);

                var expiredKeys = _cachedPairs
                    .Where(kvp => kvp.Value < DateTime.UtcNow)
                    .Select(kvp => kvp.Key)
                    .ToList();

                if (expiredKeys.Count > 0)
                {
                    foreach (var key in expiredKeys)
                    {
                        _cachedPairs.Remove(key);
                    }
                    _metrics.DecGauge(MetricsAPI.GaugePairCacheEntries, expiredKeys.Count);

                    _logger.LogDebug("PairCache:Cleanup {Owner} removed={Removed}",
                        _ownerUid, expiredKeys.Count);
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        public void Dispose()
        {
            _metrics.DecGauge(MetricsAPI.GaugePairCacheEntries, _cachedPairs.Count);
            _lock.Dispose();
        }
    }
}