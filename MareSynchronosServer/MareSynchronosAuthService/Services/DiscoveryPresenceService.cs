using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MareSynchronosAuthService.Services.Discovery;

namespace MareSynchronosAuthService.Services;

public class DiscoveryPresenceService : IHostedService, IDisposable
{
    private readonly ILogger<DiscoveryPresenceService> _logger;
    private readonly IDiscoveryPresenceStore _store;
    private readonly TimeSpan _presenceTtl = TimeSpan.FromMinutes(5);
    private readonly TimeSpan _tokenTtl = TimeSpan.FromMinutes(2);

    public DiscoveryPresenceService(ILogger<DiscoveryPresenceService> logger, IDiscoveryPresenceStore store)
    {
        _logger = logger;
        _store = store;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public void Publish(string uid, IEnumerable<string> hashes, string? displayName = null)
    {
        _store.Publish(uid, hashes, displayName);
        _logger.LogDebug("Discovery presence published for {uid} with {n} hashes", uid, hashes.Count());
    }

    public (bool Found, string Token, string? DisplayName) TryMatchAndIssueToken(string requesterUid, string hash)
    {
        return _store.TryMatchAndIssueToken(requesterUid, hash);
    }

    public bool ValidateToken(string token, out string targetUid)
    {
        return _store.ValidateToken(token, out targetUid);
    }

    public void Dispose()
    {
        (_store as IDisposable)?.Dispose();
    }
}
