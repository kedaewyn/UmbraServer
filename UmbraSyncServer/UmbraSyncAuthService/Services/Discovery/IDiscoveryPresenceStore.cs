using System.Collections.Concurrent;

namespace MareSynchronosAuthService.Services.Discovery;

public interface IDiscoveryPresenceStore : IDisposable
{
    void Publish(string uid, IEnumerable<string> hashes, string? displayName = null, bool allowRequests = true);
    void Unpublish(string uid);
    (bool Found, string? Token, string TargetUid, string? DisplayName) TryMatchAndIssueToken(string requesterUid, string hash);
    bool ValidateToken(string token, out string targetUid);
}

