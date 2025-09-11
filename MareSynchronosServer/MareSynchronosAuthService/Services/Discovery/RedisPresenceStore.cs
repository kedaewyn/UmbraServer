using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace MareSynchronosAuthService.Services.Discovery;

public sealed class RedisPresenceStore : IDiscoveryPresenceStore
{
    private readonly ILogger<RedisPresenceStore> _logger;
    private readonly IDatabase _db;
    private readonly TimeSpan _presenceTtl;
    private readonly TimeSpan _tokenTtl;
    private readonly JsonSerializerOptions _jsonOpts = new() { DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull };

    public RedisPresenceStore(ILogger<RedisPresenceStore> logger, IConnectionMultiplexer mux, TimeSpan presenceTtl, TimeSpan tokenTtl)
    {
        _logger = logger;
        _db = mux.GetDatabase();
        _presenceTtl = presenceTtl;
        _tokenTtl = tokenTtl;
    }

    public void Dispose() { }

    private static string KeyForHash(string hash) => $"nd:hash:{hash}";
    private static string KeyForToken(string token) => $"nd:token:{token}";

    public void Publish(string uid, IEnumerable<string> hashes, string? displayName = null)
    {
        var entries = hashes.Distinct(StringComparer.Ordinal).ToArray();
        if (entries.Length == 0) return;
        var batch = _db.CreateBatch();
        foreach (var h in entries)
        {
            var key = KeyForHash(h);
            var payload = JsonSerializer.Serialize(new Presence(uid, displayName), _jsonOpts);
            batch.StringSetAsync(key, payload, _presenceTtl);
        }
        batch.Execute();
        _logger.LogDebug("RedisPresenceStore: published {count} hashes", entries.Length);
    }

    public (bool Found, string Token, string? DisplayName) TryMatchAndIssueToken(string requesterUid, string hash)
    {
        var key = KeyForHash(hash);
        var val = _db.StringGet(key);
        if (!val.HasValue) return (false, string.Empty, null);
        try
        {
            var p = JsonSerializer.Deserialize<Presence>(val!);
            if (p == null || string.IsNullOrEmpty(p.Uid)) return (false, string.Empty, null);
            if (string.Equals(p.Uid, requesterUid, StringComparison.Ordinal)) return (false, string.Empty, null);
            var token = Guid.NewGuid().ToString("N");
            _db.StringSet(KeyForToken(token), p.Uid, _tokenTtl);
            return (true, token, p.DisplayName);
        }
        catch
        {
            return (false, string.Empty, null);
        }
    }

    public bool ValidateToken(string token, out string targetUid)
    {
        targetUid = string.Empty;
        var key = KeyForToken(token);
        var val = _db.StringGet(key);
        if (!val.HasValue) return false;
        targetUid = val!;
        return true;
    }

    private sealed record Presence(string Uid, string? DisplayName);
}

