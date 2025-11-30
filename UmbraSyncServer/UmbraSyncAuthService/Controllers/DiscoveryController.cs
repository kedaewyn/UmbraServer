using System.Text.Json.Serialization;
using MareSynchronosAuthService.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace MareSynchronosAuthService.Controllers;

[Authorize]
[ApiController]
[Route("discovery")]
public class DiscoveryController : Controller
{
    private readonly DiscoveryWellKnownProvider _provider;
    private readonly DiscoveryPresenceService _presence;
    private readonly IConfiguration _configuration;

    public DiscoveryController(DiscoveryWellKnownProvider provider, DiscoveryPresenceService presence, IConfiguration configuration)
    {
        _provider = provider;
        _presence = presence;
        _configuration = configuration;
    }

    public sealed class QueryRequest
    {
        [JsonPropertyName("hashes")] public string[] Hashes { get; set; } = Array.Empty<string>();
        [JsonPropertyName("salt")] public string SaltB64 { get; set; } = string.Empty;
    }

    public sealed class QueryResponseEntry
    {
        [JsonPropertyName("hash")] public string Hash { get; set; } = string.Empty;
        [JsonPropertyName("token")] public string? Token { get; set; }
        [JsonPropertyName("uid")] public string Uid { get; set; } = string.Empty;
        [JsonPropertyName("displayName")] public string? DisplayName { get; set; }
    }

    [HttpPost("query")]
    public IActionResult Query([FromBody] QueryRequest req)
    {
        if (_provider.IsExpired(req.SaltB64))
        {
            return BadRequest(new { code = "DISCOVERY_SALT_EXPIRED" });
        }

        var uid = User?.Claims?.FirstOrDefault(c => c.Type == MareSynchronosShared.Utils.MareClaimTypes.Uid)?.Value ?? string.Empty;
        if (string.IsNullOrEmpty(uid) || req?.Hashes == null || req.Hashes.Length == 0)
            return Json(Array.Empty<QueryResponseEntry>());

        List<QueryResponseEntry> matches = new();
        foreach (var h in req.Hashes.Distinct(StringComparer.Ordinal))
        {
            var (found, token, targetUid, displayName) = _presence.TryMatchAndIssueToken(uid, h);
            if (found)
            {
                matches.Add(new QueryResponseEntry { Hash = h, Token = token, Uid = targetUid, DisplayName = displayName });
            }
        }

        return Json(matches);
    }

    public sealed class RequestDto
    {
        [JsonPropertyName("token")] public string Token { get; set; } = string.Empty;
        [JsonPropertyName("displayName")] public string? DisplayName { get; set; }
    }

    [HttpPost("request")]
    public async Task<IActionResult> RequestPair([FromBody] RequestDto req)
    {
        if (string.IsNullOrEmpty(req.Token)) return BadRequest();
        if (_presence.ValidateToken(req.Token, out var targetUid))
        {
            // Phase 3 (minimal): notify target via mare-server internal controller
            try
            {
                var fromUid = User?.Claims?.FirstOrDefault(c => c.Type == MareSynchronosShared.Utils.MareClaimTypes.Uid)?.Value ?? string.Empty;
                var fromAlias = string.IsNullOrEmpty(req.DisplayName)
                    ? (User?.Claims?.FirstOrDefault(c => c.Type == MareSynchronosShared.Utils.MareClaimTypes.Alias)?.Value ?? string.Empty)
                    : req.DisplayName;

                using var http = new HttpClient();
                try { http.DefaultRequestHeaders.UserAgent.ParseAdd("UmbraAuthService/1.0"); } catch { /* ignore */ }
                // Prefer configured Main base URL if present; fallback to incoming host (nginx)
                var configuredBase = _configuration.GetValue<string>("NearbyDiscovery:MainBaseUrl");
                var baseUrl = string.IsNullOrWhiteSpace(configuredBase) ? $"{Request.Scheme}://{Request.Host.Value}" : configuredBase;
                var url = new Uri(new Uri(baseUrl), "/main/discovery/notifyRequest");

                // Generate internal JWT
                var serverToken = HttpContext.RequestServices.GetRequiredService<MareSynchronosShared.Utils.ServerTokenGenerator>().Token;
                http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", serverToken);
                var payload = System.Text.Json.JsonSerializer.Serialize(new { targetUid, fromUid, fromAlias });
                var resp = await http.PostAsync(url, new StringContent(payload, System.Text.Encoding.UTF8, "application/json"));
                if (!resp.IsSuccessStatusCode)
                {
                    var txt = await resp.Content.ReadAsStringAsync();
                    HttpContext.RequestServices.GetRequiredService<ILogger<DiscoveryController>>()
                        .LogWarning("notifyRequest failed: {code} {reason} {body}", (int)resp.StatusCode, resp.ReasonPhrase, txt);
                }
            }
            catch { /* ignore */ }

            return Accepted();
        }
        return BadRequest(new { code = "INVALID_TOKEN" });
    }

    public sealed class AcceptNotifyDto
    {
        [JsonPropertyName("targetUid")] public string TargetUid { get; set; } = string.Empty;
        [JsonPropertyName("displayName")] public string? DisplayName { get; set; }
    }

    // Accept notification relay (sender -> auth -> main)
    [HttpPost("acceptNotify")]
    public async Task<IActionResult> AcceptNotify([FromBody] AcceptNotifyDto req)
    {
        if (string.IsNullOrEmpty(req.TargetUid)) return BadRequest();
        try
        {
            var fromUid = User?.Claims?.FirstOrDefault(c => c.Type == MareSynchronosShared.Utils.MareClaimTypes.Uid)?.Value ?? string.Empty;
            var fromAlias = string.IsNullOrEmpty(req.DisplayName)
                ? (User?.Claims?.FirstOrDefault(c => c.Type == MareSynchronosShared.Utils.MareClaimTypes.Alias)?.Value ?? string.Empty)
                : req.DisplayName;

            using var http = new HttpClient();
            try { http.DefaultRequestHeaders.UserAgent.ParseAdd("UmbraAuthService/1.0"); } catch { /* ignore */ }
            var configuredBase = _configuration.GetValue<string>("NearbyDiscovery:MainBaseUrl");
            var baseUrl = string.IsNullOrWhiteSpace(configuredBase) ? $"{Request.Scheme}://{Request.Host.Value}" : configuredBase;
            var url = new Uri(new Uri(baseUrl), "/main/discovery/notifyAccept");
            var serverToken = HttpContext.RequestServices.GetRequiredService<MareSynchronosShared.Utils.ServerTokenGenerator>().Token;
            http.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", serverToken);
            var payload = System.Text.Json.JsonSerializer.Serialize(new { targetUid = req.TargetUid, fromUid, fromAlias });
            var resp = await http.PostAsync(url, new StringContent(payload, System.Text.Encoding.UTF8, "application/json"));
            if (!resp.IsSuccessStatusCode)
            {
                var txt = await resp.Content.ReadAsStringAsync();
                HttpContext.RequestServices.GetRequiredService<ILogger<DiscoveryController>>()
                    .LogWarning("notifyAccept failed: {code} {reason} {body}", (int)resp.StatusCode, resp.ReasonPhrase, txt);
            }
        }
        catch { /* ignore */ }

        return Accepted();
    }

    public sealed class PublishRequest
    {
        [JsonPropertyName("hashes")] public string[] Hashes { get; set; } = Array.Empty<string>();
        [JsonPropertyName("displayName")] public string? DisplayName { get; set; }
        [JsonPropertyName("salt")] public string SaltB64 { get; set; } = string.Empty;
        [JsonPropertyName("allowRequests")] public bool AllowRequests { get; set; } = true;
    }

    [HttpPost("disable")]
    public IActionResult Disable()
    {
        var uid = User?.Claims?.FirstOrDefault(c => c.Type == MareSynchronosShared.Utils.MareClaimTypes.Uid)?.Value ?? string.Empty;
        if (string.IsNullOrEmpty(uid)) return Accepted();
        _presence.Unpublish(uid);
        return Accepted();
    }

    [HttpPost("publish")]
    public IActionResult Publish([FromBody] PublishRequest req)
    {
        if (_provider.IsExpired(req.SaltB64))
        {
            return BadRequest(new { code = "DISCOVERY_SALT_EXPIRED" });
        }
        var uid = User?.Claims?.FirstOrDefault(c => c.Type == MareSynchronosShared.Utils.MareClaimTypes.Uid)?.Value ?? string.Empty;
        if (string.IsNullOrEmpty(uid) || req?.Hashes == null || req.Hashes.Length == 0)
            return Accepted();

        _presence.Publish(uid, req.Hashes, req.DisplayName, req.AllowRequests);
        return Accepted();
    }
}
