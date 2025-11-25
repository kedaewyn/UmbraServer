#nullable enable
using System.Globalization;
using MareSynchronosShared.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using UmbraSync.API.Dto.Group;
using UmbraSync.API.SignalR;
using Microsoft.AspNetCore.SignalR;
using UmbraSync.API.Data.Extensions;
using MareSynchronosServer.Hubs;
using MareSynchronosServer.Utils;

namespace MareSynchronosServer.Services.AutoDetect;
public sealed class AutoDetectScheduleService : BackgroundService
{
    private readonly ILogger<AutoDetectScheduleService> _logger;
    private readonly AutoDetectScheduleCache _cache;
    private readonly IDbContextFactory<MareDbContext> _dbContextFactory;
    private readonly IHubContext<MareHub, IMareHub> _hubContext;
    private readonly TimeProvider _timeProvider;

    public AutoDetectScheduleService(ILogger<AutoDetectScheduleService> logger,
        AutoDetectScheduleCache cache,
        IDbContextFactory<MareDbContext> dbContextFactory,
        IHubContext<MareHub, IMareHub> hubContext,
        TimeProvider timeProvider)
    {
        _logger = logger;
        _cache = cache;
        _dbContextFactory = dbContextFactory;
        _hubContext = hubContext;
        _timeProvider = timeProvider;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await TickAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "AutoDetect schedule tick failed");
            }

            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                
            }
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        var schedules = _cache.GetAll();
        if (schedules.Count == 0) return;

        await using var db = await _dbContextFactory.CreateDbContextAsync(ct).ConfigureAwait(false);
        foreach (var entry in schedules)
        {
            ct.ThrowIfCancellationRequested();
            var gid = entry.Key;
            var schedule = entry.Value;

            var desired = AutoDetectScheduleEvaluator.EvaluateDesiredVisibility(schedule, _timeProvider.GetUtcNow());
            if (!desired.HasValue) continue;

            var group = await db.Groups.SingleOrDefaultAsync(g => g.GID == gid, ct).ConfigureAwait(false);
            if (group == null) continue;

            if (group.AutoDetectVisible == desired.Value) continue;

            group.AutoDetectVisible = desired.Value;
            group.PasswordTemporarilyDisabled = desired.Value;
            await db.SaveChangesAsync(ct).ConfigureAwait(false);

            await db.Entry(group).Reference(g => g.Owner).LoadAsync(ct).ConfigureAwait(false);
            var groupPairs = await db.GroupPairs.AsNoTracking()
                .Where(p => p.GroupGID == group.GID)
                .Select(p => p.GroupUserUID)
                .ToListAsync(ct)
                .ConfigureAwait(false);
            await _hubContext.Clients.Users(groupPairs).Client_GroupSendInfo(
                new GroupInfoDto(
                    group.ToGroupData(),
                    group.Owner.ToUserData(),
                    group.GetGroupPermissions())
                {
                    IsTemporary = group.IsTemporary,
                    ExpiresAt = group.ExpiresAt,
                    AutoDetectVisible = group.AutoDetectVisible,
                    PasswordTemporarilyDisabled = group.PasswordTemporarilyDisabled,
                }).ConfigureAwait(false);

            _logger.LogInformation("AutoDetect schedule applied for {gid}: visible={visible}", gid, desired.Value);
        }
    }

    private static bool? EvaluateDesiredVisibility(AutoDetectScheduleCache.AutoDetectScheduleState schedule, DateTimeOffset nowUtc)
    {
        var recurring = schedule.Recurring
                        || (schedule.ActiveWeekdays != null && schedule.ActiveWeekdays.Length > 0)
                        || !string.IsNullOrWhiteSpace(schedule.TimeStartLocal)
                        || !string.IsNullOrWhiteSpace(schedule.TimeEndLocal);

        if (recurring)
        {
            if (schedule.ActiveWeekdays == null || schedule.ActiveWeekdays.Length == 0) return false;
            if (!TimeSpan.TryParse(schedule.TimeStartLocal, CultureInfo.InvariantCulture, out var start)) return false;
            if (!TimeSpan.TryParse(schedule.TimeEndLocal, CultureInfo.InvariantCulture, out var end)) return false;

            var tz = ResolveTimeZone(schedule.TimeZone);
            var nowLocal = TimeZoneInfo.ConvertTime(nowUtc, tz);
            var dayIndex = ((int)nowLocal.DayOfWeek + 6) % 7; // Monday=0
            if (!schedule.ActiveWeekdays.Contains(dayIndex)) return false;

            var todayStart = nowLocal.Date + start;
            var todayEnd = nowLocal.Date + end;
            if (end == start) return false; // zero window
            if (end < start)
            {
                
                return nowLocal >= todayStart || nowLocal < todayEnd;
            }

            return nowLocal >= todayStart && nowLocal < todayEnd;
        }
        else
        {
            if (!schedule.DisplayDurationHours.HasValue || !schedule.LastActivatedUtc.HasValue) return null;
            var endsAt = schedule.LastActivatedUtc.Value.AddHours(schedule.DisplayDurationHours.Value);
            return nowUtc <= endsAt;
        }
    }

    private static TimeZoneInfo ResolveTimeZone(string? timeZone)
    {
        if (!string.IsNullOrWhiteSpace(timeZone))
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById(timeZone);
            }
            catch
            {
                // fallback
            }
        }

        return TimeZoneInfo.Utc;
    }
}
