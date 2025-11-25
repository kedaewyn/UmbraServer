#nullable enable
using System.Text.Json;
using MareSynchronosShared.Data;
using MareSynchronosShared.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace MareSynchronosServer.Services.AutoDetect;
public sealed class AutoDetectScheduleCache
{
    public sealed record AutoDetectScheduleState(
        bool Recurring,
        int? DisplayDurationHours,
        int[]? ActiveWeekdays,
        string? TimeStartLocal,
        string? TimeEndLocal,
        string? TimeZone,
        DateTime? LastActivatedUtc);

    private sealed record CachedSchedule(
        bool Recurring,
        int? DisplayDurationHours,
        int[]? ActiveWeekdays,
        string? TimeStartLocal,
        string? TimeEndLocal,
        string? TimeZone,
        DateTime? LastActivatedUtc);

    private readonly ILogger<AutoDetectScheduleCache> _logger;
    private readonly IDbContextFactory<MareDbContext> _dbContextFactory;
    private readonly string _legacyCachePath;
    private readonly object _syncRoot = new();
    private Dictionary<string, CachedSchedule> _cache = new(StringComparer.OrdinalIgnoreCase);

    public AutoDetectScheduleCache(ILogger<AutoDetectScheduleCache> logger, IDbContextFactory<MareDbContext> dbContextFactory)
    {
        _logger = logger;
        _dbContextFactory = dbContextFactory;
        _legacyCachePath = Path.Combine(AppContext.BaseDirectory, "data", "autodetect_schedule_cache.json");
        Load();
    }

    public AutoDetectScheduleState? Get(string gid)
    {
        lock (_syncRoot)
        {
            if (!_cache.TryGetValue(gid, out var entry)) return null;

            return ToState(entry);
        }
    }

    public IReadOnlyDictionary<string, AutoDetectScheduleState> GetAll()
    {
        lock (_syncRoot)
        {
            return _cache.ToDictionary(k => k.Key, v => ToState(v.Value), StringComparer.OrdinalIgnoreCase);
        }
    }

    public void Clear(string gid)
    {
        lock (_syncRoot)
        {
            _cache.Remove(gid);
            try
            {
                using var db = _dbContextFactory.CreateDbContext();
                var entity = db.AutoDetectSchedules.SingleOrDefault(s => s.GroupGID == gid);
                if (entity != null)
                {
                    db.AutoDetectSchedules.Remove(entity);
                    var affected = db.SaveChanges();
                    _logger.LogInformation("[AutoDetectCache] CLEARED gid={gid}, rows={rows}", gid, affected);
                }
                else
                {
                    _logger.LogInformation("[AutoDetectCache] CLEAR no-op (no entity) gid={gid}", gid);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to clear AutoDetect schedule cache for {gid}.", gid);
            }
        }
    }

    public void Set(string gid, bool recurring, int? displayDurationHours, int[]? activeWeekdays, string? timeStartLocal, string? timeEndLocal, string? timeZone, DateTime? lastActivatedUtc)
    {
        _logger.LogInformation(
            "[AutoDetectCache] SET gid={gid} recurring={recurring} duration={duration} weekdays=[{weekdays}] start={start} end={end} tz={tz} lastActivatedUtc={last}",
            gid,
            recurring,
            displayDurationHours,
            activeWeekdays == null ? string.Empty : string.Join(',', activeWeekdays),
            timeStartLocal,
            timeEndLocal,
            timeZone,
            lastActivatedUtc);

        var entry = new CachedSchedule(
            recurring,
            displayDurationHours,
            activeWeekdays?.ToArray(),
            timeStartLocal,
            timeEndLocal,
            string.IsNullOrWhiteSpace(timeZone) ? null : timeZone.Trim(),
            lastActivatedUtc);

        lock (_syncRoot)
        {
            _cache[gid] = entry;
            Save(gid, entry);
        }
    }

    private static AutoDetectScheduleState ToState(CachedSchedule entry) =>
        new(entry.Recurring,
            entry.DisplayDurationHours,
            entry.ActiveWeekdays?.ToArray(),
            entry.TimeStartLocal,
            entry.TimeEndLocal,
            entry.TimeZone,
            entry.LastActivatedUtc);

    private void Load()
    {
        try
        {
            using var db = _dbContextFactory.CreateDbContext();
            var dbSchedules = db.AutoDetectSchedules.AsNoTracking().ToList();
            _cache = dbSchedules.ToDictionary(s => s.GroupGID, ToCachedSchedule, StringComparer.OrdinalIgnoreCase);

            TryImportLegacyFile();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load AutoDetect schedule cache; starting empty.");
            _cache = new Dictionary<string, CachedSchedule>(StringComparer.OrdinalIgnoreCase);
        }
    }

    private void Save(string gid, CachedSchedule schedule)
    {
        try
        {
            using var db = _dbContextFactory.CreateDbContext();
            var entity = db.AutoDetectSchedules.SingleOrDefault(s => s.GroupGID == gid);
            if (entity == null)
            {
                db.AutoDetectSchedules.Add(ToEntity(gid, schedule));
            }
            else
            {
                UpdateEntity(entity, schedule);
                db.AutoDetectSchedules.Update(entity);
            }

            var rows = db.SaveChanges();
            _logger.LogInformation("[AutoDetectCache] SAVE gid={gid} op={op} rows={rows} payload={payload}",
                gid,
                entity == null ? "INSERT" : "UPDATE",
                rows,
                new
                {
                    schedule.Recurring,
                    schedule.DisplayDurationHours,
                    schedule.ActiveWeekdays,
                    schedule.TimeStartLocal,
                    schedule.TimeEndLocal,
                    schedule.TimeZone,
                    schedule.LastActivatedUtc
                });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist AutoDetect schedule cache for {gid}.", gid);
        }
    }

    private void TryImportLegacyFile()
    {
        try
        {
            if (!File.Exists(_legacyCachePath) || _cache.Count > 0) return;

            var json = File.ReadAllText(_legacyCachePath);
            var data = JsonSerializer.Deserialize<Dictionary<string, CachedSchedule>>(json);
            if (data == null || data.Count == 0) return;

            using var db = _dbContextFactory.CreateDbContext();
            foreach (var kvp in data)
            {
                var gid = kvp.Key;
                var schedule = kvp.Value;
                var entity = db.AutoDetectSchedules.SingleOrDefault(s => s.GroupGID == gid);
                if (entity == null)
                {
                    db.AutoDetectSchedules.Add(ToEntity(gid, schedule));
                }
                else
                {
                    UpdateEntity(entity, schedule);
                    db.AutoDetectSchedules.Update(entity);
                }

                _cache[gid] = schedule;
            }

            db.SaveChanges();

            _logger.LogInformation("Imported legacy AutoDetect schedule cache into database ({count} entries).", data.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to import legacy AutoDetect schedule cache.");
        }
    }

    private static CachedSchedule ToCachedSchedule(AutoDetectSchedule entity) =>
        new(entity.Recurring,
            entity.DisplayDurationHours,
            entity.ActiveWeekdays?.ToArray(),
            entity.TimeStartLocal,
            entity.TimeEndLocal,
            entity.TimeZone,
            entity.LastActivatedUtc);

    private static AutoDetectSchedule ToEntity(string gid, CachedSchedule schedule) =>
        new()
        {
            GroupGID = gid,
            Recurring = schedule.Recurring,
            DisplayDurationHours = schedule.DisplayDurationHours,
            ActiveWeekdays = schedule.ActiveWeekdays?.ToArray(),
            TimeStartLocal = schedule.TimeStartLocal,
            TimeEndLocal = schedule.TimeEndLocal,
            TimeZone = schedule.TimeZone,
            LastActivatedUtc = schedule.LastActivatedUtc
        };

    private static void UpdateEntity(AutoDetectSchedule entity, CachedSchedule schedule)
    {
        entity.Recurring = schedule.Recurring;
        entity.DisplayDurationHours = schedule.DisplayDurationHours;
        entity.ActiveWeekdays = schedule.ActiveWeekdays?.ToArray();
        entity.TimeStartLocal = schedule.TimeStartLocal;
        entity.TimeEndLocal = schedule.TimeEndLocal;
        entity.TimeZone = schedule.TimeZone;
        entity.LastActivatedUtc = schedule.LastActivatedUtc;
    }
}
