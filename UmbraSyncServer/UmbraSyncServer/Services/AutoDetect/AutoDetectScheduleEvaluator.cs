using System.Globalization;

namespace MareSynchronosServer.Services.AutoDetect;

internal static class AutoDetectScheduleEvaluator
{
    public static bool? EvaluateDesiredVisibility(AutoDetectScheduleCache.AutoDetectScheduleState schedule, DateTimeOffset nowUtc)
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
                // window spans midnight
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
                // fallback to UTC
            }
        }

        return TimeZoneInfo.Utc;
    }
}
