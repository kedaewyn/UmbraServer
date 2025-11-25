using System;
using System.ComponentModel.DataAnnotations;

namespace MareSynchronosShared.Models;

public class AutoDetectSchedule
{
    [Key]
    [MaxLength(20)]
    public string GroupGID { get; set; } = string.Empty;

    public bool Recurring { get; set; }
    public int? DisplayDurationHours { get; set; }
    public int[]? ActiveWeekdays { get; set; }
    public string? TimeStartLocal { get; set; }
    public string? TimeEndLocal { get; set; }
    public string? TimeZone { get; set; }
    public DateTime? LastActivatedUtc { get; set; }
}
