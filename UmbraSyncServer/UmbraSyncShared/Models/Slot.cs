using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace MareSynchronosShared.Models;

public class Slot
{
    [Key]
    public Guid SlotId { get; set; } = Guid.NewGuid();

    [Required]
    [MaxLength(100)]
    public string SlotName { get; set; }

    [MaxLength(500)]
    public string SlotDescription { get; set; }

    [Required]
    [MaxLength(20)]
    public string GroupGID { get; set; }

    [ForeignKey(nameof(GroupGID))]
    public Group Group { get; set; }

    // Location details
    public uint ServerId { get; set; }
    public uint TerritoryId { get; set; }
    public uint DivisionId { get; set; }
    public uint WardId { get; set; }
    public uint PlotId { get; set; }
    
    // Coordinates for radius detection
    public float X { get; set; }
    public float Y { get; set; }
    public float Z { get; set; }
    public float Radius { get; set; } = 10f;
}
