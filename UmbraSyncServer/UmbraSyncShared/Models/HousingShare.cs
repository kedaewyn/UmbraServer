using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace MareSynchronosShared.Models;

public class HousingShare
{
    [Key]
    public Guid Id { get; set; }
    [MaxLength(10)]
    public string OwnerUID { get; set; } = string.Empty;
    public User Owner { get; set; } = null!;
    public string Description { get; set; } = string.Empty;
    public uint ServerId { get; set; }
    public uint MapId { get; set; }
    public uint TerritoryId { get; set; }
    public uint DivisionId { get; set; }
    public uint WardId { get; set; }
    public uint HouseId { get; set; }
    public uint RoomId { get; set; }
    public byte[] CipherData { get; set; } = Array.Empty<byte>();
    public byte[] Nonce { get; set; } = Array.Empty<byte>();
    public byte[] Salt { get; set; } = Array.Empty<byte>();
    public byte[] Tag { get; set; } = Array.Empty<byte>();
    public DateTime CreatedUtc { get; set; }
    public DateTime? UpdatedUtc { get; set; }
    public ICollection<HousingShareAllowedUser> AllowedIndividuals { get; set; } = new HashSet<HousingShareAllowedUser>();
    public ICollection<HousingShareAllowedGroup> AllowedSyncshells { get; set; } = new HashSet<HousingShareAllowedGroup>();
}

public class HousingShareAllowedUser
{
    public Guid ShareId { get; set; }
    public HousingShare Share { get; set; } = null!;
    [MaxLength(10)]
    public string AllowedIndividualUid { get; set; } = string.Empty;
}

public class HousingShareAllowedGroup
{
    public Guid ShareId { get; set; }
    public HousingShare Share { get; set; } = null!;
    [MaxLength(20)]
    public string AllowedGroupGid { get; set; } = string.Empty;
}
