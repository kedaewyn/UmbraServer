using System.ComponentModel.DataAnnotations;

#nullable enable

namespace MareSynchronosShared.Models;

public enum CharaDataAccess
{
    Individuals,
    ClosePairs,
    AllPairs,
    Public
}

public enum CharaDataShare
{
    Private,
    Shared
}

public class CharaData
{
    public string Id { get; set; } = string.Empty;
    public virtual User Uploader { get; set; } = null!;
    public string UploaderUID { get; set; } = string.Empty;
    public DateTime CreatedDate { get; set; }
    public DateTime UpdatedDate { get; set; }
    public string Description { get; set; } = string.Empty;
    public CharaDataAccess AccessType { get; set; }
    public CharaDataShare ShareType { get; set; }
    public DateTime? ExpiryDate { get; set; }
    public string? GlamourerData { get; set; }
    public string? CustomizeData { get; set; }
    public string? ManipulationData { get; set; }
    public int DownloadCount { get; set; } = 0;
    public virtual ICollection<CharaDataPose> Poses { get; set; } = [];
    public virtual ICollection<CharaDataFile> Files { get; set; } = [];
    public virtual ICollection<CharaDataFileSwap> FileSwaps { get; set; } = [];
    public virtual ICollection<CharaDataOriginalFile> OriginalFiles { get; set; } = [];
    public virtual ICollection<CharaDataAllowance> AllowedIndividiuals { get; set; } = [];
}

public class CharaDataAllowance
{
    [Key]
    public long Id { get; set; }
    public virtual CharaData Parent { get; set; } = null!;
    public string ParentId { get; set; } = string.Empty;
    public string ParentUploaderUID { get; set; } = string.Empty;
    public virtual User? AllowedUser { get; set; }
    public string? AllowedUserUID { get; set; }
    public virtual Group? AllowedGroup { get; set; }
    public string? AllowedGroupGID { get; set; }
}

public class CharaDataOriginalFile
{
    public virtual CharaData Parent { get; set; } = null!;
    public string ParentId { get; set; } = string.Empty;
    public string ParentUploaderUID { get; set; } = string.Empty;
    public string GamePath { get; set; } = string.Empty;
    public string Hash { get; set; } = string.Empty;
}

public class CharaDataFile
{
    public virtual FileCache FileCache { get; set; } = null!;
    public string FileCacheHash { get; set; } = string.Empty;
    public string GamePath { get; set; } = string.Empty;
    public virtual CharaData Parent { get; set; } = null!;
    public string ParentId { get; set; } = string.Empty;
    public string ParentUploaderUID { get; set; } = string.Empty;
}

public class CharaDataFileSwap
{
    public virtual CharaData Parent { get; set; } = null!;
    public string ParentId { get; set; } = string.Empty;
    public string ParentUploaderUID { get; set; } = string.Empty;
    public string GamePath { get; set; } = string.Empty;
    public string FilePath { get; set; } = string.Empty;
}

public class CharaDataPose
{
    public long Id { get; set; }
    public virtual CharaData Parent { get; set; } = null!;
    public string ParentId { get; set; } = string.Empty;
    public string ParentUploaderUID { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public string PoseData { get; set; } = string.Empty;
    public string WorldData { get; set; } = string.Empty;
}
