using System.ComponentModel.DataAnnotations;

#nullable enable

namespace MareSynchronosShared.Models;

public class Auth
{
    [Key]
    [MaxLength(64)]
    public string HashedKey { get; set; } = string.Empty;

    public string UserUID { get; set; } = string.Empty;
    public User User { get; set; } = null!;
    public bool IsBanned { get; set; }
    public string? PrimaryUserUID { get; set; }
    public User? PrimaryUser { get; set; }
}
