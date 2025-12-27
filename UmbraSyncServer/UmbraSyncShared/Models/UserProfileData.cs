using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace MareSynchronosShared.Models;

public class UserProfileData
{
    [Key]
    public string UserUID { get; set; }

    [ForeignKey(nameof(UserUID))]
    public User User { get; set; }

    public string? Base64ProfileImage { get; set; }
    public bool FlaggedForReport { get; set; }
    public bool IsNSFW { get; set; }
    public bool ProfileDisabled { get; set; }
    public string? UserDescription { get; set; }
}
