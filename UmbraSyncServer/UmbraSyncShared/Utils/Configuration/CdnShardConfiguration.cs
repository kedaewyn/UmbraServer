using System.Collections.Generic;

namespace MareSynchronosShared.Utils.Configuration;

public class CdnShardConfiguration
{
    public ICollection<string> Continents { get; set; } = new List<string>();
    public string FileMatch { get; set; } = string.Empty;
    public Uri CdnFullUrl { get; set; }

    public override string ToString()
    {
        return CdnFullUrl.ToString() + "[" + string.Join(',', Continents) + "] == " + FileMatch;
    }
}
