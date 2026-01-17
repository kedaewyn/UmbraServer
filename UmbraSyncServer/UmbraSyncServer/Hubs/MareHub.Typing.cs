using System;
using System.Linq;
using System.Threading.Tasks;
using UmbraSync.API.Data.Enum;
using UmbraSync.API.Data.Extensions;
using UmbraSync.API.Dto.User;
using MareSynchronosServer.Utils;
using Microsoft.AspNetCore.Authorization;
using Microsoft.EntityFrameworkCore;

namespace MareSynchronosServer.Hubs;

public partial class MareHub
{
    [Authorize(Policy = "Identified")]
    public async Task UserSetTypingState(bool isTyping, TypingScope scope)
    {
        _logger.LogCallInfo(MareHubLogger.Args(isTyping, scope));

        var sender = await DbContext.Users.AsNoTracking()
            .SingleAsync(u => u.UID == UserUID)
            .ConfigureAwait(false);

        var normalizedScope = Enum.IsDefined(typeof(TypingScope), scope) ? scope : TypingScope.Unknown;
        var typingDto = new TypingStateDto(sender.ToUserData(), isTyping, normalizedScope);

        await Clients.Caller.Client_UserTypingState(typingDto).ConfigureAwait(false);
        
        if (normalizedScope == TypingScope.Proximity || normalizedScope == TypingScope.Unknown)
        {
            var pairedEntries = await GetAllPairedClientsWithPauseState().ConfigureAwait(false);
            var recipients = pairedEntries
                .Where(p => !p.IsPaused)
                .Select(p => p.UID)
                .Distinct(StringComparer.Ordinal)
                .ToList();

            if (recipients.Count > 0)
            {
                await Clients.Users(recipients).Client_UserTypingState(typingDto).ConfigureAwait(false);
            }
        }
        else
        {
            _logger.LogCallWarning(MareHubLogger.Args("UserSetTypingState called with scoped channel without channelId, ignoring", scope));
        }
    }
}
