using System;
using System.Linq;
using System.Threading.Tasks;
using MareSynchronos.API.Data.Extensions;
using MareSynchronos.API.Dto.User;
using MareSynchronosServer.Utils;
using Microsoft.AspNetCore.Authorization;
using Microsoft.EntityFrameworkCore;

namespace MareSynchronosServer.Hubs;

public partial class MareHub
{
    [Authorize(Policy = "Identified")]
    public async Task UserSetTypingState(bool isTyping)
    {
        _logger.LogCallInfo(MareHubLogger.Args(isTyping));

        var pairedEntries = await GetAllPairedClientsWithPauseState().ConfigureAwait(false);
        if (pairedEntries.Count == 0)
            return;

        var recipients = pairedEntries
            .Where(p => !p.IsPaused)
            .Select(p => p.UID)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (recipients.Count == 0)
            return;

        var sender = await DbContext.Users.AsNoTracking()
            .SingleAsync(u => u.UID == UserUID)
            .ConfigureAwait(false);

        var typingDto = new TypingStateDto(sender.ToUserData(), isTyping);

        await Clients.Users(recipients).Client_UserTypingState(typingDto).ConfigureAwait(false);
    }
}
