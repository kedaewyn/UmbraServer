using UmbraSync.API.Dto.Group;
using UmbraSync.API.Dto.Ping;
using MareSynchronosServer.Utils;
using Microsoft.AspNetCore.Authorization;
using Microsoft.EntityFrameworkCore;

namespace MareSynchronosServer.Hubs;

public partial class MareHub
{
    [Authorize(Policy = "Identified")]
    public async Task GroupSendPing(GroupDto dto, PingMarkerDto ping)
    {
        _logger.LogCallInfo(MareHubLogger.Args(dto, ping.Type, ping.TerritoryId));

        var (userExists, _) = await TryValidateUserInGroup(dto.GID, UserUID).ConfigureAwait(false);
        if (!userExists) return;

        var group = await DbContext.Groups.AsNoTracking().SingleAsync(g => g.GID == dto.GID).ConfigureAwait(false);
        var sender = await DbContext.Users.AsNoTracking().SingleAsync(u => u.UID == UserUID).ConfigureAwait(false);
        var groupPairs = await DbContext.GroupPairs.AsNoTracking()
            .Where(p => p.GroupGID == dto.Group.GID)
            .ToListAsync().ConfigureAwait(false);

        var broadcastDto = new GroupPingMarkerDto(group.ToGroupData(), sender.ToUserData(), ping);

        await Clients.Users(groupPairs.Select(p => p.GroupUserUID).ToList())
            .Client_GroupReceivePing(broadcastDto)
            .ConfigureAwait(false);
    }

    [Authorize(Policy = "Identified")]
    public async Task GroupRemovePing(GroupDto dto, PingMarkerRemoveDto remove)
    {
        _logger.LogCallInfo(MareHubLogger.Args(dto, remove.PingId));

        var (userExists, _) = await TryValidateUserInGroup(dto.GID, UserUID).ConfigureAwait(false);
        if (!userExists) return;

        var group = await DbContext.Groups.AsNoTracking().SingleAsync(g => g.GID == dto.GID).ConfigureAwait(false);
        var sender = await DbContext.Users.AsNoTracking().SingleAsync(u => u.UID == UserUID).ConfigureAwait(false);
        var groupPairs = await DbContext.GroupPairs.AsNoTracking()
            .Where(p => p.GroupGID == dto.Group.GID)
            .ToListAsync().ConfigureAwait(false);

        await Clients.Users(groupPairs.Select(p => p.GroupUserUID).ToList())
            .Client_GroupRemovePing(group.ToGroupData(), sender.ToUserData(), remove)
            .ConfigureAwait(false);
    }

    [Authorize(Policy = "Identified")]
    public async Task GroupClearPings(GroupDto dto)
    {
        _logger.LogCallInfo(MareHubLogger.Args(dto));

        var (hasRights, group) = await TryValidateGroupModeratorOrOwner(dto.Group.GID).ConfigureAwait(false);
        if (!hasRights) return;

        var groupPairs = await DbContext.GroupPairs.AsNoTracking()
            .Where(p => p.GroupGID == dto.Group.GID)
            .Select(p => p.GroupUserUID)
            .ToListAsync().ConfigureAwait(false);

        await Clients.Users(groupPairs)
            .Client_GroupClearPings(group.ToGroupData())
            .ConfigureAwait(false);
    }
}
