using UmbraSync.API.Data;
using UmbraSync.API.Data.Enum;
using UmbraSync.API.Data.Extensions;
using UmbraSync.API.Dto.Slot;
using MareSynchronosShared.Models;

namespace MareSynchronosServer.Utils
{
    public static class Extensions
    {
        public static SlotInfoResponseDto ToSlotInfoDto(this Slot slot)
        {
            return new SlotInfoResponseDto
            {
                SlotId = slot.SlotId,
                SlotName = slot.SlotName,
                SlotDescription = slot.SlotDescription,
                Location = new SlotLocationDto
                {
                    ServerId = slot.ServerId,
                    TerritoryId = slot.TerritoryId,
                    DivisionId = slot.DivisionId,
                    WardId = slot.WardId,
                    PlotId = slot.PlotId,
                    X = slot.X,
                    Y = slot.Y,
                    Z = slot.Z,
                    Radius = slot.Radius
                },
                AssociatedSyncshell = new SlotSyncshellDto
                {
                    Gid = slot.GroupGID,
                    Name = slot.Group?.Alias ?? slot.GroupGID
                }
            };
        }

        public static GroupData ToGroupData(this Group group)
        {
            return new GroupData(group.GID, group.Alias);
        }

        public static UserData ToUserData(this GroupPair pair)
        {
            return new UserData(pair.GroupUser.UID, pair.GroupUser.Alias);
        }

        public static UserData ToUserData(this User user)
        {
            return new UserData(user.UID, user.Alias);
        }

        public static GroupPermissions GetGroupPermissions(this Group group)
        {
            var permissions = GroupPermissions.NoneSet;
            permissions.SetDisableAnimations(group.DisableAnimations);
            permissions.SetDisableSounds(group.DisableSounds);
            permissions.SetDisableInvites(!group.InvitesEnabled);
            permissions.SetDisableVFX(group.DisableVFX);
            permissions.SetPaused(group.IsPaused);
            return permissions;
        }

        public static GroupUserPermissions GetGroupPairPermissions(this GroupPair groupPair)
        {
            var permissions = GroupUserPermissions.NoneSet;
            permissions.SetDisableAnimations(groupPair.DisableAnimations);
            permissions.SetDisableSounds(groupPair.DisableSounds);
            permissions.SetPaused(groupPair.IsPaused);
            permissions.SetDisableVFX(groupPair.DisableVFX);
            return permissions;
        }

        public static GroupUserInfo GetGroupPairUserInfo(this GroupPair groupPair)
        {
            var groupUserInfo = GroupUserInfo.None;
            groupUserInfo.SetPinned(groupPair.IsPinned);
            groupUserInfo.SetModerator(groupPair.IsModerator);
            return groupUserInfo;
        }
    }
}
