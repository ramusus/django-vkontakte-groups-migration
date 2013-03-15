# -*- coding: utf-8 -*-
from django.contrib import admin
from vkontakte_api.admin import VkontakteModelAdmin
from vkontakte_groups.admin import Group, GroupAdmin as GroupAdminOriginal
from models import GroupMigration

class GroupMigrationInline(admin.TabularInline):
    model = GroupMigration
    fields = ('group','time','offset','hidden','members_count','members_entered_count','members_left_count')
    readonly_fields = ('group','time','offset','members_count','members_entered_count','members_left_count')
    ordering = ('-time',)
    extra = 0
    can_delete = False

    def queryset(self, request):
        qs = super(GroupMigrationInline, self).queryset(request)
        return qs.exclude(time__isnull=True).defer(
            'members_ids',
            'members_entered_ids',
            'members_left_ids',
            'members_deactivated_entered_ids',
            'members_deactivated_left_ids',
            'members_has_avatar_entered_ids',
            'members_has_avatar_left_ids'
        )

class GroupAdmin(GroupAdminOriginal):
    inlines = GroupAdminOriginal.inlines + [GroupMigrationInline]

class GroupMigrationAdmin(VkontakteModelAdmin):
    list_display = ('group','time')
    list_display_links = ('time',)
#    list_filter = ('group',)

admin.site.unregister(Group)
admin.site.register(Group, GroupAdmin)
admin.site.register(GroupMigration, GroupMigrationAdmin)