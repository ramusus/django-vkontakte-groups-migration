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
        return qs.light.exclude(time__isnull=True)

class GroupAdmin(GroupAdminOriginal):
    inlines = GroupAdminOriginal.inlines + [GroupMigrationInline]

class GroupMigrationAdmin(VkontakteModelAdmin):
    list_display = ('group','time')
    list_display_links = ('time',)
#    list_filter = ('group',)

admin.site.unregister(Group)
admin.site.register(Group, GroupAdmin)
admin.site.register(GroupMigration, GroupMigrationAdmin)