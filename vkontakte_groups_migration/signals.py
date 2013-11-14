# -*- coding: utf-8 -*-
from django.dispatch import Signal
from django.conf import settings
from annoying.decorators import signals
from vkontakte_groups.models import Group
from models import GroupMigration

group_migration_updated = Signal(providing_args=['instance'])
group_users_updated = Signal(providing_args=['instance'])

@signals(group_migration_updated, sender=GroupMigration)
def group_users_update_m2m(sender, instance, **kwargs):
    if 'djcelery' in settings.INSTALLED_APPS:
        from tasks import VkontakteGroupUpdateUsersM2M
        return VkontakteGroupUpdateUsersM2M.delay(instance.id)
    else:
        instance.update_users_relations()