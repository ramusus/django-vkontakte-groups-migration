# -*- coding: utf-8 -*-
from django.db import models
from django.core.exceptions import MultipleObjectsReturned
from vkontakte_api import fields
from vkontakte_api.utils import api_call
from vkontakte_groups.models import Group
from vkontakte_users.models import User
from datetime import datetime
import logging

log = logging.getLogger('vkontakte_groups_migration')

class GroupMigrationManager(models.Manager):

    def visible(self):
        return self.exclude(hidden=True, time__isnull=True)

    def update_for_group(self, group, offset=0):
        '''
        Fetch all users for this group, save them as IDs and after make m2m relations
        '''
        try:
            stat = self.get_or_create(group=group, time=None)[0]
        except MultipleObjectsReturned:
            self.filter(group=group, time=None).delete()
            stat = self.create(group=group, time=None, offset=0)

        offset = offset or stat.offset

        while True:
            response = api_call('groups.getMembers', gid=group.remote_id, offset=offset)
            ids = response['users']

            if len(ids) == 0:
                break

            # add new ids to group stat members
            stat.add_members(ids)
            stat.offset = offset
            stat.save()
            offset += 1000

        # save stat with time and other fields
        stat.save_final()
        signals.group_migration_updated.send(sender=Group, instance=group)

    def update_group_users_m2m(self, group, offset=0):
        '''
        Fetch all users of group, make new m2m relations, remove old m2m relations
        '''
        stats = group.migrations.order_by('-time')
        if len(stats) == 0:
            return
        stat = stats[0]
        ids = stat.members_ids
        ids_left = set(group.users.values_list('remote_id', flat=True)).difference(set(ids))

        offset = offset or stat.offset
        errors = 0

        while True:
            ids_sliced = ids[offset:offset+1000]
            if len(ids_sliced) == 0:
                break

            log.debug('Fetching users for group %s, offset %d' % (group, offset))
            try:
                users = User.remote.fetch(ids=ids_sliced, only_expired=True)
            except Exception, e:
                log.error('Error %s while getting users for group %s: "%s", offset %d' % (e.__class__, group, e, offset))
                errors += 1
                if errors == 10:
                    log.error('Number of errors of while updating users for group %s more than 10, offset %d' % (group, offset))
                    break
                continue

            if len(users) == 0:
                break
            else:
                for user in users:
                    if user.id:
                        group.users.add(user)
                stat.offset = offset
                stat.save()
                offset += 1000

        # process left users of group
        log.debug('Removing left users for group %s' % group)
        for remote_id in ids_left:
            group.users.remove(User.objects.get(remote_id=remote_id))

        signals.group_users_updated.send(sender=Group, instance=group)
        return True

class GroupMigration(models.Model):
    class Meta:
        verbose_name = u'Миграция пользователей группы Вконтакте'
        verbose_name_plural = u'Миграции пользователей групп Вконтакте'
        unique_together = ('group','time')
        ordering = ('group','time','-id')

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='migrations')
    time = models.DateTimeField(u'Дата и время', null=True)

    hidden = models.BooleanField(u'Скрыть')

    offset = models.PositiveIntegerField(default=0)

    members_ids = fields.PickledObjectField(default=[])
    members_entered_ids = fields.PickledObjectField(default=[])
    members_left_ids = fields.PickledObjectField(default=[])
    members_deactivated_entered_ids = fields.PickledObjectField(default=[])
    members_deactivated_left_ids = fields.PickledObjectField(default=[])
    members_has_avatar_entered_ids = fields.PickledObjectField(default=[])
    members_has_avatar_left_ids = fields.PickledObjectField(default=[])

    members_count = models.PositiveIntegerField(default=0)
    members_entered_count = models.PositiveIntegerField(default=0)
    members_left_count = models.PositiveIntegerField(default=0)
    members_deactivated_entered_count = models.PositiveIntegerField(default=0)
    members_deactivated_left_count = models.PositiveIntegerField(default=0)
    members_has_avatar_entered_count = models.PositiveIntegerField(default=0)
    members_has_avatar_left_count = models.PositiveIntegerField(default=0)

    objects = GroupMigrationManager()

    @property
    def next_migration(self):
        try:
            return self.group.migrations.visible().filter(time__gt=self.time).order_by('time')[0]
        except IndexError:
            return None

    @property
    def prev_migration(self):
        try:
            return self.group.migrations.visible().filter(time__lt=self.time).order_by('-time')[0]
        except IndexError:
            return None

    def delete(self, *args, **kwargs):
        '''
        Recalculate next stat members instance
        '''
        self.hide()
        super(GroupMigration, self).delete(*args, **kwargs)

    def hide(self):
        '''
        Hide curent migration, and recalculate fields of next migrations
        '''
        self.hidden = True
        self.save()
        next_stat = self.next_migration
        if next_stat:
            next_stat.update()
            next_stat.save()

    def add_members(self, ids):
        # strange, but default=[] does not work
        if isinstance(self.members_ids, str):
            self.members_ids = []
        self.members_ids += ids

    def save_final(self):
        self.time = datetime.now()
        self.offset = 0
        self.clean_members()
        self.update()
        self.save()

    def clean_members(self):
        '''
        Remove double and empty values
        '''
        self.members_ids = list(set(self.members_ids))

    def update(self):
        self.update_migration()
        self.update_deactivated()
        self.update_with_avatar()
        self.update_counters()

    def update_migration(self):
        prev_stat = self.prev_migration
        if prev_stat and self.group:
            self.members_left_ids = list(set(prev_stat.members_ids).difference(set(self.members_ids)))
            self.members_entered_ids = list(set(self.members_ids).difference(set(prev_stat.members_ids)))

    def update_deactivated(self):
        self.members_deactivated_entered_ids = list(User.objects.deactivated().filter(remote_id__in=self.members_entered_ids).values_list('remote_id', flat=True))
        self.members_deactivated_left_ids = list(User.objects.deactivated().filter(remote_id__in=self.members_left_ids).values_list('remote_id', flat=True))

    def update_with_avatar(self):
        self.members_has_avatar_entered_ids = list(User.objects.has_avatars().filter(remote_id__in=self.members_entered_ids).values_list('remote_id', flat=True))
        self.members_has_avatar_left_ids = list(User.objects.has_avatars().filter(remote_id__in=self.members_left_ids).values_list('remote_id', flat=True))

    def update_counters(self):
        for field_name in ['members','members_entered','members_left','members_deactivated_entered','members_deactivated_left','members_has_avatar_entered','members_has_avatar_left']:
            setattr(self, field_name + '_count', len(getattr(self, field_name + '_ids')))

import signals