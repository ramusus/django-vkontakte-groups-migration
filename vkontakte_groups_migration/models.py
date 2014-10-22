# -*- coding: utf-8 -*-
from django.db import models, transaction, connection
from django.db.models import Q
from django.db.models.query import QuerySet
from django.db.utils import IntegrityError
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from django.conf import settings
from vkontakte_api import fields
from vkontakte_api.utils import api_call
from vkontakte_api.decorators import opt_generator, memoize
from vkontakte_groups.models import Group
from vkontakte_users.models import User
from datetime import datetime, timedelta
import logging

log = logging.getLogger('vkontakte_groups_migration')

FETCH_ONLY_EXPIRED_USERS = getattr(settings, 'VKONTAKTE_GROUPS_MIGRATION_FETCH_ONLY_EXPIRED_USERS', True)

class WrongMembershipsAmmount(Exception):
    pass

class EnteredMembersAreNotLeft(Exception):
    pass

def update_group_users(group):
    '''
    Fetch all users of group, make new m2m relations, remove old m2m relations
    '''
    log.debug('Fetching users for the group "%s"' % group)
    ids = GroupMembership.objects.get_user_ids(group, unique=False)
    User.remote.fetch(ids=ids, only_expired=FETCH_ONLY_EXPIRED_USERS)

    # process entered and left users of the group
    # here is possible using relative migration.members_*_ids, but it's more bugless to use absolute values, calculated from group.users
    ids_current = group.users.through.objects.filter(group=group).values_list('user_id', flat=True)
    ids_left = set(ids_current).difference(set(ids))
    ids_entered = set(ids).difference(set(ids_current))

    # get only actual users from table 'vkontakte_users_user'. It's try to prevent strange error:
    # 'IntegrityError(\'insert or update on table "vkontakte_groups_group_users" violates foreign key constraint "user_id_refs_id_5588675c"\\nDETAIL:  Key (user_id)=(...) is not present in table "vkontakte_users_user".\\n\',)'
    ids_entered_new = User.objects.filter(pk__in=ids_entered).values_list('pk', flat=True)
    if len(ids_entered_new) != len(ids_entered):
        ids_unfetched = list(set(ids_entered).difference(set(ids_entered_new)))
        log.warning("Users with ids %s missed, fetch them additionally" % ids_unfetched)
        User.remote.fetch(ids=ids_unfetched)

    log.debug('Adding %d new users to the group "%s"' % (len(ids_entered), group))
    group.users.through.objects.bulk_create([group.users.through(group=group, user_id=id) for id in ids_entered])

    log.info('Removing %d left users from the group "%s"' % (len(ids_left), group))
    group.users.through.objects.filter(group=group, user_id__in=ids_left).delete()

    signals.group_users_updated.send(sender=Group, instance=group)
    log.info('Updating m2m relations of users for group "%s" successfuly finished' % group)
    return True


def ModelQuerySetManager(ManagerBase=models.Manager):
    '''
    Function that return Manager for using QuerySet class inside the model definition
    @param ManagerBase - parent class Manager
    '''
    if not issubclass(ManagerBase, models.Manager):
        raise ValueError("Parent class for ModelQuerySetManager must be models.Manager or it's child")

    class Manager(ManagerBase):
        '''
        Manager based on QuerySet class inside the model definition
        '''
        def get_query_set(self):
            return self.model.QuerySet(self.model)

    return Manager()


class GroupMigrationQueryset(object):

    @property
    def visible(self):
        return self.exclude(hidden=True).exclude(time__isnull=True)

    @property
    def light(self):
        return self.defer(
            'members_ids',
            'members_entered_ids',
            'members_left_ids',
            'members_deactivated_entered_ids',
            'members_deactivated_left_ids',
            'members_has_avatar_entered_ids',
            'members_has_avatar_left_ids'
        )

class GroupMigrationManager(models.Manager, GroupMigrationQueryset):

    @opt_generator
    def update_for_group(self, group, offset=0):
        '''
        Fetch all users for this group, save them as IDs and after make m2m relations
        '''
        try:
            stat, created = self.get_or_create(group=group, time=None)
        except MultipleObjectsReturned:
            self.filter(group=group, time=None).delete()
            stat = self.create(group=group, time=None, offset=0)
            created = True

        if created:
            stat.set_defaults()

        offset = offset or stat.offset

        offset_step = 1000
        while True:
            response = api_call('groups.getMembers', gid=group.remote_id, offset=offset)
            ids = response['users']
            log.debug('Call returned %s ids for group "%s" with offset %s, now members_ids %s' % (len(ids), group, offset, len(stat.members_ids)))

            if len(ids) == 0:
                break

            # add new ids to group stat members
            stat.members_ids += ids
            stat.offset = offset
#            stat.save()
            offset += offset_step
            yield (offset+len(ids), response['count'], offset_step)

        # save stat with time and other fields
        stat.time = datetime.now()
        stat.save_final()
        signals.group_migration_updated.send(sender=GroupMigration, instance=stat)

class GroupMigration(models.Model):
    class Meta:
        verbose_name = u'Миграция пользователей группы Вконтакте'
        verbose_name_plural = u'Миграции пользователей групп Вконтакте'
        unique_together = ('group','time')

    class QuerySet(QuerySet, GroupMigrationQueryset):
        pass

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='migrations')
    time = models.DateTimeField(u'Дата и время', null=True, db_index=True)

    hidden = models.BooleanField(u'Скрыть', default=False, db_index=True)

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

    objects = ModelQuerySetManager(GroupMigrationManager)

    def set_defaults(self):
        '''
        It's neccesary to call after creating of every instance,
        because `default` attribute of fields.PickledObjectField doesn't work properly
        '''
        self.members_ids = []
        self.members_entered_ids = []
        self.members_left_ids = []
        self.members_deactivated_entered_ids = []
        self.members_deactivated_left_ids = []
        self.members_has_avatar_entered_ids = []
        self.members_has_avatar_left_ids = []

    @property
    def next(self):
        return self.get_next()

    @property
    def prev(self):
        return self.get_prev()

    def get_next(self, step=0):
        try:
            return self.group.migrations.visible.filter(time__gt=self.time).order_by('time')[step]
        except IndexError:
            return None

    def get_prev(self, step=0):
        try:
            return self.group.migrations.visible.filter(time__lt=self.time).order_by('-time')[step]
        except IndexError:
            return None

    @property
#    @memoize
    def user_ids(self):
        return GroupMembership.objects.get_user_ids(self.group, self.time)

    @property
#    @memoize
    def entered_user_ids(self):
        return GroupMembership.objects.get_entered_user_ids(self.group, self.time)

    @property
#    @memoize
    def left_user_ids(self):
        return GroupMembership.objects.get_left_user_ids(self.group, self.time)

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

    def fix_memberships(self):
        '''
        Fixes memberships timeline after hiding current migration
        TODO: now works only for hiding, teach method to work for unhiding cases
        '''
        if not self.hidden:
            return

        if self.next:
            cursor = connection.cursor()

            # всем кто вышел сейчас и есть в следующей - проставляем нужную дату выхода
            # дату выхода берем из последнего отрезка и удаляем его
            # меняем даты выхода первого отрезка на даты выхода второго отрезка
            cursor.execute('''
                START TRANSACTION;

                -- make temporary table, because of partial indexes cannot let do all operations in current table
                CREATE TEMP TABLE vkontakte_groups_migration_groupmembership_temp AS
                    SELECT second.group_id, second.user_id, second.time_entered, second.time_left
                    FROM "vkontakte_groups_migration_groupmembership" AS second
                    WHERE (second.time_entered = %(next_time_entered)s
                        AND second.group_id = %(group)s
                        AND second.user_id IN (
                            SELECT first.user_id FROM "vkontakte_groups_migration_groupmembership" AS first
                                WHERE (first.group_id = second.group_id
                                    AND first.time_left = %(time_left)s
                                    AND first.user_id IN %(members)s)));


                -- remove second part of memberships users, who left in first part
                DELETE FROM "vkontakte_groups_migration_groupmembership" AS second
                    WHERE (second.time_entered = %(next_time_entered)s
                        AND second.group_id = %(group)s
                        AND second.user_id IN (
                            SELECT first.user_id FROM "vkontakte_groups_migration_groupmembership" AS first
                                WHERE (first.group_id = second.group_id
                                    AND first.time_left = %(time_left)s
                                    AND first.user_id IN %(members)s)));

                -- update time_left of first part of memberships using temp table
                UPDATE "vkontakte_groups_migration_groupmembership" AS first
                    SET time_left = second.time_left
                    FROM "vkontakte_groups_migration_groupmembership_temp" AS second
                    WHERE first.group_id = %(group)s
                      AND first.time_left = %(time_left)s
                      AND first.user_id IN %(members)s
                      AND first.user_id = second.user_id
                      AND second.group_id = first.group_id
                      AND second.time_entered = %(next_time_entered)s;

                DROP TABLE vkontakte_groups_migration_groupmembership_temp;
                COMMIT;
                ''', {
                    'group': self.group.pk,
                    'time_left': self.time,
                    'members': tuple(self.next.members_ids),
                    'next_time_entered': self.next.time
                })

            # все кто вышел сейчас и нет в следующей - вышли в следующей
            GroupMembership.objects.filter(group=self.group, time_left=self.time).exclude(user_id__in=self.next.members_ids) \
                .update(time_left=self.next.time)

            # все кто вошел сейчас и нет в следующей - не вошли, удаляем
            GroupMembership.objects.filter(group=self.group, time_entered=self.time).exclude(user_id__in=self.next.members_ids) \
                .delete()

            # все кто вошел сейчас и есть в следующей - вошли в следующей
            GroupMembership.objects.filter(group=self.group, time_entered=self.time).filter(user_id__in=self.next.members_ids) \
                .update(time_entered=self.next.time)

        else:
            # if no next migration -> delete all current entered users
            GroupMembership.objects.filter(group=self.group, time_entered=self.time).delete()
            GroupMembership.objects.filter(group=self.group, time_left=self.time).update(time_left=None)

    def check_memberships_count(self):
        '''
        Compares ammount of memberships and members_count value and returns True if they are equal or False otherwise
        '''
        memberships_count = self.user_ids.count()
        if memberships_count == self.members_count:
            log.info('%s - %s: %s == %s' % (self.group, self.time, memberships_count, self.members_count))
            return True
        else:
            log.info('%s - %s: %s != %s' % (self.group, self.time, memberships_count, self.members_count))
            return False

    def update_next(self):
        next_stat = self.next
        if next_stat:
            next_stat.update()
            next_stat.save()

    @transaction.commit_on_success
    def save(self, *args, **kwargs):
        try:
            assert self.hidden != self.__class__.objects.light.get(pk=self.pk).hidden
            update_next = True
        except:
            update_next = False

        super(GroupMigration, self).save(*args, **kwargs)

        if update_next:
            self.fix_memberships()
            self.update_next()

    def save_final(self):
        '''
        Update local fields, update memberships models and save model,
        Recommended to use in the last moment of creating migration
        '''
        self.offset = 0
        self.clean_members()
        self.update()
        self.save()

        # check if migration is correct
        self.compare_with_statistic()
        self.compare_entered_left()
        self.compare_with_siblings()

        # call only after saving migrations, because in case of fault we need to have right migrations as source for memberships
        try:
            self.update_users_memberships()
        except WrongMembershipsAmmount, e:
            log.warning(e)
            GroupMembership.objects.fix_timeline(self.group)

    def compare_with_siblings(self):
        if self.hidden or not self.prev or self.members_count < 10000:
            return

        delta = self.time - self.prev.time

        if delta > timedelta(2):
            return

        def check(count1, count2):
            if not count1 or not count2:
                return True
            division = float(count1) / count2
            value = float('%f' % abs(1 - division)) # otherways it will be 0.09999999999999998
            return value >= 0.1

        if check(self.members_count, self.prev.members_count):
            if not self.next:
                log.warning("Suspicious migration found. Current value is %d, previous value is %d, time delta %s. Group %s, migration ID %d" % (self.prev.members_count, self.members_count, delta, self.group, self.id))
                self.hide()
            else:
                delta_next = self.next.time - self.time
                if check(self.members_count, self.next.members_count):
                    log.warning("Suspicious migration found. Current value is %d, previous value is %d, time delta %s, next value is %d, time delta %s, Group %s, migration ID %d" % (self.members_count, self.prev.members_count, delta, self.next.members_count, delta_next, self.group, self.id))
                    self.hide()
                elif check(self.next.members_count, self.prev.members_count):
                    log.warning("Suspicious previous migration found. Current value is %d, previous value is %d, time delta %s, next value is %d, time delta %s, Group %s, migration ID %d" % (self.members_count, self.prev.members_count, delta, self.next.members_count, delta_next, self.group, self.id))
                    self.prev.hide()

    def compare_entered_left(self):
        if self.hidden or not self.prev or self.members_left_count <= 5000 or self.members_entered_count == 0:
            return

        delta = self.time - self.prev.time

        division = float(self.members_entered_count) / self.members_left_count
        if division < 0.05:
            log.warning("Suspicious migration found. Ammounts members entered is %d, left is %d, time delta %s. Group %s, migration ID %d" % (self.members_entered_count, self.members_left_count, delta, self.group, self.id))
            self.hide()

    def compare_with_statistic(self):
        try:
            assert not self.hidden
            assert 'vkontakte_groups_statistic' in settings.INSTALLED_APPS
            members_count = self.group.statistics.get(date=self.time.date(), period=1).members
            assert members_count
        except (ObjectDoesNotExist, AssertionError):
            return

        delta = abs(members_count - self.members_count)
        if delta >= 1000:
            log.warning("Suspicious migration found. API value is %d, statistic value is %d, delta is %s. Group %s, migration ID %d" % (members_count, self.members_count, delta, self.group, self.id))
            self.hide()

    def clean_members(self):
        '''
        Remove double and empty values
        '''
        self.members_ids = list(set(self.members_ids))

    def update(self):
        self.update_entered_left()
#        self.update_deactivated()
#        self.update_with_avatar()
        self.update_counters()

    def update_entered_left(self):
        prev_stat = self.prev
        if self.prev and self.group:
            self.members_left_ids = list(set(prev_stat.members_ids).difference(set(self.members_ids)))
            self.members_entered_ids = list(set(self.members_ids).difference(set(prev_stat.members_ids)))
        else:
            self.members_left_ids = []
            self.members_entered_ids = []

    def update_deactivated(self):
        self.members_deactivated_entered_ids = list(User.objects.deactivated().filter(remote_id__in=self.members_entered_ids).values_list('remote_id', flat=True))
        self.members_deactivated_left_ids = list(User.objects.deactivated().filter(remote_id__in=self.members_left_ids).values_list('remote_id', flat=True))

    def update_with_avatar(self):
        self.members_has_avatar_entered_ids = list(User.objects.with_avatar().filter(remote_id__in=self.members_entered_ids).values_list('remote_id', flat=True))
        self.members_has_avatar_left_ids = list(User.objects.with_avatar().filter(remote_id__in=self.members_left_ids).values_list('remote_id', flat=True))

    def update_counters(self):
        for field_name in ['members','members_entered','members_left','members_deactivated_entered','members_deactivated_left','members_has_avatar_entered','members_has_avatar_left']:
            setattr(self, field_name + '_count', len(getattr(self, field_name + '_ids')))

    @transaction.commit_on_success
    def update_users_memberships(self):
        '''
        Fetch all users of group, make new m2m relations, remove old m2m relations
        '''
        if self.hidden:
            return

        if not self.prev:
            # it's first migration -> create memberships
            GroupMembership.objects.bulk_create([GroupMembership(group=self.group, user_id=user_id) for user_id in self.members_ids])
        else:
            # ensure current number of memberships equal to members in previous migration
            memberships_count = GroupMembership.objects.get_user_ids(self.group).count()
            members_count = self.prev.members_count
            if members_count != memberships_count:
                # incostistent state with previous migration
                raise WrongMembershipsAmmount("Number of current memberships %d is not equal to members count %d of previous migration, group %s at %s" % (memberships_count, members_count, self.group, self.time))

            # ensure entered users not in memberships now
            error_ids_count = GroupMembership.objects.get_user_ids(self.group).filter(user_id__in=self.members_entered_ids).count()
            if error_ids_count != 0:
                raise EnteredMembersAreNotLeft("Found %d just entered users, that still not left from the group %s at %s" % (error_ids_count, self.group, self.time))

            # ensure left users in memberships now
            left_ids_count = GroupMembership.objects.get_user_ids(self.group).filter(user_id__in=self.members_left_ids).count()
            if left_ids_count != self.members_left_count:
                raise Exception("Not all left users found %d != %d between active in group %s at %s" % (left_ids_count, self.members_left_count, self.group, self.time))

            # create entered users
            GroupMembership.objects.bulk_create([GroupMembership(group=self.group, user_id=user_id, time_entered=self.time) for user_id in self.members_entered_ids])

            # update left users
            GroupMembership.objects.filter(group=self.group, time_left=None, user_id__in=self.members_left_ids).update(time_left=self.time)

        return True


class GroupMembershipManager(models.Manager):

    def fix_timeline(self, group):
        '''
        In consistent case it just check_memberships_count of last migr and exit
        In incosistent case it will go back to previous migration that is consistent, and start to update memberships further
        '''
        migr = group.migrations.latest('id')

        while True:
            if not migr or migr.check_memberships_count():
                break
            else:
                migr = migr.prev

        self.clear_timeline_after_migration(group, migr)

        while True:
            try:
                migr = migr.next if migr else group.migrations.order_by('time')[0]
                migr.save_final()
                migr.check_memberships_count()
            except EnteredMembersAreNotLeft:
                # go back and clear one more
                migr = migr.prev.prev
                self.clear_timeline_after_migration(group, migr)
            except AttributeError:
                break

        migr = group.migrations.latest('id')
        log.info('%s: %s == %s' % (group, self.get_user_ids(group).count(), migr.members_count))

    def clear_timeline_after_migration(self, group, migr):
        if migr:
            self.clear_timeline_after(group, migr.time)
        else:
            self.filter(group=group).delete()

    def clear_timeline_after(self, group, time):
        '''
        Method:
         * removes all entered and left users after `time`
         * removes all entered and not left after `time`
         * makes all left users after `time` not left
        '''
        self.filter(group=group, time_left__gt=time, time_entered__gt=time).delete()
        self.filter(group=group, time_entered__gt=time, time_left=None).delete()
        self.filter(group=group, time_left__gt=time).update(time_left=None)

    def _prepare_qs(self, qs, unique):
        if unique:
            qs = qs.order_by('user_id').distinct('user_id')
        return qs.values_list('user_id', flat=True)

    def get_user_ids(self, group, time=None, unique=True):
        if time is None:
            qs = self.filter(group=group, time_left=None)
        else:
            qs = self.filter(group=group) \
                .filter(Q(time_entered=None,        time_left=None) | \
                        Q(time_entered=None,        time_left__gt=time) | \
                        Q(time_entered__lte=time,   time_left=None) | \
                        Q(time_entered__lte=time,   time_left__gt=time))

        return self._prepare_qs(qs, unique)

    def get_entered_user_ids(self, group, time, unique=True):
        qs = self.filter(group=group).filter(time_entered=time)
        return self._prepare_qs(qs, unique)

    def get_left_user_ids(self, group, time, unique=True):
        qs = self.filter(group=group).filter(time_left=time)
        return self._prepare_qs(qs, unique)

    def get_user_ids_of_period(self, group, date_from, date_to, field=None, unique=True):

        if field is None:
            # TODO: made normal filtering
            kwargs = {'time_entered': None, 'time_left': None} \
                | {'time_entered__lte': date_from, 'time_left': None} \
                | {'time_entered': None, 'time_left__gte': date_to}
        elif field in ['left','entered']:
            kwargs = {'time_%s__gt' % field: date_from, 'time_%s__lte' % field: date_to}
        else:
            raise ValueError("Attribute `field` should be equal to 'left' of 'entered'")

        qs = self.filter(group=group, **kwargs)
        return self._prepare_qs(qs, unique)

class GroupMembership(models.Model):
    class Meta:
        verbose_name = u'Членство пользователя группы Вконтакте'
        verbose_name_plural = u'Членства пользователей групп Вконтакте'

    group = models.ForeignKey(Group, verbose_name=u'Группа', related_name='memberships')
    user_id = models.PositiveIntegerField(u'ID пользователя', db_index=True)

    time_entered = models.DateTimeField(u'Дата и время вступления', null=True, db_index=True)
    time_left = models.DateTimeField(u'Дата и время выхода', null=True, db_index=True)

    objects = GroupMembershipManager()

    def save(self, *args, **kwargs):
        # TODO: perhaps useless checkings, since all GroupMemberships are created by bulk_create..

        if self.time_entered and self.time_left and self.time_entered > self.time_left:
            raise IntegrityError("GroupMembership couldn't have time_entered (%s) > time_left (%s), group %s, user remote ID %s" % (self.time_entered, self.time_left, self.group, self.user_id))

        # check additionally null values of time_entered and time_left,
        # because for postgres null values are acceptable in unique constraint
#         qs = self.__class__.objects.filter(group=self.group, user_id=self.user_id)
#         if not self.time_entered and qs.filter(time_entered=None).count() != 0:
#             raise IntegrityError("columns group_id=%s, user_id=%s, time_entered=None are not unique" % (self.group_id, self.user_id))
#         if not self.time_left and qs.filter(time_left=None).count() != 0:
#             raise IntegrityError("columns group_id=%s, user_id=%s, time_left=None are not unique" % (self.group_id, self.user_id))

        return super(GroupMembership, self).save(*args, **kwargs)

import signals
