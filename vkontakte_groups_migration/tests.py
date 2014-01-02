# -*- coding: utf-8 -*-
from django.test import TestCase
from django.conf import settings
from models import GroupMigration, User
from vkontakte_users.factories import UserFactory
from vkontakte_groups.factories import GroupFactory
from factories import GroupMigrationFactory, GroupMembership
from datetime import datetime, timedelta

GROUP_ID = 30221121

class VkontakteGroupsMigrationTest(TestCase):

    def test_user_memberships(self):

        migration1 = GroupMigrationFactory(time=datetime.now() - timedelta(3), members_ids=range(30, 100))
        migration2 = GroupMigrationFactory(group=migration1.group, time=datetime.now() - timedelta(2), members_ids=range(0, 50))
        migration3 = GroupMigrationFactory(group=migration1.group, time=datetime.now() - timedelta(1), members_ids=range(30, 110))
        migration4 = GroupMigrationFactory(group=migration1.group, members_ids=range(15, 100))

        def membership(id):
            return GroupMembership.objects.get(user_id=id)

        def memberships(id):
            return GroupMembership.objects.filter(user_id=id)

        def id90_state1():
            self.assertEqual(membership(90).time_entered, None)
            self.assertEqual(membership(90).time_left, None)

        def id90_state2():
            self.assertEqual(membership(90).time_entered, None)
            self.assertEqual(membership(90).time_left, migration2.time)

        def id90_state3():
            self.assertEqual(memberships(90)[0].time_entered, None)
            self.assertEqual(memberships(90)[0].time_left, migration2.time)
            self.assertEqual(memberships(90)[1].time_entered, migration3.time)
            self.assertEqual(memberships(90)[1].time_left, None)

        def id90_state4():
            self.assertEqual(memberships(90)[0].time_entered, None)
            self.assertEqual(memberships(90)[0].time_left, migration2.time)
            self.assertEqual(memberships(90)[1].time_entered, migration3.time)
            self.assertEqual(memberships(90)[1].time_left, None)

        def id90_state4_corrected():
            self.assertEqual(memberships(90)[0].time_entered, None)
            self.assertEqual(memberships(90)[0].time_left, migration2.time)
            self.assertEqual(memberships(90)[1].time_entered, migration4.time)
            self.assertEqual(memberships(90)[1].time_left, None)

        def id0_state1():
            self.assertEqual(memberships(0).count(), 0)

        def id0_state2():
            self.assertEqual(membership(0).time_entered, migration2.time)
            self.assertEqual(membership(0).time_left, None)

        def id0_state3():
            self.assertEqual(membership(0).time_entered, migration2.time)
            self.assertEqual(membership(0).time_left, migration3.time)

        def id0_state3_corrected():
            self.assertEqual(membership(0).time_entered, migration2.time)
            self.assertEqual(membership(0).time_left, migration4.time)

        def id20_state1():
            self.assertEqual(memberships(20).count(), 0)

        def id20_state2():
            self.assertEqual(membership(20).time_entered, migration2.time)
            self.assertEqual(membership(20).time_left, None)

        def id20_state3():
            self.assertEqual(membership(20).time_entered, migration2.time)
            self.assertEqual(membership(20).time_left, migration3.time)

        def id20_state4():
            self.assertEqual(memberships(20)[0].time_entered, migration2.time)
            self.assertEqual(memberships(20)[0].time_left, migration3.time)
            self.assertEqual(memberships(20)[1].time_entered, migration4.time)
            self.assertEqual(memberships(20)[1].time_left, None)

        def id40_state1():
            self.assertEqual(membership(40).time_entered, None)
            self.assertEqual(membership(40).time_left, None)

        def id40_state3():
            self.assertEqual(membership(40).time_entered, None)
            self.assertEqual(membership(40).time_left, None)

        def id105_state1():
            self.assertEqual(memberships(105).count(), 0)

        def id105_state3():
            self.assertEqual(membership(105).time_entered, migration3.time)
            self.assertEqual(membership(105).time_left, None)

        def id105_state4():
            self.assertEqual(membership(105).time_entered, migration3.time)
            self.assertEqual(membership(105).time_left, migration4.time)

        def check_users_ids(migration):
            self.assertItemsEqual(migration.members_ids, migration.user_ids)
            self.assertItemsEqual(migration.members_left_ids, migration.left_user_ids)
            self.assertItemsEqual(migration.members_entered_ids, migration.entered_user_ids)


        migration1.save_final()
        self.assertEqual(GroupMembership.objects.count(), 70)
        check_users_ids(migration1)
        id0_state1()
        id20_state1()
        id40_state1()
        id90_state1()
        id105_state1()

        migration2.save_final()
        self.assertEqual(GroupMembership.objects.count(), 100)
        check_users_ids(migration2)
        id0_state2()
        id20_state2()
        id40_state1()
        id90_state2()
        id105_state1()

        migration3.save_final()
        self.assertEqual(GroupMembership.objects.count(), 160)
        check_users_ids(migration3)
        id0_state3()
        id20_state3()
        id40_state3()
        id90_state3()
        id105_state3()

        migration4.save_final()
        self.assertEqual(GroupMembership.objects.count(), 175)
        check_users_ids(migration4)
        id0_state3()
        id20_state4()
        id40_state3()
        id90_state4()
        id105_state4()

        # hide migration3
        migration3 = GroupMigration.objects.get(id=migration3.id)
        migration3.hide()
        self.assertEqual(GroupMembership.objects.count(), 150)
        id0_state3_corrected()
        id20_state2()
        id40_state1()
        id90_state4_corrected()
        id105_state1()

        # hide migration4 -> back to state2
        migration4 = GroupMigration.objects.get(id=migration4.id)
        migration4.hide()
        self.assertEqual(GroupMembership.objects.count(), 100)
        id0_state2()
        id20_state2()
        id40_state1()
        id90_state2()
        id105_state1()

        # hide migration2 -> back to state1
        migration2 = GroupMigration.objects.get(id=migration2.id)
        migration2.hide()
        self.assertEqual(GroupMembership.objects.count(), 70)
        id0_state1()
        id20_state1()
        id40_state1()
        id90_state1()
        id105_state1()

    def test_comparing_with_statistic(self):

        if 'vkontakte_groups_statistic' not in settings.INSTALLED_APPS:
            return False

        from vkontakte_groups_statistic.factories import GroupStatFactory

        migration = GroupMigrationFactory(members_ids=range(0, 10000))
        migration.update()
        migration.save()

        stat = GroupStatFactory(group=migration.group, members=9900, date=migration.time.date())

        self.assertEqual(migration.hidden, False)
        migration.compare_with_statistic()
        self.assertEqual(migration.hidden, True)

        stat.members = 9901
        stat.save()
        migration.hidden = False

        self.assertEqual(migration.hidden, False)
        migration.compare_with_statistic()
        self.assertEqual(migration.hidden, False)

    def test_comparing_with_previous(self):

        migration1 = GroupMigrationFactory(time=datetime.now() - timedelta(2), members_ids=range(0, 10000))
        migration1.update()
        migration1.save()

        migration2 = GroupMigrationFactory(group=migration1.group, time=datetime.now() - timedelta(1), members_ids=range(0, 9000))
        migration2.update()
        migration2.save()

        self.assertEqual(migration2.hidden, False)
        migration2.compare_with_previous()
        self.assertEqual(migration2.hidden, True)

        migration3 = GroupMigrationFactory(group=migration1.group, members_ids=range(0, 9001))
        migration3.update()
        migration3.save()

        self.assertEqual(migration3.hidden, False)
        migration3.compare_with_previous()
        self.assertEqual(migration3.hidden, False)

    def test_m2m_relations(self):

        [UserFactory(remote_id=i, fetched=datetime.now()) for i in range(0, 20)]
        migration = GroupMigrationFactory(members_ids=list(range(10, 20)))
        for i in range(0, 15):
            migration.group.users.add(User.objects.get(remote_id=i))
        migration.save_final()

        self.assertListEqual(list(migration.group.users.values_list('remote_id', flat=True)), range(0, 15))
        migration.update_users_relations()
        self.assertListEqual(list(migration.group.users.values_list('remote_id', flat=True)), range(10, 20))

    def test_deleting_hiding_migration(self):

        for i in range(1,7):
            UserFactory.create(remote_id=i)

        group = GroupFactory.create(remote_id=GROUP_ID)
        stat1 = GroupMigrationFactory(group=group, time=datetime.now()-timedelta(10), members_ids=[1,2,3,4,5])
        stat1.save_final()
        stat2 = GroupMigrationFactory(group=group, time=datetime.now()-timedelta(9), members_ids=[1,2,3,4,6])
        stat2.save_final()
        stat3 = GroupMigrationFactory(group=group, time=datetime.now()-timedelta(8), members_ids=[1,2,3,5,7])
        stat3.save_final()

        # difference between stat2 and stat1
        self.assertItemsEqual(stat2.members_entered_ids, [6])
        self.assertItemsEqual(stat2.members_left_ids, [5])
        # difference between stat3 and stat2
        self.assertItemsEqual(stat3.members_entered_ids, [5,7])
        self.assertItemsEqual(stat3.members_left_ids, [4,6])

        stat2.delete()
        stat3 = GroupMigration.objects.get(id=stat3.id)

        # difference between stat3 and stat1
        self.assertItemsEqual(stat3.members_entered_ids, [7])
        self.assertItemsEqual(stat3.members_left_ids, [4])

        stat4 = GroupMigrationFactory(group=group, time=datetime.now()-timedelta(7), members_ids=[1,2,3,4,6])
        stat4.save_final()

        # difference between stat4 and stat3
        self.assertItemsEqual(stat4.members_entered_ids, [4,6])
        self.assertItemsEqual(stat4.members_left_ids, [5,7])

        stat3.hide()
        stat4 = GroupMigration.objects.get(id=stat4.id)

        # difference between stat4 and stat1
        self.assertItemsEqual(stat4.members_entered_ids, [6])
        self.assertItemsEqual(stat4.members_left_ids, [5])

        stat5 = GroupMigrationFactory(group=group, time=datetime.now()-timedelta(6), members_ids=[1,2,3,5,7])
        stat5.save_final()

        # difference between stat5 and stat4
        self.assertItemsEqual(stat5.members_entered_ids, [5,7])
        self.assertItemsEqual(stat5.members_left_ids, [4,6])

        stat4.hidden = True
        stat4.save()
        stat5 = GroupMigration.objects.get(id=stat5.id)

        # difference between stat5 and stat1
        self.assertItemsEqual(stat5.members_entered_ids, [7])
        self.assertItemsEqual(stat5.members_left_ids, [4])