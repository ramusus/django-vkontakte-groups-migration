# -*- coding: utf-8 -*-
from django.test import TestCase
from models import GroupMigration, User
from vkontakte_users.factories import UserFactory
from vkontakte_groups.factories import GroupFactory
from factories import GroupMigrationFactory
from datetime import datetime

GROUP_ID = 30221121

class VkontakteGroupsMigrationTest(TestCase):

    def test_m2m_relations(self):

        [UserFactory(remote_id=i, fetched=datetime.now()) for i in range(0, 2000)]
        migration = GroupMigrationFactory(members_ids=list(range(1000, 2000)))
        for i in range(0, 1500):
            migration.group.users.add(User.objects.get(remote_id=i))

        self.assertListEqual(list(migration.group.users.values_list('remote_id', flat=True)), range(0, 1500))
        migration.update_users_relations()
        self.assertListEqual(list(migration.group.users.values_list('remote_id', flat=True)), range(1000, 2000))

    def test_deleting_hiding_migration(self):

        for i in range(1,7):
            UserFactory.create(remote_id=i)

        group = GroupFactory.create(remote_id=GROUP_ID)
        stat1 = GroupMigration.objects.create(group=group, members_ids=[1,2,3,4,5])
        stat1.save_final()
        stat2 = GroupMigration.objects.create(group=group, members_ids=[1,2,3,4,6])
        stat2.save_final()
        stat3 = GroupMigration.objects.create(group=group, members_ids=[1,2,3,5,7])
        stat3.save_final()

        # difference between stat2 and stat1
        self.assertEqual(stat2.members_entered_ids, [6])
        self.assertEqual(stat2.members_left_ids, [5])
        # difference between stat3 and stat2
        self.assertEqual(stat3.members_entered_ids, [5,7])
        self.assertEqual(stat3.members_left_ids, [4,6])

        stat2.delete()
        stat3 = GroupMigration.objects.get(id=stat3.id)

        # difference between stat3 and stat1
        self.assertEqual(stat3.members_entered_ids, [7])
        self.assertEqual(stat3.members_left_ids, [4])

        stat4 = GroupMigration.objects.create(group=group, members_ids=[1,2,3,4,6])
        stat4.save_final()

        # difference between stat4 and stat3
        self.assertEqual(stat4.members_entered_ids, [4,6])
        self.assertEqual(stat4.members_left_ids, [5,7])

        stat3.hide()
        stat4 = GroupMigration.objects.get(id=stat4.id)

        # difference between stat4 and stat1
        self.assertEqual(stat4.members_entered_ids, [6])
        self.assertEqual(stat4.members_left_ids, [5])

        stat5 = GroupMigration.objects.create(group=group, members_ids=[1,2,3,5,7])
        stat5.save_final()

        # difference between stat5 and stat4
        self.assertEqual(stat5.members_entered_ids, [5,7])
        self.assertEqual(stat5.members_left_ids, [4,6])

        stat4.hidden = True
        stat4.save()
        stat5 = GroupMigration.objects.get(id=stat5.id)

        # difference between stat5 and stat1
        self.assertEqual(stat5.members_entered_ids, [7])
        self.assertEqual(stat5.members_left_ids, [4])