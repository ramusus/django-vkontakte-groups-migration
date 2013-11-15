# -*- coding: utf-8 -*-
import datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):

        # Changing field 'GroupMigrationUser.migration'
        db.alter_column('vkontakte_groups_migration_groupmigrationuser', 'migration_id', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['vkontakte_groups_migration.GroupMigrationNew']))
        # Adding unique constraint on 'GroupMigrationUser', fields ['user_id', 'migration']
        db.create_unique('vkontakte_groups_migration_groupmigrationuser', ['user_id', 'migration_id'])


    def backwards(self, orm):
        # Removing unique constraint on 'GroupMigrationUser', fields ['user_id', 'migration']
        db.delete_unique('vkontakte_groups_migration_groupmigrationuser', ['user_id', 'migration_id'])


        # Changing field 'GroupMigrationUser.migration'
        db.alter_column('vkontakte_groups_migration_groupmigrationuser', 'migration_id', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['vkontakte_groups_migration.GroupMigration']))

    models = {
        u'contenttypes.contenttype': {
            'Meta': {'ordering': "('name',)", 'unique_together': "(('app_label', 'model'),)", 'object_name': 'ContentType', 'db_table': "'django_content_type'"},
            'app_label': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'model': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '100'})
        },
        u'vkontakte_groups.group': {
            'Meta': {'ordering': "['name']", 'object_name': 'Group'},
            'fetched': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True', 'null': 'True', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'is_admin': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'is_closed': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '800'}),
            'photo': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'photo_big': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'photo_medium': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'remote_id': ('django.db.models.fields.BigIntegerField', [], {'unique': 'True'}),
            'screen_name': ('django.db.models.fields.CharField', [], {'max_length': '50', 'db_index': 'True'}),
            'type': ('django.db.models.fields.CharField', [], {'max_length': '10'}),
            'users': ('django.db.models.fields.related.ManyToManyField', [], {'to': u"orm['vkontakte_users.User']", 'symmetrical': 'False'})
        },
        u'vkontakte_groups_migration.groupmigration': {
            'Meta': {'ordering': "('group', 'time', '-id')", 'unique_together': "(('group', 'time'),)", 'object_name': 'GroupMigration', 'db_table': "'vkontakte_groups_groupstatmembers'"},
            'group': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'migrations'", 'to': u"orm['vkontakte_groups.Group']"}),
            'hidden': ('django.db.models.fields.BooleanField', [], {'default': 'False', 'db_index': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'members_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_deactivated_entered_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_deactivated_entered_ids': ('picklefield.fields.PickledObjectField', [], {'default': '[]'}),
            'members_deactivated_left_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_deactivated_left_ids': ('picklefield.fields.PickledObjectField', [], {'default': '[]'}),
            'members_entered_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_entered_ids': ('picklefield.fields.PickledObjectField', [], {'default': '[]'}),
            'members_has_avatar_entered_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_has_avatar_entered_ids': ('picklefield.fields.PickledObjectField', [], {'default': '[]'}),
            'members_has_avatar_left_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_has_avatar_left_ids': ('picklefield.fields.PickledObjectField', [], {'default': '[]'}),
            'members_ids': ('picklefield.fields.PickledObjectField', [], {'default': '[]'}),
            'members_left_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_left_ids': ('picklefield.fields.PickledObjectField', [], {'default': '[]'}),
            'offset': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'time': ('django.db.models.fields.DateTimeField', [], {'null': 'True', 'db_index': 'True'})
        },
        u'vkontakte_groups_migration.groupmigrationnew': {
            'Meta': {'ordering': "('group', 'time', '-id')", 'unique_together': "(('group', 'time'),)", 'object_name': 'GroupMigrationNew', 'db_table': "'vkontakte_groups_migration_groupmigration'"},
            'group': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'migrations_new'", 'to': u"orm['vkontakte_groups.Group']"}),
            'hidden': ('django.db.models.fields.BooleanField', [], {'default': 'False'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'members_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_deactivated_entered_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_deactivated_left_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_entered_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_has_avatar_entered_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_has_avatar_left_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'members_left_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'offset': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'time': ('django.db.models.fields.DateTimeField', [], {'null': 'True', 'db_index': 'True'})
        },
        u'vkontakte_groups_migration.groupmigrationuser': {
            'Meta': {'unique_together': "(('migration', 'user_id'),)", 'object_name': 'GroupMigrationUser'},
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'migration': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'users'", 'to': u"orm['vkontakte_groups_migration.GroupMigrationNew']"}),
            'user_id': ('django.db.models.fields.PositiveIntegerField', [], {})
        },
        u'vkontakte_places.city': {
            'Meta': {'ordering': "['name']", 'object_name': 'City'},
            'area': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'country': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'cities'", 'null': 'True', 'to': u"orm['vkontakte_places.Country']"}),
            'fetched': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True', 'null': 'True', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'region': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'remote_id': ('django.db.models.fields.BigIntegerField', [], {'unique': 'True'})
        },
        u'vkontakte_places.country': {
            'Meta': {'ordering': "['name']", 'object_name': 'Country'},
            'fetched': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True', 'null': 'True', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'name': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'remote_id': ('django.db.models.fields.BigIntegerField', [], {'unique': 'True'})
        },
        u'vkontakte_users.user': {
            'Meta': {'ordering': "['remote_id']", 'object_name': 'User'},
            'about': ('django.db.models.fields.TextField', [], {}),
            'activity': ('django.db.models.fields.TextField', [], {}),
            'albums': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'audios': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'bdate': ('django.db.models.fields.CharField', [], {'max_length': '100'}),
            'books': ('django.db.models.fields.TextField', [], {}),
            'city': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['vkontakte_places.City']", 'null': 'True', 'on_delete': 'models.SET_NULL'}),
            'counters_updated': ('django.db.models.fields.DateTimeField', [], {'null': 'True'}),
            'country': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['vkontakte_places.Country']", 'null': 'True', 'on_delete': 'models.SET_NULL'}),
            'facebook': ('django.db.models.fields.CharField', [], {'max_length': '500'}),
            'facebook_name': ('django.db.models.fields.CharField', [], {'max_length': '500'}),
            'faculty': ('django.db.models.fields.PositiveIntegerField', [], {'null': 'True'}),
            'faculty_name': ('django.db.models.fields.CharField', [], {'max_length': '500'}),
            'fetched': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True', 'null': 'True', 'blank': 'True'}),
            'first_name': ('django.db.models.fields.CharField', [], {'max_length': '200'}),
            'followers': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'friends': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'friends_count': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'friends_users': ('django.db.models.fields.related.ManyToManyField', [], {'related_name': "'followers_users'", 'symmetrical': 'False', 'to': u"orm['vkontakte_users.User']"}),
            'games': ('django.db.models.fields.TextField', [], {}),
            'graduation': ('django.db.models.fields.PositiveIntegerField', [], {'null': 'True'}),
            'has_mobile': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'}),
            'home_phone': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'interests': ('django.db.models.fields.TextField', [], {}),
            'last_name': ('django.db.models.fields.CharField', [], {'max_length': '200'}),
            'livejournal': ('django.db.models.fields.CharField', [], {'max_length': '500'}),
            'mobile_phone': ('django.db.models.fields.CharField', [], {'max_length': '50'}),
            'movies': ('django.db.models.fields.TextField', [], {}),
            'mutual_friends': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'notes': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'photo': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'photo_big': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'photo_medium': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'photo_medium_rec': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'photo_rec': ('django.db.models.fields.URLField', [], {'max_length': '200'}),
            'rate': ('django.db.models.fields.PositiveIntegerField', [], {'null': 'True'}),
            'relation': ('django.db.models.fields.SmallIntegerField', [], {'null': 'True'}),
            'remote_id': ('django.db.models.fields.BigIntegerField', [], {'unique': 'True'}),
            'screen_name': ('django.db.models.fields.CharField', [], {'max_length': '100', 'db_index': 'True'}),
            'sex': ('django.db.models.fields.IntegerField', [], {'null': 'True'}),
            'skype': ('django.db.models.fields.CharField', [], {'max_length': '500'}),
            'subscriptions': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'sum_counters': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'timezone': ('django.db.models.fields.IntegerField', [], {'null': 'True'}),
            'tv': ('django.db.models.fields.TextField', [], {}),
            'twitter': ('django.db.models.fields.CharField', [], {'max_length': '500'}),
            'university': ('django.db.models.fields.PositiveIntegerField', [], {'null': 'True'}),
            'university_name': ('django.db.models.fields.CharField', [], {'max_length': '500'}),
            'user_photos': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'user_videos': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'videos': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0'}),
            'wall_comments': ('django.db.models.fields.NullBooleanField', [], {'null': 'True', 'blank': 'True'})
        },
        u'vkontakte_wall.comment': {
            'Meta': {'ordering': "['post', '-date']", 'object_name': 'Comment'},
            'author_content_type': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'comments'", 'to': u"orm['contenttypes.ContentType']"}),
            'author_id': ('django.db.models.fields.PositiveIntegerField', [], {'db_index': 'True'}),
            'date': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True'}),
            'fetched': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True', 'null': 'True', 'blank': 'True'}),
            'from_id': ('django.db.models.fields.IntegerField', [], {'null': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'like_users': ('django.db.models.fields.related.ManyToManyField', [], {'related_name': "'like_comments'", 'symmetrical': 'False', 'to': u"orm['vkontakte_users.User']"}),
            'likes': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0', 'db_index': 'True'}),
            'post': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'wall_comments'", 'to': u"orm['vkontakte_wall.Post']"}),
            'raw_html': ('django.db.models.fields.TextField', [], {}),
            'raw_json': ('annoying.fields.JSONField', [], {'default': '{}', 'null': 'True'}),
            'remote_id': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': "'20'"}),
            'reply_for_content_type': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'replies'", 'null': 'True', 'to': u"orm['contenttypes.ContentType']"}),
            'reply_for_id': ('django.db.models.fields.PositiveIntegerField', [], {'null': 'True', 'db_index': 'True'}),
            'reply_to': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['vkontakte_wall.Comment']", 'null': 'True'}),
            'text': ('django.db.models.fields.TextField', [], {}),
            'wall_owner_content_type': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'vkontakte_wall_comments'", 'to': u"orm['contenttypes.ContentType']"}),
            'wall_owner_id': ('django.db.models.fields.PositiveIntegerField', [], {'db_index': 'True'})
        },
        u'vkontakte_wall.post': {
            'Meta': {'ordering': "['wall_owner_id', '-date']", 'object_name': 'Post'},
            'attachments': ('django.db.models.fields.TextField', [], {}),
            'author_content_type': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'vkontakte_posts'", 'to': u"orm['contenttypes.ContentType']"}),
            'author_id': ('django.db.models.fields.PositiveIntegerField', [], {'db_index': 'True'}),
            'comments': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0', 'db_index': 'True'}),
            'copy_owner_content_type': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'vkontakte_wall_copy_posts'", 'null': 'True', 'to': u"orm['contenttypes.ContentType']"}),
            'copy_owner_id': ('django.db.models.fields.PositiveIntegerField', [], {'null': 'True', 'db_index': 'True'}),
            'copy_post': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['vkontakte_wall.Post']", 'null': 'True'}),
            'copy_text': ('django.db.models.fields.TextField', [], {}),
            'date': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True'}),
            'fetched': ('django.db.models.fields.DateTimeField', [], {'db_index': 'True', 'null': 'True', 'blank': 'True'}),
            'geo': ('django.db.models.fields.TextField', [], {}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'like_users': ('django.db.models.fields.related.ManyToManyField', [], {'related_name': "'like_posts'", 'symmetrical': 'False', 'to': u"orm['vkontakte_users.User']"}),
            'likes': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0', 'db_index': 'True'}),
            'media': ('django.db.models.fields.TextField', [], {}),
            'online': ('django.db.models.fields.PositiveSmallIntegerField', [], {'null': 'True'}),
            'post_source': ('django.db.models.fields.TextField', [], {}),
            'raw_html': ('django.db.models.fields.TextField', [], {}),
            'raw_json': ('annoying.fields.JSONField', [], {'default': '{}', 'null': 'True'}),
            'remote_id': ('django.db.models.fields.CharField', [], {'unique': 'True', 'max_length': "'20'"}),
            'reply_count': ('django.db.models.fields.PositiveSmallIntegerField', [], {'null': 'True'}),
            'repost_users': ('django.db.models.fields.related.ManyToManyField', [], {'related_name': "'repost_posts'", 'symmetrical': 'False', 'to': u"orm['vkontakte_users.User']"}),
            'reposts': ('django.db.models.fields.PositiveIntegerField', [], {'default': '0', 'db_index': 'True'}),
            'signer_id': ('django.db.models.fields.PositiveIntegerField', [], {'null': 'True'}),
            'text': ('django.db.models.fields.TextField', [], {}),
            'wall_owner_content_type': ('django.db.models.fields.related.ForeignKey', [], {'related_name': "'vkontakte_wall_posts'", 'to': u"orm['contenttypes.ContentType']"}),
            'wall_owner_id': ('django.db.models.fields.PositiveIntegerField', [], {'db_index': 'True'})
        }
    }

    complete_apps = ['vkontakte_groups_migration']