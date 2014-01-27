CREATE UNIQUE INDEX vkontakte_groups_migration_groupmembersh_time_entered_3col_uniq
ON vkontakte_groups_migration_groupmembership (group_id, user_id, time_entered)
WHERE time_entered IS NOT NULL;

CREATE UNIQUE INDEX vkontakte_groups_migration_groupmembersh_time_entered_2col_uniq
ON vkontakte_groups_migration_groupmembership (group_id, user_id)
WHERE time_entered IS NULL;

CREATE UNIQUE INDEX vkontakte_groups_migration_groupmembersh_time_left_3col_uniq
ON vkontakte_groups_migration_groupmembership (group_id, user_id, time_left)
WHERE time_left IS NOT NULL;

CREATE UNIQUE INDEX vkontakte_groups_migration_groupmembersh_time_left_2col_uniq 
ON vkontakte_groups_migration_groupmembership (group_id, user_id)
WHERE time_left IS NULL;
