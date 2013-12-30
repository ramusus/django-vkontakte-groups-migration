from vkontakte_groups.factories import GroupFactory
from models import GroupMigration, GroupMembership
from datetime import datetime
import factory

class GroupMigrationFactory(factory.DjangoModelFactory):
    FACTORY_FOR = GroupMigration

    group = factory.SubFactory(GroupFactory)
    time = datetime.now()