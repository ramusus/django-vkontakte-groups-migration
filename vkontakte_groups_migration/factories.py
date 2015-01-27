from vkontakte_groups.factories import GroupFactory
from models import GroupMigration, GroupMembership
from datetime import datetime
import factory

class GroupMigrationFactory(factory.DjangoModelFactory):
    FACTORY_FOR = GroupMigration

    group = factory.SubFactory(GroupFactory)
    time = datetime.now()

class GroupMembershipFactory(factory.DjangoModelFactory):
    FACTORY_FOR = GroupMembership

    group = factory.SubFactory(GroupFactory)
    user_id = factory.Sequence(lambda n: n)

    time_entered = None
    time_left = None