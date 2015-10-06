import logging

from kazoo import exceptions


log = logging.getLogger(__name__)


class Party(object):
    """
    Represents a "party" recipe in Zookeeper.

    A party is a recipe where various clients "join" or "leave" (a loss of
    connection constituting a "leave") and each member is notified when
    membership changes.

    This is used in the Zookeeper-based ``GroupedConsumer`` in order to
    determine who and how many hosts to divvy up partitions to.
    """
    def __init__(self, client, member_name, path, on_change):
        self.client = client

        self.member_name = member_name
        self.path = path

        self.on_change = on_change

    def start(self):
        """
        Simple method that sets up the membership change callback.

        Expected to be called by potential members before the `join()` method.
        """
        self.client.ensure_path(self.path)

        @self.client.ChildrenWatch(self.path)
        def member_change(members):
            self.on_change(members)

    def join(self):
        """
        Establishes the client as a "member" of the party.

        This is done by creating an ephemeral child node of the party's root
        path unique to this member.  If the path of the child node exists but
        this client isn't the owner, the node is re-created in a transaction
        to establish ownership.

        .. note::
           It is important that the child node is *ephemeral* so that lost
           connections are indistinguishable from "leaving" the party.
        """
        log.info("Joining %s party as %s", self.path, self.member_name)

        path = "/".join([self.path, self.member_name])

        znode = self.client.exists(path)

        if not znode:
            log.debug("ZNode at %s does not exist, creating new one.", path)
            self.client.create(path, ephemeral=True, makepath=True)
        elif znode.owner_session_id != self.client.client_id[0]:
            log.debug("ZNode at %s not owned by us, recreating.", path)
            txn = self.client.transaction()
            txn.delete(path)
            txn.create(path, ephemeral=True)
            txn.commit()

    def leave(self):
        """
        Simple method that marks the client as having left the party.

        A simple matter of deleting the ephemeral child node associated with
        the client.  If the node is already deleted this becomes a no-op.
        """
        log.info("Leaving %s party as %s", self.path, self.member_name)
        path = "/".join([self.path, self.member_name])

        try:
            self.client.delete(path)
        except exceptions.NoNodeError:
            pass
