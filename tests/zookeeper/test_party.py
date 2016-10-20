import unittest

from mock import Mock
from kazoo.exceptions import NoNodeError

from kiel.zookeeper import party


class PartyTests(unittest.TestCase):

    def test_start_ensures_path_and_watches_changes(self):
        client = Mock()

        def collect_callback(fn):
            client.change_callback = fn

        client.ChildrenWatch.return_value.side_effect = collect_callback

        on_change = Mock()

        p = party.Party(client, "host.local", "/my/party", on_change)

        p.start()

        client.ensure_path.assert_called_once_with("/my/party")
        client.ChildrenWatch.assert_called_once_with("/my/party")

        assert on_change.called is False

        client.change_callback(["foo", "bar"])

        on_change.assert_called_once_with(["foo", "bar"])

    def test_join_when_znode_does_not_exist(self):
        client = Mock()
        client.exists.return_value = None

        p = party.Party(client, "host.local", "/my/party", Mock())

        p.join()

        client.exists.assert_called_once_with("/my/party/host.local")
        client.create.assert_called_once_with(
            "/my/party/host.local", ephemeral=True, makepath=True
        )

    def test_join_when_znode_belongs_to_someone_else(self):
        client = Mock()
        client.exists.return_value = Mock(owner_session_id=1234)
        client.client_id = (4321, 0)

        p = party.Party(client, "host.local", "/my/party", Mock())

        p.join()

        client.transaction.assert_called_once_with()
        transaction = client.transaction.return_value
        transaction.delete.assert_called_once_with("/my/party/host.local")
        transaction.create.assert_called_once_with(
            "/my/party/host.local", ephemeral=True
        )
        transaction.commit.assert_called_once_with()

    def test_join_when_znode_belongs_to_us(self):
        client = Mock()
        client.exists.return_value = Mock(owner_session_id=1234)
        client.client_id = (1234, 0)

        p = party.Party(client, "host.local", "/my/party", Mock())

        p.join()

        assert client.create.called is False
        assert client.transaction.called is False

    def test_leave(self):
        client = Mock()

        p = party.Party(client, "host.local", "/my/party", Mock())

        p.leave()

        client.delete.assert_called_once_with("/my/party/host.local")

    def test_leave_znode_does_not_exist(self):
        client = Mock()
        client.delete.side_effect = NoNodeError

        p = party.Party(client, "host.local", "/my/party", Mock())

        p.leave()

        client.delete.assert_called_once_with("/my/party/host.local")
