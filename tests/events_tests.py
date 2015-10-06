import unittest


from mock import patch, Mock

from kiel import events


class EventsTests(unittest.TestCase):

    def test_wait_on_event_with_timeout(self):
        mock_event = Mock()

        events.wait_on_event(mock_event, timeout=60)

        mock_event.wait.assert_called_once_with(60)

    @patch("kiel.events.six")
    def test_wait_on_event_uses_no_timeout_on_py3(self, mock_six):
        mock_six.PY2 = False

        mock_event = Mock()

        events.wait_on_event(mock_event)

        mock_event.wait.assert_called_once_with()

    @patch("kiel.events.sys")
    @patch("kiel.events.six")
    def test_wait_on_event_uses_maxint_on_py2(self, mock_six, mock_sys):
        mock_six.PY2 = True

        mock_event = Mock()
        mock_event.is_set.return_value = False

        def set_event(*args):
            mock_event.is_set.return_value = True

        # set the event when we wait, otherwise the while loop would go forever
        mock_event.wait.side_effect = set_event

        events.wait_on_event(mock_event)

        mock_event.wait.assert_called_once_with(mock_sys.maxint)

    @patch("kiel.events.sys")
    @patch("kiel.events.six")
    def test_wait_on_event_set_event_is_noop_on_py2(self, mock_six, mock_sys):
        mock_six.PY2 = True

        mock_event = Mock()
        mock_event.is_set.return_value = True

        events.wait_on_event(mock_event)

        assert mock_event.wait.called is False
