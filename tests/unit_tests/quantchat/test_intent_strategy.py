# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from datetime import UTC
from datetime import datetime

import pytest

from nautilus_trader.quantchat.intent_strategy import _parse_utc_datetime


class TestIntentStrategyTimeParsing:
    def test_parse_utc_datetime_with_empty_value_returns_none(self):
        # Arrange, Act, Assert
        assert _parse_utc_datetime("", "startTime") is None

    def test_parse_utc_datetime_with_invalid_value_fails_fast(self):
        # Arrange, Act, Assert
        with pytest.raises(ValueError, match=r"runtimeBindings\.startTime"):
            _parse_utc_datetime("not-a-timestamp", "startTime")

    def test_parse_utc_datetime_with_z_suffix_returns_utc_datetime(self):
        # Arrange, Act
        result = _parse_utc_datetime("2026-05-23T03:30:00Z", "startTime")

        # Assert
        assert result == datetime(2026, 5, 23, 3, 30, tzinfo=UTC)
