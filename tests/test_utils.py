import os
from unittest import TestCase

import mock
import psutil

from common import TEST_DIR
from pg_view.exceptions import InvalidConnectionParamError
from pg_view.models.parsers import connection_params
from pg_view.utils import UnitConverter, read_configuration, validate_autodetected_conn_param, \
    output_method_is_valid, get_process_or_none


class UnitConverterTest(TestCase):
    def test_kb_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(3, UnitConverter.kb_to_mbytes(3072))

    def test_kb_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.kb_to_mbytes(None))

    def test_sectors_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(10, UnitConverter.sectors_to_mbytes(20480))

    def test_sectors_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.sectors_to_mbytes(None))

    def test_bytes_to_mbytes_should_convert_when_ok(self):
        self.assertEqual(2, UnitConverter.bytes_to_mbytes(2097152))

    def test_bytes_to_mbytes_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.bytes_to_mbytes(None))

    @mock.patch('pg_view.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_convert_when_ok(self):
        self.assertEqual(5, UnitConverter.ticks_to_seconds(500))

    @mock.patch('pg_view.consts.USER_HZ', 100)
    def test_ticks_to_seconds_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.ticks_to_seconds(None))

    def test_time_diff_to_percent_should_convert_when_ok(self):
        self.assertEqual(1000.0, UnitConverter.time_diff_to_percent(10))

    def test_time_diff_to_percent_should_return_none_when_none(self):
        self.assertIsNone(UnitConverter.time_diff_to_percent(None))


class ReadConfigurationTest(TestCase):
    def test_read_configuration_should_return_none_when_not_config_file_name(self):
        self.assertIsNone(read_configuration(None))

    @mock.patch('pg_view.loggers.logger')
    def test_read_configuration_should_return_none_when_cannot_read_file(self, mocked_logger):
        config_file_path = os.path.join(TEST_DIR, 'not-existing')
        self.assertIsNone(read_configuration(config_file_path))
        expected_msg = 'Configuration file {0} is empty or not found'.format(config_file_path)
        mocked_logger.error.assert_called_with(expected_msg)

    def test_read_configuration_should_return_config_data_when_config_file_ok(self):
        config_file_path = os.path.join(TEST_DIR, 'configs', 'default_ok.cfg')
        expected_conf = {'testdb': {
            'host': '/var/run/postgresql', 'port': '5432', 'dbname': 'postgres', 'user': 'username'}
        }
        config = read_configuration(config_file_path)
        self.assertDictEqual(expected_conf, config)

    def test_read_configuration_should_skip_empty_options_when_not_exist(self):
        config_file_path = os.path.join(TEST_DIR, 'configs', 'default_with_none_user.cfg')
        expected_conf = {'testdb': {
            'host': '/var/run/postgresql', 'port': '5432', 'dbname': 'postgres'}
        }
        config = read_configuration(config_file_path)
        self.assertDictEqual(expected_conf, config)


class ValidateConnParamTest(TestCase):
    def test_validate_autodetected_conn_param_should_return_none_when_no_user_dbname(self):
        self.assertIsNone(validate_autodetected_conn_param(None, '9.3', '/var/run/postgresql', {}))

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_different_dbnames(self):
        conn_parameters = connection_params(pid=1049, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param('/var/lib/postgresql/9.5/main', 9.3, '/var/run/postgresql',
                                             conn_parameters)

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_no_result_work_dir(self):
        conn_parameters = connection_params(pid=1049, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param('/var/lib/postgresql/9.3/main', 9.3, '', conn_parameters)

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_no_connection_params_pid(self):
        conn_parameters = connection_params(pid=None, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param(
                '/var/lib/postgresql/9.3/main', 9.3, '/var/run/postgresql', conn_parameters)

    def test_validate_autodetected_conn_param_should_raise_invalid_param_when_different_versions(self):
        conn_parameters = connection_params(pid=2, version=9.3, dbname='/var/lib/postgresql/9.3/main')
        with self.assertRaises(InvalidConnectionParamError):
            validate_autodetected_conn_param(
                '/var/lib/postgresql/9.3/main', 9.5, '/var/run/postgresql', conn_parameters)


class ValidatorTest(TestCase):
    def test_output_method_is_valid_should_return_true_when_valid(self):
        for output in ['console', 'json', 'curses']:
            self.assertTrue(output_method_is_valid(output))

    def test_output_method_is_valid_should_return_false_when_invalid(self):
        for output in ['test', 'foo', 1]:
            self.assertFalse(output_method_is_valid(output))


class GetProcessOrNoneTest(TestCase):
    @mock.patch('pg_view.loggers.logger')
    @mock.patch('pg_view.utils.psutil.Process')
    def test_get_process_or_none_should_return_none_when_no_such_process_raised(self, mocked_process, mocked_logger):
        mocked_process.side_effect = psutil.NoSuchProcess('')
        self.assertIsNone(get_process_or_none(1049))
        mocked_logger.warning.assert_called_with('Process no. 1049 disappeared while processing')

    @mock.patch('pg_view.loggers.logger')
    @mock.patch('pg_view.utils.psutil.Process')
    def test_get_process_or_none_should_return_none_when_access_denied_raised(self, mocked_process, mocked_logger):
        mocked_process.side_effect = psutil.AccessDenied('')
        self.assertIsNone(get_process_or_none(1049))
        mocked_logger.warning.assert_called_with('No permission to access process no. 1049')

    @mock.patch('pg_view.utils.psutil.Process')
    def test_get_process_or_none_should_return_process_when_no_errors(self, mocked_process):
        process = mock.Mock()
        mocked_process.return_value = process
        self.assertEqual(process, get_process_or_none(1049))
