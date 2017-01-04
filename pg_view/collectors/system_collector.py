from pg_view import loggers
from pg_view.collectors.base_collector import BaseStatCollector
from pg_view.consts import RD


class SystemStatCollector(BaseStatCollector):
    """ Collect global system statistics, i.e. CPU/IO usage, not including memory. """

    PROC_STAT_FILENAME = '/proc/stat'

    def __init__(self):
        super(SystemStatCollector, self).__init__()

        self.transform_list_data = [
            {'out': 'utime', 'in': 0, 'fn': float},
            {'out': 'stime', 'in': 2, 'fn': float},
            {'out': 'idle', 'in': 3, 'fn': float},
            {'out': 'iowait', 'in': 4, 'fn': float},
            {'out': 'irq', 'in': 5, 'fn': float},
            {'out': 'softirq', 'in': 6, 'fn': float, 'optional': True},
            {'out': 'steal', 'in': 7, 'fn': float, 'optional': True},
            {'out': 'guest', 'in': 8, 'fn': float, 'optional': True}
        ]

        self.transform_dict_data = [
            {'out': 'ctxt', 'fn': float},
            {'out': 'cpu'},
            {'out': 'running', 'in': 'procs_running', 'fn': int},
            {'out': 'blocked', 'in': 'procs_blocked', 'fn': int}
        ]

        self.diff_generator_data = [
            {'out': 'utime', 'fn': self._cpu_time_diff},
            {'out': 'stime', 'fn': self._cpu_time_diff},
            {'out': 'idle', 'fn': self._cpu_time_diff},
            {'out': 'iowait', 'fn': self._cpu_time_diff},
            {'out': 'irq', 'fn': self._cpu_time_diff},
            {'out': 'softirq', 'fn': self._cpu_time_diff},
            {'out': 'steal', 'fn': self._cpu_time_diff},
            {'out': 'guest', 'fn': self._cpu_time_diff},
            {'out': 'ctxt'},
            {'out': 'running', 'diff': False},
            {'out': 'blocked', 'diff': False},
        ]

        self.output_transform_data = [
            {
                'out': 'utime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'minw': 5,
                'pos': 0,
                'warning': 50,
                'critial': 90,
            },
            {
                'out': 'stime',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 1,
                'minw': 5,
                'warning': 10,
                'critical': 30,
            },
            {
                'out': 'idle',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 2,
                'minw': 5,
            },
            {
                'out': 'iowait',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
                'pos': 3,
                'minw': 5,
                'warning': 20,
                'critical': 50,
            },
            {
                'out': 'irq',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'soft',
                'in': 'softirq',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'steal',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'guest',
                'units': '%',
                'fn': self.unit_converter.time_diff_to_percent,
                'round': RD,
            },
            {
                'out': 'ctxt',
                'units': '/s',
                'fn': int,
                'pos': 4,
            },
            {
                'out': 'run',
                'in': 'running',
                'pos': 5,
                'minw': 3,
            },
            {
                'out': 'block',
                'in': 'blocked',
                'pos': 6,
                'minw': 3,
                'warning': 1,
                'critial': 5,
            },
        ]

        self.previos_total_cpu_time = 0
        self.current_total_cpu_time = 0
        self.cpu_time_diff = 0
        self.ncurses_custom_fields = {'header': False, 'prefix': 'sys: ', 'prepend_column_headers': True}
        self.postinit()

    def refresh(self):
        """ Read data from global /proc/stat """

        result = {}
        stat_data = self._read_proc_stat()
        cpu_data = self._read_cpu_data(stat_data.get('cpu', []))
        result.update(stat_data)
        result.update(cpu_data)
        self._refresh_cpu_time_values(cpu_data)
        self._do_refresh([result])
        return result

    def _refresh_cpu_time_values(self, cpu_data):
        # calculate the sum of all CPU indicators and store it.
        total_cpu_time = sum(v for v in cpu_data.values() if v is not None)
        # calculate actual differences in cpu time values
        self.previos_total_cpu_time = self.current_total_cpu_time
        self.current_total_cpu_time = total_cpu_time
        self.cpu_time_diff = self.current_total_cpu_time - self.previos_total_cpu_time

    def _cpu_time_diff(self, colname, current, previous):
        if current.get(colname) and previous.get(colname) and self.cpu_time_diff > 0:
            return (current[colname] - previous[colname]) / self.cpu_time_diff
        return None

    def _read_cpu_data(self, cpu_row):
        """ Parse the cpu row from /proc/stat """

        return self._transform_input(cpu_row)

    def output(self, displayer):
        return super(SystemStatCollector, self).output(displayer, before_string='System statistics:', after_string='\n')
