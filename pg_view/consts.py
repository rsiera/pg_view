import os

# some global variables for keyboard output
freeze = False
filter_aux = True
autohide_fields = False
display_units = False
notrim = False
realtime = False

USER_HZ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
NCURSES_CUSTOM_OUTPUT_FIELDS = ['header', 'prefix', 'prepend_column_headers']
TICK_LENGTH = 1
RD = 1
