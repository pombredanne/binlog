from binlog import binlog
from binlog import reader, writer

# Binlog implementations.
BINLOG_IMPL = [binlog.TDSBinlog, binlog.CDSBinlog]

# Reader & Writer implementations.
RW_IMPL = [
    (reader.TDSReader, writer.TDSWriter),
    (reader.CDSReader, writer.CDSWriter)]
