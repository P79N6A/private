CREATE TABLE `ontime` (
  `Year` Int32,
  `FlightDate` Date,
  `TailNum` String
) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/ontime', '{replica}') PARTITION BY FlightDate Order By (Year, FlightDate) SETTINGS index_granularity=8192