CREATE TABLE `ontime` (
  `Year` Int32,
  `FlightDate` Date,
  `TailNum` String
) ENGINE = MergeTree(FlightDate, (Year, FlightDate), 8192)