# This is a solution to the weblog challenge.

* See __WeblogAnalyzerSpec__ for details, and as an entry point to this solution.

## Known issues:

* Something's broken with the timestamp parser, which doesn't allow proper parsing of entries:
    * Text '2015-07-22T09:00:28.140219Z' could not be parsed: Unable to obtain LocalDateTime from TemporalAccessor: {MicroOfSecond=140219, MilliOfSecond=140, NanoOfSecond=140219000, InstantSeconds=1437555628},ISO of type java.time.format.Parsed
    * See: `WeblogEntry:56`