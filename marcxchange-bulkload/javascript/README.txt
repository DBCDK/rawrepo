
Loading marcx records into raw repo.


This is a two stage process.
* First load all records
  $ jsinputtool -f xml -F records.js user/pass@host[:port]/database < marcxchange.xml
* Second load all relations (all referred records now exist)
  $ jsinputtool -f xml -F relations.js user/pass@host[:port]/database < marcxchange.xml

or use the load script:

 $ ./load.bash user:pass@host[:port]/database marcxchange.xml ...
