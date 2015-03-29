#!/bin/sh
bin/hadoop fs -rm -r -f table1_c
bin/hadoop fs -rm -r -f table2_c
bin/hadoop fs -rm -r -f table3
bin/hadoop jar TableJoin-1.0.jar tw.edu.ncku.ee.hpds.db.TableConvert table1 table_schema1.txt table1_c
bin/hadoop jar TableJoin-1.0.jar tw.edu.ncku.ee.hpds.db.TableConvert table2 table_schema2.txt table2_c
bin/hadoop jar TableJoin-1.0.jar tw.edu.ncku.ee.hpds.db.TableMerge table1_c table2_c table_schema3.txt table3
