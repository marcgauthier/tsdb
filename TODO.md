need 

To reduce memory usage do not keep full [] item metric until it's compaction time, compaction should happen more often just keep adding entries in the chunk and change the last value. Unless it;s already the case. but do not save to disk right away.  Update the chunk to disk only when end of interval is reached or set another value to flush to disk then update. 


Create a benchmarks


