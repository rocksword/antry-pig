/*
* Myscript.pig
* Another line of comment
*/

log = LOAD '$input' AS (user, time, query);
lmt = LIMIT log $size;
DUMP lmt;

-- End of program