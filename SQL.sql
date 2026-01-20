
SQL Interview
===================
COALESCE:
it returns first non null values
it is basically used to handle null values in satging areas.
 select empid,coalesce(bonus,0) from employee
 
 
 DELETE vs DROP vs TRUNCATE:
 =============================
 delete : removes one or more records based on condition
 truncate : removes all records from TABLE
 drop : removes all records along with structure aswell
 
Primary Key: Ensures uniqueness + not null, only one per table.
Unique Key: Ensures uniqueness, allows one NULL, can have multiple.
 
CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(100) NOT NULL,
    dept_id INT,
    salary NUMERIC CHECK (salary > 0),
    bonus NUMERIC DEFAULT 0,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);


IS NULL vs = NULL
You cannot check nulls with =. Must use IS NULL.

difference between in and between:
=======================================
in-> for range of avalues
between->range of values
select * from employee where salary range between 10000 and 50000;
select * from employee where empid in(101,102,103,104)

Numeric functions:
=======================
round(): rounds a number to a spefied number of disgits eg .. round(49.777,2) --49.77
ceil(): returns smallest value of greatest or equal value of given number eg..ceil(46.77)  --47
floor():returns largest value of smallest or equal value of given number eg.. floor(47.77) --47
abs():returns value without plus or minus symbol

String Functions:
======================
CONCAT:Combines two or more strings
eg:select concat(firstnme+''+lastname) from emp;
len/length():returns number of charactes in String
upper()/lower(): used to convert lower to upper and upper to lower eg.. select upper('navya') from emp
trim: removes leading and trailling spaces
ltrim:removes leading spaces
rtrim: removes trailling spaces
trim,ltrim,rtim basically used to clean messy data with extra spaces
substring: returns part of string substring('navyaakula',1,5) --index starts with 1 only
replace: all occurances of a string with new string
SELECT REPLACE('Data Engineer', 'Engineer', 'Analyst');
-- Output: 'Data Analyst'

INSTR() / CHARINDEX() / POSITION()-- find the position of a substring
instr(string,substring)

left/right:
left(): used to extract left most charactes basically to fetch year from DATE
right() :used to extract right most charates basically to fetch last phone number digits
SELECT LEFT('2025-08-30', 4);   -- '2025'
SELECT RIGHT('2025-08-30', 2);  -- '30'

CONCAT_WS() (Concat With Separator)
===================================
Definition: Concatenates strings with a given separator.

SELECT CONCAT_WS('-', '2025', '08', '30');  
-- Output: '2025-08-30'

Wild card operators(%,_)
%->allows multiple values
_->allos single value


