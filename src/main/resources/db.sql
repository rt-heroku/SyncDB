DROP SCHEMA syncdb CASCADE ;

CREATE SCHEMA syncdb;

------------------------------------------------------------------
--  TABLE job
------------------------------------------------------------------

CREATE TABLE syncdb.job
(
   id            SERIAL,
   job_start     timestamp (5) WITH TIME ZONE,
   job_end       timestamp (6) WITH TIME ZONE,
   status        CHARACTER VARYING (20),
   num_of_jobs   integer,
   chunk_size    integer,
   db_from       CHARACTER VARYING (50),
   db_to         CHARACTER VARYING (50),
   next_job      timestamp (6) WITH TIME ZONE,
   type          CHARACTER VARYING (20),
   "user"        CHARACTER VARYING (50),
   jobid         CHARACTER VARYING (50),
   status_date   timestamp (5) WITH TIME ZONE,
   PRIMARY KEY (id)
);


------------------------------------------------------------------
--  TABLE job_detail
------------------------------------------------------------------

CREATE TABLE syncdb.job_detail
(
   id             SERIAL,
   jobid          CHARACTER VARYING (50),
   status_date    timestamp (6) WITH TIME ZONE,
   status         CHARACTER VARYING (20),
   comment        CHARACTER VARYING (4096),
   "table"        CHARACTER VARYING (20),
   num_of_tasks   INTEGER DEFAULT 0,
   maxid          INTEGER DEFAULT 0,
   job_num        INTEGER DEFAULT 0,
   PRIMARY KEY (id)
);


------------------------------------------------------------------
--  TABLE task
------------------------------------------------------------------

CREATE TABLE syncdb.task
(
   id             SERIAL,
   jobid          CHARACTER VARYING (50),
   "table"        CHARACTER VARYING (50),
   tasknum        integer,
   index_loaded   integer,
   status         CHARACTER VARYING (20),
   status_date    timestamp (5) WITH TIME ZONE,
   PRIMARY KEY (id)
);


