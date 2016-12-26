DROP SCHEMA syncdb;

CREATE SCHEMA syncdb;

CREATE TABLE syncdb.jobs
(
   id             INTEGER NOT NULL,
   job_start      TIMESTAMP (5) WITHOUT TIME ZONE,
   job_end        timestamp,
   status         VARCHAR (20) NOT NULL,
   num_of_tasks   INTEGER,
   chunk_size     INTEGER,
   db_from        VARCHAR (20) NOT NULL,
   db_to          VARCHAR (20) NOT NULL,
   next_job       timestamp,
   type           VARCHAR (20),
   "user"         VARCHAR (20) NOT NULL,
   PRIMARY KEY (id)
);

CREATE TABLE syncdb.job_detail
(
   id           INTEGER NOT NULL,
   job_id       INTEGER NOT NULL,
   event_date   timestamp NOT NULL,
   status       VARCHAR (20) NOT NULL,
   comment      VARCHAR (512),
   PRIMARY KEY (id)
);

CREATE TABLE syncdb.task
(
   id             INTEGER NOT NULL,
   jobid          INTEGER NOT NULL,
   "table"        VARCHAR (50) NOT NULL,
   tasknum        INTEGER NOT NULL,
   index_loaded   INTEGER NOT NULL,
   PRIMARY KEY (id)
);

