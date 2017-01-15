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
   comment        CHARACTER VARYING (131072),
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

------------------------------------------------------------------
--  TABLE items_to_transfer
------------------------------------------------------------------

CREATE TABLE syncdb.objects_to_transfer
(
   "transfer_order" integer,
   "schema"      	CHARACTER VARYING (50),
   "object_name"    CHARACTER VARYING (200),
   "type"        	CHARACTER VARYING (50),
   "number_of_rows" integer,
   "transfer"   	boolean,
   "refresh"   	 	boolean,
   "analyze"     	boolean,
);

------------------------------------------------------------------
--  VIEW all_items
--
--  View to get all elements on the database that are 
--  transferrable.
--
------------------------------------------------------------------

CREATE OR REPLACE VIEW syncdb.all_transferable_objects
AS
     SELECT row_number () OVER (ORDER BY c.oid) AS "transfer_order",
            n.nspname                         AS "schema",
            c.relname                         AS "object_name",
            --c.oid::regclass::text,
            CASE
               WHEN c.relkind = 'i' THEN 'INDEX'
               WHEN c.relkind = 't' THEN 'TOAST'
               WHEN c.relkind = 'r' THEN 'TABLE'
               WHEN c.relkind = 'v' THEN 'VIEW'
               WHEN c.relkind = 'c' THEN 'COMPOSITE TYPE'
               WHEN c.relkind = 'S' THEN 'SEQUENCE'
               WHEN c.relkind = 'm' THEN 'MATERIALIZED VIEW'
               ELSE c.relkind::text
            END
               AS "type",
            c.reltuples as "number_of_rows",
            CASE
               WHEN c.relkind = 'r' THEN true
               WHEN c.relkind = 'v' THEN true
               WHEN c.relkind = 'm' THEN true
               ELSE false
            END
               AS "transfer",
            CASE
               WHEN c.relkind = 'm' THEN true
               ELSE false
            END
               AS "refresh",
            CASE
               WHEN c.relkind = 'i' THEN true
               WHEN c.relkind = 'r' THEN true
               WHEN c.relkind = 'v' THEN true
               WHEN c.relkind = 'm' THEN true
               ELSE false
            END
               AS "analyze"
       FROM pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE     relkind IN ('m', 'v', 'r')
            AND n.nspname IN ('transfer', 'public', 'tempviews')
            AND c.relname NOT LIKE 'pg_%'
   ORDER BY c.oid;

