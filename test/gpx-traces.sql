-- Test data for the --gpx output. Load it on top of
-- liechtenstein-2013-08-03.dmp, then write gpx-traces.dmp with only the
-- tables a GPX dump needs:
--
--   pg_dump --format=custom --data-only \
--     --table=users --table=gpx_files --table=gpx_tracks --table=gpx_file_tags \
--     --file=gpx-traces.dmp osm

-- display name that needs escaping in the trace URL
UPDATE users SET display_name = 'mike hd/ü' WHERE id = 6326;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE gpx_tracks (
  gpx_id bigint NOT NULL REFERENCES gpx_files (id),
  trackid integer NOT NULL,
  segment integer NOT NULL,
  geom geometry(GeometryZM, 4326) NOT NULL,
  started_at timestamp without time zone,
  ended_at timestamp without time zone,
  PRIMARY KEY (gpx_id, trackid, segment)
);

INSERT INTO gpx_files (id, user_id, visible, name, description, "timestamp", inserted, visibility) VALUES
  -- written: two tracks, the first one split in two segments
  (1, 1, true, 'Morning ride', 'Vaduz <loop> & back', '2013-08-01 12:00:00', true, 'identifiable'),
  -- skipped: not identifiable
  (2, 2680, true, 'trackable', '', '2013-08-01 12:00:00', true, 'trackable'),
  (3, 2680, true, 'public', '', '2013-08-01 12:00:00', true, 'public'),
  (4, 2680, true, 'private', '', '2013-08-01 12:00:00', true, 'private'),
  -- skipped: deleted
  (5, 2680, false, 'deleted', '', '2013-08-01 12:00:00', true, 'identifiable'),
  -- written without URL: the user has no public data
  (6, -1, true, E'bad\001name', 'no url', '2013-08-01 12:00:00', true, 'identifiable'),
  -- skipped: no tracks
  (7, 6326, true, 'no tracks', '', '2013-08-01 12:00:00', false, 'identifiable'),
  -- written: escaped display name in the URL
  (8, 6326, true, 'Single point', '', '2013-08-01 12:00:00', true, 'identifiable');

-- inserted out of order, planet-dump-ng has to sort them
INSERT INTO gpx_tracks (gpx_id, trackid, segment, geom) VALUES
  (1, 1, 0, ST_SetSRID(ST_MakePoint(9.53, 47.15, 470, extract(epoch FROM timestamp '2013-08-01 10:01:40')), 4326)),
  (1, 0, 1, ST_SetSRID(ST_MakeLine(
      ST_MakePoint(9.522, 47.142, 0, extract(epoch FROM timestamp '2013-08-01 10:00:20')),
      ST_MakePoint(9.523, 47.143, 460, '-infinity')), 4326)),
  (1, 0, 0, ST_SetSRID(ST_MakeLine(
      ST_MakePoint(9.52, 47.14, 450.5, extract(epoch FROM timestamp '2013-08-01 10:00:00')),
      ST_MakePoint(9.521, 47.141, 455, extract(epoch FROM timestamp '2013-08-01 10:00:10'))), 4326)),
  (2, 0, 0, ST_SetSRID(ST_MakePoint(9.5, 47.1, 0, 0), 4326)),
  (3, 0, 0, ST_SetSRID(ST_MakePoint(9.5, 47.1, 0, 0), 4326)),
  (4, 0, 0, ST_SetSRID(ST_MakePoint(9.5, 47.1, 0, 0), 4326)),
  (5, 0, 0, ST_SetSRID(ST_MakePoint(9.5, 47.1, 0, 0), 4326)),
  (6, 0, 0, ST_SetSRID(ST_MakePoint(9.54, 47.16, 0, '-infinity'), 4326)),
  (8, 0, 0, ST_SetSRID(ST_MakePoint(9.55, 47.17, 480, extract(epoch FROM timestamp '1969-12-31 23:59:59')), 4326));

-- inserted out of order: the output follows id, not the insert order
INSERT INTO gpx_file_tags (gpx_id, tag, id) VALUES
  (1, 'bike', 2),
  (1, 'vaduz', 1),
  -- trackable: tags are not written
  (2, 'secret', 3),
  -- needs escaping
  (8, 'a<b', 4);
