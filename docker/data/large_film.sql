CREATE DOMAIN public.year AS integer CONSTRAINT year_check CHECK (((VALUE >= 1901) AND (VALUE <= 2155)));

CREATE TYPE public.mpaa_rating AS ENUM (
  'G',
  'PG',
  'PG-13',
  'R',
  'NC-17'
);

CREATE SEQUENCE public.film_film_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;

CREATE TABLE public.film (
  film_id integer DEFAULT nextval('public.film_film_id_seq'::regclass) NOT NULL,
  title text NOT NULL,
  description text,
  release_year public.year,
  language_id integer NOT NULL,
  original_language_id integer,
  rental_duration smallint DEFAULT 3 NOT NULL,
  rental_rate numeric(4, 2) DEFAULT 4.99 NOT NULL,
  length smallint,
  replacement_cost numeric(5, 2) DEFAULT 19.99 NOT NULL,
  rating public.mpaa_rating DEFAULT 'G' ::public.mpaa_rating,
  last_update timestamp with time zone DEFAULT now() NOT NULL,
  special_features text[],
  fulltext tsvector NOT NULL
);

