CREATE DATABASE wallaby;
CREATE DATABASE source;
CREATE DATABASE destination;

\connect source

CREATE TABLE public.orders (
  id bigint PRIMARY KEY,
  customer text NOT NULL,
  total_cents integer NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

\connect destination

CREATE TABLE public.orders (
  id bigint PRIMARY KEY,
  customer text NOT NULL,
  total_cents integer NOT NULL,
  updated_at timestamptz NOT NULL
);
