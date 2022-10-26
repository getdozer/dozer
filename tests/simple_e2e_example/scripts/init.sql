CREATE SEQUENCE users_id_seq;

CREATE TABLE users (
  id integer NOT NULL DEFAULT nextval('users_id_seq') CONSTRAINT users_pk PRIMARY KEY,
  email varchar(255) NOT NULL,
  phone varchar(255) NOT NULL
);

CREATE UNIQUE INDEX email_index ON users (email);

ALTER SEQUENCE users_id_seq OWNED BY users.id;

-- Generate data in users table
INSERT INTO users (email, phone)
SELECT
  concat(id, '@', 'google.com'),
  concat(id, '1233422', id * 3)
FROM
  GENERATE_SERIES(1, 1000) AS id;

