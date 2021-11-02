CREATE TABLE url (
    id INTEGER NOT NULL PRIMARY KEY,
    value TEXT UNIQUE NOT NULL
);
CREATE INDEX url_value ON url (value);

CREATE TABLE digest (
    id INTEGER NOT NULL PRIMARY KEY,
    value TEXT UNIQUE NOT NULL
);
CREATE INDEX digest_value ON digest (value);

CREATE TABLE mime_type (
    id INTEGER NOT NULL PRIMARY KEY,
    value TEXT UNIQUE NOT NULL
);
CREATE INDEX mime_type_value ON mime_type (value);

INSERT INTO mime_type (value) VALUES
    ("text/html"),
    ("application/json"),
    ("unk"),
    ("warc/revisit"),
    ("text/plain");

CREATE TABLE item (
    id INTEGER NOT NULL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    digest_id INTEGER NOT NULL,
    mime_type_id INTEGER NOT NULL,
    status INTEGER,
    FOREIGN KEY (url_id) REFERENCES url (id),
    FOREIGN KEY (digest_id) REFERENCES digest (id),
    FOREIGN KEY (mime_type_id) REFERENCES mime_type (id)
);
CREATE INDEX item_url_id ON item (url_id);
CREATE INDEX item_ts ON item (ts);
CREATE INDEX item_digest_id ON item (digest_id);
CREATE INDEX item_mime_type_id ON item (mime_type_id);
CREATE INDEX item_status ON item (status);
CREATE INDEX item_lookup ON item (url_id, ts, digest_id, mime_type_id, status);

CREATE TABLE size (
    item_id INTEGER NOT NULL,
    value INTEGER NOT NULL,
    FOREIGN KEY (item_id) REFERENCES item (id),
    UNIQUE (item_id, value)
);
CREATE INDEX size_item_id ON size (item_id);
