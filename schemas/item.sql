CREATE TABLE item (
    id INTEGER NOT NULL PRIMARY KEY,
    url_id INTEGER NOT NULL,
    timestamp_s INTEGER NOT NULL,
    digest_id INTEGER NOT NULL,
    mime_type_id INTEGER NOT NULL,
    length INTEGER,
    status INTEGER,
    FOREIGN KEY (url_id) REFERENCES url (id),
    FOREIGN KEY (digest_id) REFERENCES digest (id),
    FOREIGN KEY (mime_type_id) REFERENCES mime_type (id)
);
CREATE INDEX item_url_id ON item (url_id);
CREATE INDEX item_digest_id ON item (digest_id);
CREATE INDEX item_status ON item (status);
CREATE INDEX item_logical_key ON item (url_id, timestamp_s, digest_id);

CREATE TABLE url (
    id INTEGER NOT NULL PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE UNIQUE INDEX url_value ON url (value);

CREATE TABLE digest (
    id INTEGER NOT NULL PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE UNIQUE INDEX digest_value ON digest (value);

CREATE TABLE mime_type (
    id INTEGER NOT NULL PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE UNIQUE INDEX mime_type_value ON mime_type (value);

INSERT INTO mime_type (id, value) VALUES
    (1, "warc/revisit"),
    (2, "text/html"),
    (3, "application/json"),
    (4, "text/plain"),
    (5, "image/jpeg"),
    (6, "application/xml"),
    (7, "application/xhtml+xml"),
    (8, "unk"),
    (9, "unknown");
