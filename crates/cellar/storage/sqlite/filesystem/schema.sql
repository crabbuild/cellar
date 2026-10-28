-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

CREATE TABLE IF NOT EXISTS __sys_fs_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    format INTEGER NOT NULL CHECK(format=1),
    next_inode INTEGER NOT NULL CHECK(next_inode>1),
    revision INTEGER NOT NULL CHECK(revision>=0),
    logical_bytes INTEGER NOT NULL CHECK(logical_bytes>=0),
    allocated_bytes INTEGER NOT NULL CHECK(allocated_bytes>=0),
    inode_count INTEGER NOT NULL CHECK(inode_count>=0)
);
CREATE TABLE IF NOT EXISTS __sys_fs_inodes (
    id INTEGER PRIMARY KEY CHECK(id>0),
    kind INTEGER NOT NULL CHECK(kind IN (0,1)),
    parent INTEGER NOT NULL,
    mode INTEGER NOT NULL CHECK(mode BETWEEN 0 AND 511),
    uid INTEGER NOT NULL DEFAULT 1000 CHECK(uid BETWEEN 0 AND 4294967295),
    gid INTEGER NOT NULL DEFAULT 1000 CHECK(gid BETWEEN 0 AND 4294967295),
    size INTEGER NOT NULL CHECK(size>=0),
    nlink INTEGER NOT NULL CHECK(nlink>=0),
    mtime_ns INTEGER NOT NULL,
    ctime_ns INTEGER NOT NULL,
    version INTEGER NOT NULL CHECK(version>0),
    next_cookie INTEGER NOT NULL CHECK(next_cookie>0),
    allocated_bytes INTEGER NOT NULL DEFAULT 0 CHECK(allocated_bytes>=0)
);
CREATE INDEX IF NOT EXISTS __sys_fs_orphans ON __sys_fs_inodes(nlink,id);
CREATE INDEX IF NOT EXISTS __sys_fs_parents ON __sys_fs_inodes(parent,kind,nlink,id);
CREATE INDEX IF NOT EXISTS __sys_fs_live_directories ON __sys_fs_inodes(parent,id) WHERE kind=1 AND nlink>0;
CREATE TABLE IF NOT EXISTS __sys_fs_entries (
    parent INTEGER NOT NULL,
    name BLOB NOT NULL CHECK(length(name) BETWEEN 1 AND 255),
    inode INTEGER NOT NULL UNIQUE,
    cookie INTEGER NOT NULL CHECK(cookie>0),
    PRIMARY KEY(parent,name),
    UNIQUE(parent,cookie)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS __sys_fs_chunks (
    inode INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL CHECK(chunk_index>=0),
    data BLOB NOT NULL CHECK(length(data)=65536),
    revision INTEGER NOT NULL CHECK(revision>0),
    PRIMARY KEY(inode,chunk_index)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS __sys_fs_discarded_ranges (
    id INTEGER PRIMARY KEY,
    inode INTEGER NOT NULL,
    first_chunk INTEGER NOT NULL,
    last_chunk INTEGER NOT NULL,
    cutoff INTEGER NOT NULL,
    cursor INTEGER NOT NULL,
    CHECK(first_chunk>=0 AND last_chunk>=first_chunk AND cursor>=first_chunk)
);
CREATE INDEX IF NOT EXISTS __sys_fs_range_lookup ON __sys_fs_discarded_ranges(inode,first_chunk,last_chunk);
CREATE TRIGGER IF NOT EXISTS __sys_fs_inode_insert AFTER INSERT ON __sys_fs_inodes BEGIN
    UPDATE __sys_fs_meta SET inode_count=inode_count+1,logical_bytes=logical_bytes+NEW.size;
END;
-- Subtract first: total+new-old can overflow even for a shrink/overwrite.
CREATE TRIGGER IF NOT EXISTS __sys_fs_inode_size AFTER UPDATE OF size ON __sys_fs_inodes BEGIN
    UPDATE __sys_fs_meta SET logical_bytes=logical_bytes-OLD.size+NEW.size;
END;
CREATE TRIGGER IF NOT EXISTS __sys_fs_inode_delete AFTER DELETE ON __sys_fs_inodes BEGIN
    UPDATE __sys_fs_meta SET inode_count=inode_count-1,logical_bytes=logical_bytes-OLD.size;
END;
CREATE TRIGGER IF NOT EXISTS __sys_fs_chunk_insert AFTER INSERT ON __sys_fs_chunks BEGIN
    UPDATE __sys_fs_meta SET allocated_bytes=allocated_bytes+length(NEW.data);
    UPDATE __sys_fs_inodes SET allocated_bytes=allocated_bytes+length(NEW.data) WHERE id=NEW.inode;
END;
CREATE TRIGGER IF NOT EXISTS __sys_fs_chunk_delete AFTER DELETE ON __sys_fs_chunks BEGIN
    UPDATE __sys_fs_meta SET allocated_bytes=allocated_bytes-length(OLD.data);
    UPDATE __sys_fs_inodes SET allocated_bytes=allocated_bytes-length(OLD.data) WHERE id=OLD.inode;
END;
INSERT OR IGNORE INTO __sys_fs_meta VALUES(1,1,2,1,0,0,0);
INSERT OR IGNORE INTO __sys_fs_inodes(id,kind,parent,mode,size,nlink,mtime_ns,ctime_ns,version,next_cookie)
    VALUES(1,1,1,493,0,2,0,0,1,1);
