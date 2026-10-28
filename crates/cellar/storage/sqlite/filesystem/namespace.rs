// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{now_ns, Database};
use crate::application::filesystem::{
    types::Path, DirEntry, Errno, Error, Kind, OpenOptions, Stat,
};
use anyhow::Result;
use rusqlite::{params, OptionalExtension};

impl Database<'_> {
    pub fn require_permission(&self, inode: u64, bits: u32) -> Result<()> {
        permission(&self.stat(inode)?, bits)
    }

    pub fn resolve(&self, start: u64, path: &Path) -> Result<u64> {
        let inode = self.walk(if path.absolute { 1 } else { start }, &path.components)?;
        if path.trailing_slash && self.stat(inode)?.kind != Kind::Directory {
            return Err(Error::new(Errno::ENOTDIR, "trailing slash requires a directory").into());
        }
        Ok(inode)
    }

    fn walk(&self, mut inode: u64, components: &[Vec<u8>]) -> Result<u64> {
        for name in components {
            let directory = self.stat(inode)?;
            require_directory(&directory)?;
            permission(&directory, 0o100)?;
            match name.as_slice() {
                b"." => {}
                b".." => inode = self.parent(inode)?,
                _ => {
                    inode = self
                        .lookup(inode, name)?
                        .ok_or_else(|| Error::new(Errno::ENOENT, "path component does not exist"))?
                }
            }
        }
        Ok(inode)
    }

    pub fn parent(&self, inode: u64) -> Result<u64> {
        self.connection
            .query_row(
                "SELECT parent FROM __sys_fs_inodes WHERE id=?1",
                [inode],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| Error::new(Errno::ENOENT, "directory does not exist").into())
    }

    fn parent_path<'a>(&self, start: u64, path: &'a Path) -> Result<(u64, &'a [u8])> {
        let (name, ancestors) = path
            .components
            .split_last()
            .ok_or_else(|| Error::new(Errno::EBUSY, "cannot mutate the filesystem root"))?;
        if matches!(name.as_slice(), b"." | b"..") {
            return Err(Error::new(Errno::EINVAL, "reserved final path component").into());
        }
        let parent = self.walk(if path.absolute { 1 } else { start }, ancestors)?;
        let stat = self.stat(parent)?;
        require_directory(&stat)?;
        permission(&stat, 0o300)?;
        if stat.nlink == 0 {
            return Err(Error::new(Errno::ENOENT, "parent directory has been removed").into());
        }
        Ok((parent, name))
    }

    fn lookup(&self, parent: u64, name: &[u8]) -> Result<Option<u64>> {
        Ok(self
            .connection
            .query_row(
                "SELECT inode FROM __sys_fs_entries WHERE parent=?1 AND name=?2",
                params![parent, name],
                |row| row.get(0),
            )
            .optional()?)
    }

    fn namespace_changed(&self, inode: u64, directory_links: i64) -> Result<()> {
        self.connection.execute(
            "UPDATE __sys_fs_inodes SET mtime_ns=?2,ctime_ns=?2,version=(SELECT revision FROM __sys_fs_meta WHERE singleton=1),nlink=nlink+?3 WHERE id=?1",
            params![inode,now_ns()?,directory_links])?;
        Ok(())
    }

    fn cookie(&self, parent: u64) -> Result<u64> {
        let cookie: u64 = self.connection.query_row(
            "SELECT next_cookie FROM __sys_fs_inodes WHERE id=?1",
            [parent],
            |row| row.get(0),
        )?;
        if cookie >= i64::MAX as u64 {
            return Err(Error::new(Errno::EOVERFLOW, "directory cookie exhausted").into());
        }
        self.connection.execute(
            "UPDATE __sys_fs_inodes SET next_cookie=next_cookie+1 WHERE id=?1",
            [parent],
        )?;
        Ok(cookie)
    }

    fn create_inode(&self, parent: u64, name: &[u8], kind: Kind, mode: u32) -> Result<u64> {
        if mode & !0o777 != 0 {
            return Err(Error::new(Errno::EOPNOTSUPP, "unsupported mode bits").into());
        }
        self.check_ancestry(parent, None, 1)?;
        let inode: u64 = self.connection.query_row(
            "SELECT next_inode FROM __sys_fs_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if inode >= i64::MAX as u64 {
            return Err(Error::new(Errno::EOVERFLOW, "inode identity exhausted").into());
        }
        self.check_growth(0, 0, 1)?;
        let revision = self.revision()?;
        let time = now_ns()?;
        self.connection.execute(
            "UPDATE __sys_fs_meta SET next_inode=next_inode+1 WHERE singleton=1",
            [],
        )?;
        self.connection.execute("INSERT INTO __sys_fs_inodes(id,kind,parent,mode,size,nlink,mtime_ns,ctime_ns,version,next_cookie) VALUES(?1,?2,?3,?4,0,?5,?6,?6,?7,1)",
            params![inode,if kind==Kind::Directory {1} else {0},parent,mode,if kind==Kind::Directory {2} else {1},time,revision])?;
        let cookie = self.cookie(parent)?;
        self.connection.execute(
            "INSERT INTO __sys_fs_entries VALUES(?1,?2,?3,?4)",
            params![parent, name, inode, cookie],
        )?;
        self.namespace_changed(parent, i64::from(kind == Kind::Directory))?;
        Ok(inode)
    }

    pub fn open(&self, start: u64, path: &Path, options: OpenOptions, mask: u32) -> Result<u64> {
        self.open_with_policy(start, path, options, mask, false)
    }

    pub fn open_read_only(&self, start: u64, path: &Path, options: OpenOptions) -> Result<u64> {
        self.open_with_policy(start, path, options, 0, true)
    }

    fn open_with_policy(
        &self,
        start: u64,
        path: &Path,
        options: OpenOptions,
        mask: u32,
        read_only: bool,
    ) -> Result<u64> {
        options.validate()?;
        if read_only && options.writable {
            return Err(Error::new(Errno::EROFS, "filesystem class policy is read-only").into());
        }
        let existing = self.resolve(start, path);
        let mut created = false;
        let inode = match existing {
            Ok(inode) => {
                if options.exclusive {
                    return Err(Error::new(
                        Errno::EEXIST,
                        "exclusive create found an existing entry",
                    )
                    .into());
                }
                inode
            }
            Err(error)
                if options.create
                    && error
                        .downcast_ref::<Error>()
                        .is_some_and(|error| error.errno == Errno::ENOENT) =>
            {
                if path.trailing_slash {
                    return Err(Error::new(
                        Errno::ENOENT,
                        "cannot create a regular file with a trailing slash",
                    )
                    .into());
                }
                let (parent, name) = self.parent_path(start, path)?;
                if read_only {
                    return Err(
                        Error::new(Errno::EROFS, "filesystem class policy is read-only").into(),
                    );
                }
                created = true;
                self.create_inode(parent, name, Kind::File, options.mode & !mask)?
            }
            Err(error) => return Err(error),
        };
        let stat = self.stat(inode)?;
        if stat.kind != Kind::File {
            return Err(Error::new(Errno::EISDIR, "regular file operation on a directory").into());
        }
        if !created {
            permission(
                &stat,
                (if options.readable { 0o400 } else { 0 })
                    | (if options.writable { 0o200 } else { 0 }),
            )?;
        }
        if options.truncate {
            self.truncate(inode, 0)?;
        }
        Ok(inode)
    }

    pub fn mkdir(&self, start: u64, path: &Path, mode: u32, mask: u32) -> Result<()> {
        if matches!(
            path.components.last().map(Vec::as_slice),
            None | Some(b".") | Some(b"..")
        ) {
            // Root and dot names already identify a directory. Resolve the
            // prefix first so missing/non-directory ancestors keep their error.
            self.resolve(start, path)?;
            return Err(Error::new(Errno::EEXIST, "directory entry already exists").into());
        }
        let (parent, name) = self.parent_path(start, path)?;
        if self.lookup(parent, name)?.is_some() {
            return Err(Error::new(Errno::EEXIST, "directory entry already exists").into());
        }
        self.create_inode(parent, name, Kind::Directory, mode & !mask)?;
        Ok(())
    }

    pub fn replace(&self, path: &Path, mode: u32, mask: u32) -> Result<u64> {
        if path.trailing_slash {
            return Err(Error::new(
                Errno::ENOTDIR,
                "regular-file replacement cannot have a trailing slash",
            )
            .into());
        }
        let (parent, name) = self.parent_path(1, path)?;
        if let Some(old) = self.lookup(parent, name)? {
            if self.stat(old)?.kind != Kind::File {
                return Err(Error::new(
                    Errno::EISDIR,
                    "cannot replace a directory with a regular file",
                )
                .into());
            }
            self.connection.execute(
                "DELETE FROM __sys_fs_entries WHERE parent=?1 AND name=?2",
                params![parent, name],
            )?;
            self.connection.execute(
                "UPDATE __sys_fs_inodes SET nlink=0,ctime_ns=?2 WHERE id=?1",
                params![old, now_ns()?],
            )?;
        }
        self.create_inode(parent, name, Kind::File, mode & !mask)
    }

    pub fn read_dir_page(
        &self,
        inode: u64,
        position: u64,
        limit: usize,
    ) -> Result<Vec<(u64, DirEntry)>> {
        require_directory(&self.stat(inode)?)?;
        permission(&self.stat(inode)?, 0o400)?;
        Ok(self.connection.prepare("SELECT e.cookie,e.name,e.inode,i.kind FROM __sys_fs_entries e JOIN __sys_fs_inodes i ON i.id=e.inode WHERE e.parent=?1 AND e.cookie>?2 ORDER BY e.cookie LIMIT ?3")?
            .query_map(params![inode,position,limit],|row|Ok((row.get(0)?,DirEntry {name:row.get(1)?,inode:row.get(2)?,kind:if row.get::<_,i64>(3)?==1 {Kind::Directory} else {Kind::File}})))?.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn empty_directory(&self, inode: u64) -> Result<()> {
        let populated: bool = self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_fs_entries WHERE parent=?1)",
            [inode],
            |row| row.get(0),
        )?;
        if populated {
            return Err(Error::new(Errno::ENOTEMPTY, "directory is not empty").into());
        }
        Ok(())
    }

    pub fn unlink(&self, start: u64, path: &Path, directory: bool) -> Result<()> {
        if matches!(
            path.components.last().map(Vec::as_slice),
            None | Some(b".") | Some(b"..")
        ) {
            let inode = self.resolve(start, path)?;
            if inode == 1 {
                return Err(Error::new(Errno::EBUSY, "cannot remove the filesystem root").into());
            }
            let errno = if !directory {
                Errno::EPERM
            } else if path.components.last().is_some_and(|name| name == b".") {
                Errno::EINVAL
            } else {
                Errno::ENOTEMPTY
            };
            return Err(Error::new(errno, "cannot remove a reserved directory name").into());
        }
        let (parent, name) = self.parent_path(start, path)?;
        let inode = self.resolve(start, path)?;
        let stat = self.stat(inode)?;
        if directory {
            require_directory(&stat)?;
            self.empty_directory(inode)?;
        } else if stat.kind == Kind::Directory {
            return Err(Error::new(Errno::EPERM, "unlink cannot remove a directory").into());
        }
        self.revision()?;
        self.connection.execute(
            "DELETE FROM __sys_fs_entries WHERE parent=?1 AND name=?2",
            params![parent, name],
        )?;
        self.connection.execute(
            "UPDATE __sys_fs_inodes SET nlink=0,ctime_ns=?2 WHERE id=?1",
            params![inode, now_ns()?],
        )?;
        self.namespace_changed(parent, -i64::from(directory))?;
        Ok(())
    }

    // Bound the persisted tree as well as the number of components in a single
    // *at request. Repeated relative mkdir must not create unbounded traversal.
    fn check_ancestry(
        &self,
        mut parent: u64,
        forbidden: Option<u64>,
        extra_depth: usize,
    ) -> Result<()> {
        for depth in 0..=self.limits.max_path_depth {
            if Some(parent) == forbidden {
                return Err(
                    Error::new(Errno::EINVAL, "directory rename would create a cycle").into(),
                );
            }
            if parent == 1 {
                if depth.saturating_add(extra_depth) > self.limits.max_path_depth {
                    break;
                }
                return Ok(());
            }
            parent = self.parent(parent)?;
        }
        Err(Error::new(Errno::ENAMETOOLONG, "filesystem tree depth exceeded").into())
    }

    fn subtree_height(&self, inode: u64) -> Result<usize> {
        let mut queue = std::collections::VecDeque::from([(inode, 0usize)]);
        let mut visited = 1usize;
        let mut height = 0;
        while let Some((inode, depth)) = queue.pop_front() {
            let populated: bool = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM __sys_fs_entries WHERE parent=?1)",
                [inode],
                |row| row.get(0),
            )?;
            height = height.max(depth + usize::from(populated));
            let remaining = self
                .limits
                .max_namespace_directories
                .saturating_sub(visited);
            let children=self.connection.prepare("SELECT id FROM __sys_fs_inodes INDEXED BY __sys_fs_live_directories WHERE parent=?1 AND kind=1 AND nlink>0 ORDER BY id LIMIT ?2")?
                .query_map(params![inode,remaining+1],|row|row.get::<_,u64>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            if children.len() > remaining {
                return Err(Error::new(
                    Errno::EDQUOT,
                    "directory rename exceeds bounded namespace work",
                )
                .into());
            }
            visited += children.len();
            queue.extend(children.into_iter().map(|child| (child, depth + 1)));
        }
        Ok(height)
    }

    pub fn rename(&self, old_start: u64, old: &Path, new_start: u64, new: &Path) -> Result<()> {
        let (old_parent, old_name) = self.parent_path(old_start, old)?;
        let inode = self.resolve(old_start, old)?;
        let source = self.stat(inode)?;
        let (new_parent, new_name) = self.parent_path(new_start, new)?;
        let target = self.lookup(new_parent, new_name)?;
        if new.trailing_slash && source.kind != Kind::Directory {
            return Err(Error::new(Errno::ENOTDIR, "trailing slash requires a directory").into());
        }
        if target == Some(inode) {
            return Ok(());
        }
        if source.kind == Kind::Directory {
            self.check_ancestry(new_parent, Some(inode), 1 + self.subtree_height(inode)?)?;
        }
        if let Some(target) = target {
            let destination = self.stat(target)?;
            if source.kind != destination.kind {
                return Err(Error::new(
                    if source.kind == Kind::Directory {
                        Errno::ENOTDIR
                    } else {
                        Errno::EISDIR
                    },
                    "rename types differ",
                )
                .into());
            }
            if destination.kind == Kind::Directory {
                self.empty_directory(target)?;
            }
            self.connection.execute(
                "DELETE FROM __sys_fs_entries WHERE parent=?1 AND name=?2",
                params![new_parent, new_name],
            )?;
            self.connection.execute(
                "UPDATE __sys_fs_inodes SET nlink=0,ctime_ns=?2 WHERE id=?1",
                params![target, now_ns()?],
            )?;
            self.namespace_changed(new_parent, -i64::from(destination.kind == Kind::Directory))?;
        }
        self.revision()?;
        let cookie = self.cookie(new_parent)?;
        self.connection.execute(
            "DELETE FROM __sys_fs_entries WHERE parent=?1 AND name=?2",
            params![old_parent, old_name],
        )?;
        self.connection.execute(
            "INSERT INTO __sys_fs_entries VALUES(?1,?2,?3,?4)",
            params![new_parent, new_name, inode, cookie],
        )?;
        self.connection.execute(
            "UPDATE __sys_fs_inodes SET parent=?2,ctime_ns=?3 WHERE id=?1",
            params![inode, new_parent, now_ns()?],
        )?;
        let links = i64::from(source.kind == Kind::Directory);
        self.namespace_changed(old_parent, -links)?;
        self.namespace_changed(new_parent, links)?;
        Ok(())
    }

    pub fn readdir(&self, inode: u64, after: u64) -> Result<Option<(u64, DirEntry)>> {
        require_directory(&self.stat(inode)?)?;
        Ok(self.connection.query_row("SELECT e.cookie,e.name,e.inode,i.kind FROM __sys_fs_entries e JOIN __sys_fs_inodes i ON i.id=e.inode WHERE e.parent=?1 AND e.cookie>?2 ORDER BY e.cookie LIMIT 1",
            params![inode,after], |row| Ok((row.get(0)?,DirEntry { name: row.get(1)?,inode: row.get(2)?,kind: if row.get::<_,i64>(3)?==1 {Kind::Directory} else {Kind::File} }))).optional()?)
    }
}

pub(super) fn require_directory(stat: &Stat) -> Result<()> {
    if stat.kind != Kind::Directory {
        return Err(Error::new(Errno::ENOTDIR, "path component is not a directory").into());
    }
    Ok(())
}

pub(crate) fn permission(stat: &Stat, bits: u32) -> Result<()> {
    if stat.mode & bits != bits {
        return Err(Error::new(Errno::EACCES, "filesystem mode denies this operation").into());
    }
    Ok(())
}
