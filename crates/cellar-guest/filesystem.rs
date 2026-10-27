// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-local descriptor registry and byte/integer preserving wire API.
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{
    filesystem::{self as fs, Directory, File, FsPath, OpenOptions},
    Context,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

static NEXT_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Filesystem metadata, capabilities and structured errors must fit before any
/// guest descriptor operation can be admitted. Other primitive-only sources
/// retain their existing smaller transport profiles.
pub fn validate_configuration(enabled: bool, maximum: usize) -> Result<()> {
    ensure!(
        !enabled || maximum >= 4096,
        "filesystem registrations require max_value_bytes of at least 4096"
    );
    Ok(())
}

struct WireBudget {
    transfer: usize,
    helper: usize,
    directory_entries: usize,
}

impl WireBudget {
    fn new(limits: fs::FilesystemLimits, options: &crate::Limits, scope_bytes: usize) -> Self {
        // Reserve the result envelope/metadata before base64 expansion. The
        // directory reserve additionally covers an encoded continuation scope;
        // each entry has at most 255 name bytes (340 base64 bytes), an inode
        // decimal, a kind and fixed JSON keys, all within 512 encoded bytes.
        let bytes = options.max_value_bytes.saturating_sub(1024) / 4 * 3;
        Self {
            transfer: bytes
                .min(options.max_body_bytes)
                .min(limits.max_transfer_bytes),
            helper: bytes
                .min(options.max_body_bytes)
                .min(limits.max_file_helper_bytes),
            directory_entries: options
                .max_value_bytes
                .saturating_sub(1024 + scope_bytes)
                .checked_div(512)
                .unwrap_or(0)
                .min(limits.max_directory_entries),
        }
    }

    fn directory(&self, requested: usize) -> Result<()> {
        if requested == 0 || requested > self.directory_entries {
            return Err(fs::Error::new(
                fs::Errno::EINVAL,
                "directory page exceeds the guest result budget",
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct Session {
    handles: Mutex<HashMap<String, Handle>>,
}

#[derive(Clone)]
enum Handle {
    File(Arc<File>),
    Directory(Arc<Directory>),
}

#[derive(Deserialize)]
#[serde(untagged)]
pub(crate) enum Path {
    Text(String),
    Bytes(BinaryPath),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BinaryPath {
    bytes_base64: String,
}

impl Path {
    pub(crate) fn decode(self) -> Result<FsPath> {
        let limit = cellar::asyncrt::services()
            .cell_limits()
            .filesystem
            .max_path_bytes;
        Ok(match self {
            Self::Text(text) => {
                if text.len() > limit {
                    return Err(path_limit().into());
                }
                text.into()
            }
            Self::Bytes(bytes) => {
                if bytes.bytes_base64.len() > limit.div_ceil(3).saturating_mul(4) {
                    return Err(path_limit().into());
                }
                let decoded = STANDARD
                    .decode(bytes.bytes_base64)
                    .context(crate::failure::Validation)?;
                if decoded.len() > limit {
                    return Err(path_limit().into());
                }
                decoded.into()
            }
        })
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Options {
    access: String,
    create: bool,
    create_new: bool,
    truncate: bool,
    append: bool,
    mode: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            access: "read".into(),
            create: false,
            create_new: false,
            truncate: false,
            append: false,
            mode: 0o666,
        }
    }
}

impl Options {
    fn decode(self) -> Result<OpenOptions> {
        let options = match self.access.as_str() {
            "read" => OpenOptions::read_only(),
            "write" => OpenOptions::write_only(),
            "read_write" => OpenOptions::read_write(),
            _ => anyhow::bail!("unsupported access mode"),
        };
        Ok(options
            .create(self.create)
            .create_new(self.create_new)
            .truncate(self.truncate)
            .append(self.append)
            .mode(self.mode))
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Capabilities,
    Statvfs,
    Sync,
    CollectGarbage,
    Open {
        path: Path,
        #[serde(default)]
        options: Options,
    },
    Creat {
        path: Path,
        mode: u32,
    },
    Openat {
        directory: String,
        path: Path,
        #[serde(default)]
        options: Options,
    },
    Read {
        handle: String,
        count: usize,
    },
    Pread {
        handle: String,
        count: usize,
        offset: String,
    },
    Write {
        handle: String,
        data_base64: String,
    },
    Pwrite {
        handle: String,
        data_base64: String,
        offset: String,
    },
    Lseek {
        handle: String,
        whence: String,
        offset: String,
    },
    Fstat {
        handle: String,
    },
    Ftruncate {
        handle: String,
        size: String,
    },
    Fchmod {
        handle: String,
        mode: u32,
    },
    Dup {
        handle: String,
    },
    Close {
        handle: String,
    },
    Fsync {
        handle: String,
    },
    Fdatasync {
        handle: String,
    },
    Stat {
        path: Path,
    },
    Lstat {
        path: Path,
    },
    Truncate {
        path: Path,
        size: String,
    },
    Mkdir {
        path: Path,
        mode: u32,
    },
    Rmdir {
        path: Path,
    },
    Unlink {
        path: Path,
    },
    Rename {
        old: Path,
        new: Path,
    },
    Chmod {
        path: Path,
        mode: u32,
    },
    Utimens {
        path: Path,
        mtime_ns: Option<String>,
    },
    Opendir {
        path: Path,
    },
    Readdir {
        handle: String,
    },
    Telldir {
        handle: String,
    },
    Seekdir {
        handle: String,
        cookie: String,
    },
    Rewinddir {
        handle: String,
    },
    Closedir {
        handle: String,
    },
    Fstatat {
        directory: String,
        path: Path,
        #[serde(default)]
        no_follow: bool,
    },
    Mkdirat {
        directory: String,
        path: Path,
        mode: u32,
    },
    Unlinkat {
        directory: String,
        path: Path,
        #[serde(default)]
        remove_directory: bool,
    },
    Renameat {
        old_directory: String,
        old: Path,
        new_directory: String,
        new: Path,
    },
    ReadFile {
        path: Path,
        max_bytes: Option<usize>,
    },
    WriteFile {
        path: Path,
        data_base64: String,
    },
    ReplaceFile {
        path: Path,
        data_base64: String,
    },
    EditFile {
        path: Path,
        inode: String,
        version: String,
        offset: String,
        delete_bytes: String,
        data_base64: String,
    },
    ReadDir {
        path: Path,
        limit: usize,
        cursor: Option<fs::ReadDirCursor>,
    },
    ReadPage {
        handle: String,
        limit: usize,
    },
}

fn integer(value: &str) -> Result<u64> {
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid unsigned filesystem integer"
    );
    Ok(value.parse()?)
}

fn signed(value: &str) -> Result<i64> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    ensure!(
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()),
        "invalid signed filesystem integer"
    );
    Ok(value.parse()?)
}

pub(crate) fn stat(value: fs::Stat) -> Value {
    json!({"inode":value.inode.to_string(),"kind":value.kind,"mode":value.mode,"uid":value.uid,"gid":value.gid,
        "size":value.size.to_string(),"nlink":value.nlink.to_string(),"atime_ns":value.atime_ns.to_string(),
        "mtime_ns":value.mtime_ns.to_string(),"ctime_ns":value.ctime_ns.to_string(),"version":value.version.to_string(),"allocated_bytes":value.allocated_bytes.to_string()})
}

fn entry(value: fs::DirEntry) -> Value {
    json!({"name_base64":STANDARD.encode(value.name),"inode":value.inode.to_string(),"kind":value.kind})
}

impl Session {
    fn insert(&self, handle: Handle) -> Result<Value> {
        let token = NEXT_TOKEN
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| anyhow::anyhow!("filesystem guest token identity exhausted"))?
            .to_string();
        self.handles.lock().unwrap().insert(token.clone(), handle);
        Ok(json!({"handle":token}))
    }

    fn get(&self, token: &str) -> Result<Handle> {
        self.handles
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .ok_or_else(|| bad_descriptor().into())
    }

    fn file(&self, token: &str) -> Result<Arc<File>> {
        match self.get(token)? {
            Handle::File(file) => Ok(file),
            _ => Err(bad_descriptor().into()),
        }
    }

    fn dir(&self, token: &str) -> Result<Arc<Directory>> {
        match self.get(token)? {
            Handle::Directory(dir) => Ok(dir),
            _ => Err(bad_descriptor().into()),
        }
    }

    pub async fn execute(
        &self,
        context: &Context,
        name: &str,
        mut value: Value,
        options: &crate::Limits,
    ) -> Result<Value> {
        let request = (|| {
            ensure!(
                value.is_object() && value.get("op").is_none(),
                "invalid filesystem operation payload"
            );
            value["op"] = json!(name
                .strip_prefix("filesystem.")
                .context("invalid filesystem operation")?);
            Ok(serde_json::from_value::<Request>(value)?)
        })()
        .context(crate::failure::Validation)?;
        let filesystem = context.fs()?;
        let limits = filesystem.capabilities()?.limits;
        let scope_bytes =
            serde_json::to_vec(&context.identity().map(|identity| &identity.scope))?.len();
        let budget = WireBudget::new(limits, options, scope_bytes);
        let data_limit = budget.transfer;
        let decode = |value: String, limit: usize| -> Result<Vec<u8>> {
            ensure!(
                value.len() <= limit.div_ceil(3).saturating_mul(4),
                "filesystem byte payload exceeds limit"
            );
            let bytes = STANDARD.decode(value)?;
            ensure!(
                bytes.len() <= limit,
                "filesystem byte payload exceeds limit"
            );
            Ok(bytes)
        };
        use Request::*;
        match request {
            Capabilities=>{let mut value=serde_json::to_value(filesystem.capabilities()?)?;
                for (key,integer) in [("max_file_bytes",limits.max_file_bytes),("max_logical_bytes",limits.max_logical_bytes),("max_allocated_bytes",limits.max_allocated_bytes),("max_inodes",limits.max_inodes)] {value["limits"][key]=json!(integer.to_string());}
                value["limits"]["max_transfer_bytes"]=json!(budget.transfer);
                value["limits"]["max_file_helper_bytes"]=json!(budget.helper);
                value["limits"]["max_directory_entries"]=json!(budget.directory_entries);
                value["transport"]=json!({"max_value_bytes":options.max_value_bytes,"max_body_bytes":options.max_body_bytes,"max_batch_operations":options.max_batch_operations});
                Ok(value)},
            Statvfs=>{let stats=filesystem.statvfs()?; Ok(json!({"logical_bytes":stats.logical_bytes.to_string(),"allocated_bytes":stats.allocated_bytes.to_string(),"inodes":stats.inodes.to_string(),"available_logical_bytes":stats.available_logical_bytes.to_string(),"available_allocated_bytes":stats.available_allocated_bytes.to_string(),"available_inodes":stats.available_inodes.to_string(),"chunk_bytes":stats.chunk_bytes}))},
            Sync=>{filesystem.sync().await?; Ok(Value::Null)},
            CollectGarbage=>Ok(json!(filesystem.collect_garbage()?)),
            Open {path,options}=>self.insert(Handle::File(Arc::new(filesystem.open(path.decode()?,options.decode()?)?))),
            Creat {path,mode}=>self.insert(Handle::File(Arc::new(filesystem.creat(path.decode()?,mode)?))),
            Openat {directory,path,options}=>self.insert(Handle::File(Arc::new(filesystem.openat(self.dir(&directory)?.as_ref(),path.decode()?,options.decode()?)?))),
            Read {handle,count}=>{let mut bytes=vec![0;count.min(data_limit)]; let read=self.file(&handle)?.read(&mut bytes)?; bytes.truncate(read); Ok(json!({"data_base64":STANDARD.encode(bytes),"count":read}))},
            Pread {handle,count,offset}=>{let offset=integer(&offset).context(crate::failure::Validation)?;let mut bytes=vec![0;count.min(data_limit)];let read=self.file(&handle)?.pread(&mut bytes,offset)?;bytes.truncate(read);Ok(json!({"data_base64":STANDARD.encode(bytes),"count":read}))},
            Write {handle,data_base64}=>{let bytes=decode(data_base64,options.max_body_bytes).context(crate::failure::Validation)?;Ok(json!(self.file(&handle)?.write(&bytes)?))},
            Pwrite {handle,data_base64,offset}=>{let offset=integer(&offset).context(crate::failure::Validation)?;let bytes=decode(data_base64,options.max_body_bytes).context(crate::failure::Validation)?;Ok(json!(self.file(&handle)?.pwrite(&bytes,offset)?))},
            Lseek {handle,whence,offset}=>{let seek=(|| Ok(match whence.as_str() {"start"=>fs::Seek::Start(integer(&offset)?),"current"=>fs::Seek::Current(signed(&offset)?),"end"=>fs::Seek::End(signed(&offset)?),_=>anyhow::bail!("unsupported seek origin")}))().context(crate::failure::Validation)?;Ok(json!(self.file(&handle)?.lseek(seek)?.to_string()))},
            Fstat {handle}=>Ok(stat(match self.get(&handle)? {Handle::File(file)=>file.fstat()?,Handle::Directory(dir)=>dir.fstat()?})),
            Ftruncate {handle,size}=>{self.file(&handle)?.ftruncate(integer(&size).context(crate::failure::Validation)?)?;Ok(Value::Null)},
            Fchmod {handle,mode}=>{self.file(&handle)?.fchmod(mode)?;Ok(Value::Null)},
            Dup {handle}=>self.insert(Handle::File(Arc::new(self.file(&handle)?.dup()?))),
            Close {handle}|Closedir {handle}=>{self.handles.lock().unwrap().remove(&handle).ok_or_else(bad_descriptor)?;Ok(Value::Null)},
            Fsync {handle}|Fdatasync {handle}=>{match self.get(&handle)? {Handle::File(file)=>file.fsync().await?,Handle::Directory(dir)=>dir.fsync().await?};Ok(Value::Null)},
            Stat {path}|Lstat {path}=>Ok(stat(filesystem.stat(path.decode()?)?)),
            Truncate {path,size}=>{let size=integer(&size).context(crate::failure::Validation)?;filesystem.truncate(path.decode()?,size)?;Ok(Value::Null)},
            Mkdir {path,mode}=>{filesystem.mkdir(path.decode()?,mode)?;Ok(Value::Null)},
            Rmdir {path}=>{filesystem.rmdir(path.decode()?)?;Ok(Value::Null)},
            Unlink {path}=>{filesystem.unlink(path.decode()?)?;Ok(Value::Null)},
            Rename {old,new}=>{filesystem.rename(old.decode()?,new.decode()?)?;Ok(Value::Null)},
            Chmod {path,mode}=>{filesystem.chmod(path.decode()?,mode)?;Ok(Value::Null)},
            Utimens {path,mtime_ns}=>{let mtime=match mtime_ns {None=>fs::Time::Omit,Some(value) if value=="now"=>fs::Time::Now,Some(value)=>fs::Time::Value(signed(&value).context(crate::failure::Validation)?)};filesystem.utimens(path.decode()?,fs::Times {atime:fs::Time::Omit,mtime})?;Ok(Value::Null)},
            Opendir {path}=>self.insert(Handle::Directory(Arc::new(filesystem.opendir(path.decode()?)?))),
            Readdir {handle}=>Ok(self.dir(&handle)?.readdir()?.map_or(Value::Null,|entry| json!({"name_base64":STANDARD.encode(entry.name),"inode":entry.inode.to_string(),"kind":entry.kind}))),
            Telldir {handle}=>Ok(json!(STANDARD.encode(self.dir(&handle)?.telldir()?.to_bytes()))),
            Seekdir {handle,cookie}=>{let bytes=decode(cookie,16).context(crate::failure::Validation)?;let bytes=bytes.try_into().map_err(|_| anyhow::anyhow!("invalid directory cookie")).context(crate::failure::Validation)?;self.dir(&handle)?.seekdir(fs::DirCookie::from_bytes(bytes))?;Ok(Value::Null)},
            Rewinddir {handle}=>{self.dir(&handle)?.rewinddir()?;Ok(Value::Null)},
            Fstatat {directory,path,no_follow}=>Ok(stat(filesystem.fstatat(self.dir(&directory)?.as_ref(),path.decode()?,if no_follow {fs::StatFlags::NoFollow} else {fs::StatFlags::Follow})?)),
            Mkdirat {directory,path,mode}=>{filesystem.mkdirat(self.dir(&directory)?.as_ref(),path.decode()?,mode)?;Ok(Value::Null)},
            Unlinkat {directory,path,remove_directory}=>{filesystem.unlinkat(self.dir(&directory)?.as_ref(),path.decode()?,if remove_directory {fs::UnlinkFlags::RemoveDirectory} else {fs::UnlinkFlags::File})?;Ok(Value::Null)},
            Renameat {old_directory,old,new_directory,new}=>{filesystem.renameat(self.dir(&old_directory)?.as_ref(),old.decode()?,self.dir(&new_directory)?.as_ref(),new.decode()?)?;Ok(Value::Null)},
            ReadFile {path,max_bytes}=>Ok(json!({"data_base64":STANDARD.encode(filesystem.read_file_bounded(path.decode()?,max_bytes.unwrap_or(budget.helper).min(budget.helper))?)})),
            WriteFile {path,data_base64}=>{let bytes=decode(data_base64,budget.helper).context(crate::failure::Validation)?;filesystem.write_file(path.decode()?,&bytes)?;Ok(Value::Null)},
            ReplaceFile {path,data_base64}=>{let bytes=decode(data_base64,budget.helper).context(crate::failure::Validation)?;filesystem.replace_file(path.decode()?,&bytes)?;Ok(Value::Null)},
            EditFile {path,inode,version,offset,delete_bytes,data_base64}=>{
                let expected=fs::FileVersion {inode:integer(&inode).context(crate::failure::Validation)?,version:integer(&version).context(crate::failure::Validation)?};
                let patch=fs::Edit {offset:integer(&offset).context(crate::failure::Validation)?,delete_bytes:integer(&delete_bytes).context(crate::failure::Validation)?,insert:decode(data_base64,budget.helper).context(crate::failure::Validation)?};
                let version=filesystem.edit_file(path.decode()?,expected,&patch)?;
                Ok(json!({"inode":version.inode.to_string(),"version":version.version.to_string()}))
            },
            ReadDir {path,limit,cursor}=>{budget.directory(limit)?;let page=filesystem.read_dir(path.decode()?,limit,cursor.as_ref())?;Ok(json!({"entries":page.entries.into_iter().map(entry).collect::<Vec<_>>(),"next":page.next}))},
            ReadPage {handle,limit}=>{budget.directory(limit)?;Ok(Value::Array(self.dir(&handle)?.read_page(limit)?.into_iter().map(entry).collect()))},
        }
    }
}

fn path_limit() -> fs::Error {
    fs::Error::new(
        fs::Errno::ENAMETOOLONG,
        "filesystem path byte limit exceeded",
    )
}

fn bad_descriptor() -> fs::Error {
    // Use the public native descriptor contract to classify guest tokens without
    // passing a guessed token into another invocation's registry.
    fs::Error::new(
        fs::Errno::EBADF,
        "filesystem descriptor is not present in this invocation",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_bounds_fit_binary_reads_and_worst_case_directory_results() -> Result<()> {
        for maximum in [4096, 8192, 2 * 1024 * 1024] {
            let options = crate::Limits {
                max_value_bytes: maximum,
                max_body_bytes: maximum,
                max_batch_operations: 64,
            };
            let scope = "Class:".to_owned() + &"\u{0001}".repeat(80);
            let encoded_scope = serde_json::to_vec(&scope)?.len();
            let budget = WireBudget::new(fs::FilesystemLimits::default(), &options, encoded_scope);
            let bytes = vec![255; budget.transfer];
            let read = crate::failure::result(
                "filesystem.read",
                Ok(json!({"data_base64":STANDARD.encode(&bytes),"count":bytes.len()})),
                maximum,
            )?;
            assert_eq!(serde_json::from_str::<Value>(&read)?["ok"], true);
            let entry = json!({"name_base64":STANDARD.encode([255;255]),"inode":u64::MAX.to_string(),"kind":"Directory"});
            let page = json!({"entries":vec![entry;budget.directory_entries],"next":{"scope":scope,"inode":u64::MAX.to_string(),"position":u64::MAX.to_string()}});
            let encoded = crate::failure::result("filesystem.read_dir", Ok(page), maximum)?;
            assert_eq!(serde_json::from_str::<Value>(&encoded)?["ok"], true);
            assert!(budget.directory(budget.directory_entries + 1).is_err());
        }
        assert!(validate_configuration(true, 4095).is_err());
        validate_configuration(true, 4096)?;
        validate_configuration(false, 128)?;
        Ok(())
    }
}
