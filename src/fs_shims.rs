//! Runtime filesystem shims for [`FileSystemStorage`](crate::FileSystemStorage).
//!
//! Selects a concrete async filesystem implementation from the enabled runtime feature
//! (`smol`, `tokio`, or `async-std`) and exposes a small uniform surface: streaming
//! [`Reader`]/[`Writer`] handles plus whole-file and directory operations. With no runtime
//! feature enabled the surface still exists but every call panics, so the crate — and its
//! documentation — builds with `fs` alone.

use std::{
    io,
    path::{Path, PathBuf},
};

cfg_if::cfg_if! {
    if #[cfg(feature = "tokio")] {
        // tokio's `File` speaks `tokio::io`; wrap it so callers see `futures_lite` traits.
        pub(crate) type Reader = async_compat::Compat<tokio::fs::File>;
        pub(crate) type Writer = async_compat::Compat<tokio::fs::File>;

        pub(crate) async fn open(path: &Path) -> io::Result<Reader> {
            Ok(async_compat::Compat::new(tokio::fs::File::open(path).await?))
        }

        pub(crate) async fn create(path: &Path) -> io::Result<Writer> {
            Ok(async_compat::Compat::new(tokio::fs::File::create(path).await?))
        }

        pub(crate) async fn read(path: &Path) -> io::Result<Vec<u8>> {
            tokio::fs::read(path).await
        }

        pub(crate) async fn write(path: &Path, contents: &[u8]) -> io::Result<()> {
            tokio::fs::write(path, contents).await
        }

        pub(crate) async fn create_dir_all(path: &Path) -> io::Result<()> {
            tokio::fs::create_dir_all(path).await
        }

        pub(crate) async fn rename(from: &Path, to: &Path) -> io::Result<()> {
            tokio::fs::rename(from, to).await
        }

        pub(crate) async fn remove_dir_all(path: &Path) -> io::Result<()> {
            tokio::fs::remove_dir_all(path).await
        }

        pub(crate) async fn metadata_len(path: &Path) -> io::Result<u64> {
            Ok(tokio::fs::metadata(path).await?.len())
        }

        pub(crate) async fn read_dir_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
            let mut read_dir = tokio::fs::read_dir(path).await?;
            let mut paths = Vec::new();
            while let Some(entry) = read_dir.next_entry().await? {
                paths.push(entry.path());
            }
            Ok(paths)
        }
    } else if #[cfg(feature = "async-std")] {
        pub(crate) type Reader = async_std::fs::File;
        pub(crate) type Writer = async_std::fs::File;

        pub(crate) async fn open(path: &Path) -> io::Result<Reader> {
            async_std::fs::File::open(path).await
        }

        pub(crate) async fn create(path: &Path) -> io::Result<Writer> {
            async_std::fs::File::create(path).await
        }

        pub(crate) async fn read(path: &Path) -> io::Result<Vec<u8>> {
            async_std::fs::read(path).await
        }

        pub(crate) async fn write(path: &Path, contents: &[u8]) -> io::Result<()> {
            async_std::fs::write(path, contents).await
        }

        pub(crate) async fn create_dir_all(path: &Path) -> io::Result<()> {
            async_std::fs::create_dir_all(path).await
        }

        pub(crate) async fn rename(from: &Path, to: &Path) -> io::Result<()> {
            async_std::fs::rename(from, to).await
        }

        pub(crate) async fn remove_dir_all(path: &Path) -> io::Result<()> {
            async_std::fs::remove_dir_all(path).await
        }

        pub(crate) async fn metadata_len(path: &Path) -> io::Result<u64> {
            Ok(async_std::fs::metadata(path).await?.len())
        }

        pub(crate) async fn read_dir_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
            use futures_lite::StreamExt;
            let mut read_dir = async_std::fs::read_dir(path).await?;
            let mut paths = Vec::new();
            while let Some(entry) = read_dir.next().await {
                paths.push(entry?.path().into());
            }
            Ok(paths)
        }
    } else if #[cfg(feature = "smol")] {
        pub(crate) type Reader = async_fs::File;
        pub(crate) type Writer = async_fs::File;

        pub(crate) async fn open(path: &Path) -> io::Result<Reader> {
            async_fs::File::open(path).await
        }

        pub(crate) async fn create(path: &Path) -> io::Result<Writer> {
            async_fs::File::create(path).await
        }

        pub(crate) async fn read(path: &Path) -> io::Result<Vec<u8>> {
            async_fs::read(path).await
        }

        pub(crate) async fn write(path: &Path, contents: &[u8]) -> io::Result<()> {
            async_fs::write(path, contents).await
        }

        pub(crate) async fn create_dir_all(path: &Path) -> io::Result<()> {
            async_fs::create_dir_all(path).await
        }

        pub(crate) async fn rename(from: &Path, to: &Path) -> io::Result<()> {
            async_fs::rename(from, to).await
        }

        pub(crate) async fn remove_dir_all(path: &Path) -> io::Result<()> {
            async_fs::remove_dir_all(path).await
        }

        pub(crate) async fn metadata_len(path: &Path) -> io::Result<u64> {
            Ok(async_fs::metadata(path).await?.len())
        }

        pub(crate) async fn read_dir_paths(path: &Path) -> io::Result<Vec<PathBuf>> {
            use futures_lite::StreamExt;
            let mut read_dir = async_fs::read_dir(path).await?;
            let mut paths = Vec::new();
            while let Some(entry) = read_dir.next().await {
                paths.push(entry?.path());
            }
            Ok(paths)
        }
    } else {
        use std::{pin::Pin, task::{Context, Poll}};

        const NO_RUNTIME: &str =
            "enable the `smol`, `tokio`, or `async-std` feature to use FileSystemStorage";

        #[derive(Debug)]
        pub(crate) struct Reader;

        #[derive(Debug)]
        pub(crate) struct Writer;

        impl futures_lite::AsyncRead for Reader {
            fn poll_read(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &mut [u8],
            ) -> Poll<io::Result<usize>> {
                unimplemented!("{NO_RUNTIME}")
            }
        }

        impl futures_lite::AsyncWrite for Writer {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                _buf: &[u8],
            ) -> Poll<io::Result<usize>> {
                unimplemented!("{NO_RUNTIME}")
            }

            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                unimplemented!("{NO_RUNTIME}")
            }

            fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
                unimplemented!("{NO_RUNTIME}")
            }
        }

        pub(crate) async fn open(_path: &Path) -> io::Result<Reader> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn create(_path: &Path) -> io::Result<Writer> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn read(_path: &Path) -> io::Result<Vec<u8>> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn write(_path: &Path, _contents: &[u8]) -> io::Result<()> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn create_dir_all(_path: &Path) -> io::Result<()> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn rename(_from: &Path, _to: &Path) -> io::Result<()> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn remove_dir_all(_path: &Path) -> io::Result<()> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn metadata_len(_path: &Path) -> io::Result<u64> {
            unimplemented!("{NO_RUNTIME}")
        }

        pub(crate) async fn read_dir_paths(_path: &Path) -> io::Result<Vec<PathBuf>> {
            unimplemented!("{NO_RUNTIME}")
        }
    }
}
