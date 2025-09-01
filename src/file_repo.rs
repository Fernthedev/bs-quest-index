use std::{collections::HashMap, io::Result, path::PathBuf};

use bytes::Bytes;
use semver::Version;
use tokio::{fs, sync::RwLock};

pub struct FileRepo {
    path: PathBuf,
    // TODO: Synchronize
    cache: RwLock<HashMap<(String, Version), String>>,
}

impl FileRepo {
    pub fn new(path: PathBuf) -> FileRepo {
        FileRepo {
            path,
            cache: Default::default(),
        }
    }

    pub async fn remove_file(&self, id: String, ver: Version) -> Result<()> {
        self.cache.write().await.remove(&(id.clone(), ver.clone()));

        let file = self
            .path
            .join(&id)
            .join(format!("{}/{}/{}", &ver.major, &ver.minor, &ver.patch));
        if fs::metadata(&file).await.is_ok() {
            fs::remove_file(file).await?;
        }

        Ok(())
    }

    pub async fn get_file(&self, id: String, ver: Version) -> Result<String> {
        if let Some(o) = self.cache.read().await.get(&(id.clone(), ver.clone())) {
            return Ok(o.clone());
        }

        // lock to ensure no other thread is reading
        let mut cache = self.cache.write().await;

        let contents: String = fs::read_to_string(
            self.path
                .join(&id)
                .join(format!("{}/{}/{}", &ver.major, &ver.minor, &ver.patch)),
        )
        .await?;

        cache.insert((id.clone(), ver.clone()), contents.clone());
        Ok(contents)
    }

    pub async fn write_file(&self, id: String, ver: Version, contents: Bytes) -> Result<()> {
        self.cache.write().await.insert(
            (id.clone(), ver.clone()),
            String::from_utf8_lossy(&contents).into(),
        );

        let dir = self
            .path
            .join(id)
            .join(format!("{}/{}", ver.major, ver.minor));
        fs::create_dir_all(&dir).await?;

        let file = dir.join(ver.patch.to_string());
        fs::write(file, contents).await?;

        Ok(())
    }
}

// trait UnsafeCellExt<T>: Sized {
//     fn get_safe(&self) -> &T;

//     #[allow(clippy::mut_from_ref)]
//     fn get_mut_safe(&self) -> &mut T;
// }

// impl<T> UnsafeCellExt<T> for UnsafeCell<T> {
//     fn get_safe(&self) -> &T {
//         unsafe { &*self.get() }
//     }

//     fn get_mut_safe(&self) -> &mut T {
//         unsafe { &mut *self.get() }
//     }
// }
