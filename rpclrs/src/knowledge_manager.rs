use redis::{Client, Commands, Connection};
use serde::{Serialize, Deserialize};
use anyhow::Result;

pub struct KnowledgeManager {
    client: Client,
    connection: Option<Connection>,
    db: i32,
}

impl KnowledgeManager {
    pub fn new(host: &str, port: u16, db: i32) -> Result<Self> {
        let client = Client::open(format!("redis://{}:{}/{}", host, port, db))?;
        let mut connection = client.get_connection()?;
        
        // Select database
        let _: () = redis::cmd("SELECT").arg(db).query(&mut connection)?;
        
        Ok(KnowledgeManager {
            client,
            connection: Some(connection),
            db,
        })
    }

    pub fn write(&mut self, key: &str, value: &str) -> Result<()> {
        if let Some(ref mut conn) = self.connection {
            let _: () = conn.set(key, value)?;
            log::debug!("Wrote knowledge: {}", key);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Not connected"))
        }
    }

    pub fn read(&mut self, key: &str) -> Result<Option<String>> {
        if let Some(ref mut conn) = self.connection {
            let value: Option<String> = conn.get(key)?;
            if value.is_some() {
                log::debug!("Read knowledge: {}", key);
            } else {
                log::debug!("Knowledge not found: {}", key);
            }
            Ok(value)
        } else {
            Err(anyhow::anyhow!("Not connected"))
        }
    }

    pub fn exists(&mut self, key: &str) -> Result<bool> {
        if let Some(ref mut conn) = self.connection {
            let exists: bool = conn.exists(key)?;
            Ok(exists)
        } else {
            Err(anyhow::anyhow!("Not connected"))
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<bool> {
        if let Some(ref mut conn) = self.connection {
            let deleted: i32 = conn.del(key)?;
            Ok(deleted > 0)
        } else {
            Err(anyhow::anyhow!("Not connected"))
        }
    }

    pub fn write_object<T>(&mut self, key: &str, obj: &T) -> Result<()>
    where
        T: Serialize,
    {
        let json_str = serde_json::to_string(obj)?;
        self.write(key, &json_str)
    }

    pub fn read_object<T>(&mut self, key: &str) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        if let Some(json_str) = self.read(key)? {
            let obj: T = serde_json::from_str(&json_str)?;
            Ok(Some(obj))
        } else {
            Ok(None)
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }
}
