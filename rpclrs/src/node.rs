use crate::communication_manager::CommunicationManager;
use crate::knowledge_manager::KnowledgeManager;
use serde::{Serialize, Deserialize};
use serde_json::{json, Value};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use anyhow::Result;
use std::sync::{Arc, Mutex};

pub type EventCallback = Box<dyn Fn(&str, &str) + Send + 'static>;
pub type MessageCallback = Box<dyn Fn(&str, &str) + Send + 'static>;

pub struct Node {
    name: String,
    event_manager: Option<CommunicationManager>,
    message_manager: Option<CommunicationManager>,
    knowledge_manager: Option<KnowledgeManager>,
    running: Arc<Mutex<bool>>,
}

impl Node {
    pub fn new(name: &str, redis_host: &str, redis_port: u16) -> Result<Self> {
        let mut node = Node {
            name: name.to_string(),
            event_manager: None,
            message_manager: None,
            knowledge_manager: None,
            running: Arc::new(Mutex::new(false)),
        };

        // Initialize managers with different Redis databases
        node.event_manager = Some(CommunicationManager::new(redis_host, redis_port, 0)?);      // DB 0 for events
        node.message_manager = Some(CommunicationManager::new(redis_host, redis_port, 1)?);    // DB 1 for messages
        node.knowledge_manager = Some(KnowledgeManager::new(redis_host, redis_port, 2)?);      // DB 2 for knowledge

        log::info!("Node {} initialized", name);
        Ok(node)
    }

    pub fn start(&mut self) -> Result<()> {
        if *self.running.lock().unwrap() {
            return Ok(());
        }

        log::info!("Starting node {}", self.name);

        // Start event manager
        if let Some(ref mut event_mgr) = self.event_manager {
            event_mgr.start()?;
        }

        // Start message manager
        if let Some(ref mut message_mgr) = self.message_manager {
            message_mgr.start()?;
        }

        *self.running.lock().unwrap() = true;
        log::info!("Node {} started successfully", self.name);
        Ok(())
    }

    pub fn shutdown(&mut self) {
        if !*self.running.lock().unwrap() {
            return;
        }

        log::info!("Shutting down node {}", self.name);

        *self.running.lock().unwrap() = false;

        if let Some(ref mut event_mgr) = self.event_manager {
            event_mgr.stop();
        }

        if let Some(ref mut message_mgr) = self.message_manager {
            message_mgr.stop();
        }

        log::info!("Node {} shut down", self.name);
    }

    pub fn is_running(&self) -> bool {
        *self.running.lock().unwrap()
    }

    pub fn publish_event(&mut self, topic: &str, message: Value) -> Result<()> {
        if !self.is_running() {
            return Err(anyhow::anyhow!("Node is not running"));
        }

        // Add metadata
        let mut enriched_message = message;
        if let Value::Object(ref mut map) = enriched_message {
            map.insert("uid".to_string(), json!(self.generate_uuid()));
            map.insert("timestamp".to_string(), json!(self.get_timestamp()));
        }

        if let Some(ref mut event_mgr) = self.event_manager {
            let json_str = serde_json::to_string(&enriched_message)?;
            event_mgr.publish(topic, &json_str)?;
            log::info!("Published event to topic: {}", topic);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Event manager not initialized"))
        }
    }

    pub fn publish_message(&mut self, topic: &str, message: &str) -> Result<()> {
        if !self.is_running() {
            return Err(anyhow::anyhow!("Node is not running"));
        }

        if let Some(ref mut message_mgr) = self.message_manager {
            message_mgr.publish(topic, message)?;
            log::info!("Published message to topic: {}", topic);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Message manager not initialized"))
        }
    }

    pub fn register_event_callback<F>(&mut self, topic: &str, callback: F) -> Result<()>
    where
        F: Fn(&str, &str) + Send + 'static,
    {
        if !self.is_running() {
            return Err(anyhow::anyhow!("Node is not running"));
        }

        if let Some(ref mut event_mgr) = self.event_manager {
            event_mgr.subscribe(topic, callback)?;
            log::info!("Registered event callback for topic: {}", topic);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Event manager not initialized"))
        }
    }

    pub fn register_message_callback<F>(&mut self, topic: &str, callback: F) -> Result<()>
    where
        F: Fn(&str, &str) + Send + 'static,
    {
        if !self.is_running() {
            return Err(anyhow::anyhow!("Node is not running"));
        }

        if let Some(ref mut message_mgr) = self.message_manager {
            message_mgr.subscribe(topic, callback)?;
            log::info!("Registered message callback for topic: {}", topic);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Message manager not initialized"))
        }
    }

    pub fn write_knowledge(&mut self, key: &str, value: &str) -> Result<()> {
        if let Some(ref mut knowledge_mgr) = self.knowledge_manager {
            knowledge_mgr.write(key, value)?;
            log::debug!("Wrote knowledge: {}", key);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Knowledge manager not initialized"))
        }
    }

    pub fn read_knowledge(&mut self, key: &str) -> Result<Option<String>> {
        if let Some(ref mut knowledge_mgr) = self.knowledge_manager {
            let result = knowledge_mgr.read(key)?;
            if result.is_some() {
                log::debug!("Read knowledge: {}", key);
            } else {
                log::debug!("Knowledge not found: {}", key);
            }
            Ok(result)
        } else {
            Err(anyhow::anyhow!("Knowledge manager not initialized"))
        }
    }

    pub fn knowledge_exists(&mut self, key: &str) -> Result<bool> {
        if let Some(ref mut knowledge_mgr) = self.knowledge_manager {
            knowledge_mgr.exists(key)
        } else {
            Err(anyhow::anyhow!("Knowledge manager not initialized"))
        }
    }

    pub fn write_knowledge_object<T>(&mut self, key: &str, obj: &T) -> Result<()>
    where
        T: Serialize,
    {
        if let Some(ref mut knowledge_mgr) = self.knowledge_manager {
            knowledge_mgr.write_object(key, obj)?;
            log::debug!("Wrote knowledge object: {}", key);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Knowledge manager not initialized"))
        }
    }

    pub fn read_knowledge_object<T>(&mut self, key: &str) -> Result<Option<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        if let Some(ref mut knowledge_mgr) = self.knowledge_manager {
            let result = knowledge_mgr.read_object(key)?;
            if result.is_some() {
                log::debug!("Read knowledge object: {}", key);
            } else {
                log::debug!("Knowledge object not found: {}", key);
            }
            Ok(result)
        } else {
            Err(anyhow::anyhow!("Knowledge manager not initialized"))
        }
    }

    pub fn generate_uuid(&self) -> String {
        Uuid::new_v4().simple().to_string()[..8].to_string()
    }

    pub fn get_timestamp(&self) -> String {
        Utc::now().to_rfc3339()
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }
}
