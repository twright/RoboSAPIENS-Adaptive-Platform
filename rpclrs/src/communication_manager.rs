use redis::{Client, Connection, Commands, PubSub};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::mpsc;
use anyhow::Result;

pub type MessageCallback = Box<dyn Fn(&str, &str) + Send + 'static>;

pub struct CommunicationManager {
    client: Client,
    db: i32,
    connection: Option<Connection>,
    pubsub: Option<PubSub>,
    callbacks: Arc<Mutex<HashMap<String, MessageCallback>>>,
    running: Arc<Mutex<bool>>,
}

impl CommunicationManager {
    pub fn new(host: &str, port: u16, db: i32) -> Result<Self> {
        let client = Client::open(format!("redis://{}:{}/{}", host, port, db))?;
        
        Ok(CommunicationManager {
            client,
            db,
            connection: None,
            pubsub: None,
            callbacks: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(Mutex::new(false)),
        })
    }

    pub fn start(&mut self) -> Result<()> {
        // Create connection for publishing
        self.connection = Some(self.client.get_connection()?);
        
        // Create pubsub for subscribing
        self.pubsub = Some(self.client.get_pubsub()?);
        
        *self.running.lock().unwrap() = true;
        
        // Start subscriber thread
        self.start_subscriber_thread();
        
        log::info!("Communication manager started on database {}", self.db);
        Ok(())
    }

    pub fn stop(&mut self) {
        *self.running.lock().unwrap() = false;
        self.connection = None;
        self.pubsub = None;
        log::info!("Communication manager stopped");
    }

    pub fn publish(&mut self, topic: &str, message: &str) -> Result<()> {
        if let Some(ref mut conn) = self.connection {
            let _: () = conn.publish(topic, message)?;
            log::debug!("Published message to topic: {}", topic);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Not connected"))
        }
    }

    pub fn subscribe<F>(&mut self, topic: &str, callback: F) -> Result<()>
    where
        F: Fn(&str, &str) + Send + 'static,
    {
        // Store callback
        {
            let mut callbacks = self.callbacks.lock().unwrap();
            callbacks.insert(topic.to_string(), Box::new(callback));
        }

        // Subscribe to topic
        if let Some(ref mut pubsub) = self.pubsub {
            pubsub.subscribe(topic)?;
            log::info!("Subscribed to topic: {}", topic);
            Ok(())
        } else {
            Err(anyhow::anyhow!("PubSub not initialized"))
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connection.is_some() && self.pubsub.is_some()
    }

    fn start_subscriber_thread(&self) {
        let callbacks = Arc::clone(&self.callbacks);
        let running = Arc::clone(&self.running);
        let client = self.client.clone();
        let db = self.db;

        thread::spawn(move || {
            if let Ok(mut pubsub) = client.get_pubsub() {
                while *running.lock().unwrap() {
                    if let Ok(msg) = pubsub.get_message() {
                        if let Ok(payload) = msg.get_payload::<String>() {
                            let channel = msg.get_channel_name().to_string();
                            
                            let callbacks = callbacks.lock().unwrap();
                            if let Some(callback) = callbacks.get(&channel) {
                                callback(&channel, &payload);
                            }
                        }
                    }
                }
            }
        });
    }
}
