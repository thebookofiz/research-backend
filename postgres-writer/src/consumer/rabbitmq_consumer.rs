use futures::StreamExt;
use lapin::{options::*, types::FieldTable, Channel, Connection, ConnectionProperties, Consumer};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::core::types::{RindexerEvent, StreamMessage};
use crate::error::{Result, SyncError};

pub struct RabbitMQConsumer {
    connection: Arc<Connection>,
    channels: Vec<Arc<Mutex<Channel>>>,
    consumers: Vec<Arc<Mutex<Consumer>>>,
    queue_prefix: String,
    exchanges: Vec<String>,
    routing_keys: Vec<String>,
    last_start_index: AtomicUsize,
}

impl RabbitMQConsumer {
    pub async fn new(
        rabbitmq_url: &str,
        queue_prefix: &str,
        exchanges: &[String],
        prefetch_count: u16,
    ) -> Result<Self> {
        let connection = Connection::connect(rabbitmq_url, ConnectionProperties::default())
            .await
            .map_err(|e| SyncError::Connection(format!("Failed to connect to RabbitMQ: {e}")))?;

        info!(
            "Connected to RabbitMQ with queue prefix: {} and exchanges: {:?}",
            queue_prefix, exchanges
        );

        // Define routing keys for each exchange
        let routing_keys = exchanges
            .iter()
            .map(|exchange| format!("intuition.{exchange}"))
            .collect::<Vec<_>>();

        let mut channels = Vec::new();
        let mut consumers = Vec::new();

        // Create a channel and consumer for each queue
        for (exchange, routing_key) in exchanges.iter().zip(routing_keys.iter()) {
            let channel = connection
                .create_channel()
                .await
                .map_err(|e| SyncError::Connection(format!("Failed to create channel: {e}")))?;

            // Set prefetch count for fair dispatch
            channel
                .basic_qos(prefetch_count, BasicQosOptions::default())
                .await
                .map_err(|e| SyncError::Connection(format!("Failed to set prefetch count: {e}")))?;

            // Declare exchange (direct type) - marked as durable for message durability
            // Note: This ensures the exchange survives RabbitMQ restarts
            // However, for full message persistence, rindexer must also publish with delivery_mode=2
            channel
                .exchange_declare(
                    exchange,
                    lapin::ExchangeKind::Direct,
                    ExchangeDeclareOptions {
                        durable: true,
                        auto_delete: false,
                        internal: false,
                        nowait: false,
                        passive: false,
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!("Failed to declare exchange {exchange}: {e}"))
                })?;

            // Declare queue
            let queue_name = format!("{queue_prefix}.{exchange}");
            channel
                .queue_declare(
                    &queue_name,
                    QueueDeclareOptions {
                        durable: true,
                        exclusive: false,
                        auto_delete: false,
                        nowait: false,
                        passive: false,
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!("Failed to declare queue {queue_name}: {e}"))
                })?;

            // Bind queue to exchange with routing key
            channel
                .queue_bind(
                    &queue_name,
                    exchange,
                    routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!(
                        "Failed to bind queue {queue_name} to exchange {exchange} with routing key {routing_key}: {e}"
                    ))
                })?;

            // Start consuming from the queue
            // Use UUID to ensure consumer tag uniqueness across restarts
            let consumer_tag = format!("{queue_prefix}_{exchange}_{}", Uuid::new_v4());
            let consumer = channel
                .basic_consume(
                    &queue_name,
                    &consumer_tag,
                    BasicConsumeOptions {
                        no_ack: false,
                        exclusive: false,
                        no_local: false,
                        nowait: false,
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!(
                        "Failed to start consumer for queue {queue_name}: {e}"
                    ))
                })?;

            info!(
                "Setup complete: consumer started for queue '{}' bound to exchange '{}' with routing key '{}'",
                queue_name, exchange, routing_key
            );

            channels.push(Arc::new(Mutex::new(channel)));
            consumers.push(Arc::new(Mutex::new(consumer)));
        }

        Ok(Self {
            connection: Arc::new(connection),
            channels,
            consumers,
            queue_prefix: queue_prefix.to_string(),
            exchanges: exchanges.to_vec(),
            routing_keys,
            last_start_index: AtomicUsize::new(0),
        })
    }

    pub async fn consume_batch(&self, batch_size: usize) -> Result<Vec<StreamMessage>> {
        debug!("consume_batch called with batch_size: {}", batch_size);

        let mut messages = Vec::new();
        let num_consumers = self.consumers.len();

        if num_consumers == 0 {
            return Ok(messages);
        }

        // Weighted round-robin: take up to 100 messages from each queue per cycle
        const CHUNK_SIZE: usize = 100;

        // Get starting index and rotate for next call
        let start_index = self.last_start_index.fetch_add(1, Ordering::Relaxed) % num_consumers;

        debug!(
            "Starting weighted round-robin from queue index {} (rotated)",
            start_index
        );

        // Keep cycling through queues until we have enough messages or all queues are exhausted
        let mut all_queues_empty = false;
        let mut cycle_count = 0;

        while messages.len() < batch_size && !all_queues_empty {
            all_queues_empty = true;
            cycle_count += 1;

            // Iterate through all queues starting from start_index
            for offset in 0..num_consumers {
                let idx = (start_index + offset) % num_consumers;
                let queue_name = format!("{}.{}", self.queue_prefix, self.exchanges[idx]);

                let mut consumer = self.consumers[idx].lock().await;

                // Take up to CHUNK_SIZE messages from this queue (or until batch is full)
                let messages_to_take = std::cmp::min(CHUNK_SIZE, batch_size - messages.len());
                let initial_count = messages.len();

                for _ in 0..messages_to_take {
                    // Try to get a message with a short timeout
                    match tokio::time::timeout(
                        tokio::time::Duration::from_millis(10),
                        consumer.next(),
                    )
                    .await
                    {
                        Ok(Some(Ok(delivery))) => {
                            // Parse the message
                            match self.parse_message(&delivery.data, &queue_name, &delivery.acker) {
                                Ok(mut parsed_messages) => {
                                    debug!(
                                        "Parsed {} messages from delivery on queue {}",
                                        parsed_messages.len(),
                                        queue_name
                                    );
                                    messages.append(&mut parsed_messages);
                                    all_queues_empty = false;
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to parse message from queue {}: {}",
                                        queue_name, e
                                    );
                                    // Reject the message (don't requeue malformed messages)
                                    if let Err(nack_err) = delivery
                                        .acker
                                        .nack(BasicNackOptions {
                                            requeue: false,
                                            multiple: false,
                                        })
                                        .await
                                    {
                                        error!("Failed to nack message: {}", nack_err);
                                    }
                                }
                            }
                        }
                        Ok(Some(Err(e))) => {
                            error!("Error receiving message from queue {}: {}", queue_name, e);
                            break;
                        }
                        Ok(None) => {
                            // Stream closed
                            warn!("Consumer stream closed for queue {}", queue_name);
                            break;
                        }
                        Err(_) => {
                            // Timeout - no more messages available from this queue
                            break;
                        }
                    }

                    // Stop if we've collected enough messages
                    if messages.len() >= batch_size {
                        break;
                    }
                }

                let collected = messages.len() - initial_count;
                if collected > 0 {
                    debug!(
                        "Collected {} messages from queue {} (cycle {})",
                        collected, queue_name, cycle_count
                    );
                }

                // Stop if we've collected enough messages
                if messages.len() >= batch_size {
                    break;
                }
            }
        }

        debug!(
            "Consumed {} messages total across {} cycle(s)",
            messages.len(),
            cycle_count
        );
        Ok(messages)
    }

    pub async fn ack_message(&self, acker: &lapin::acker::Acker) -> Result<()> {
        acker
            .ack(BasicAckOptions { multiple: false })
            .await
            .map_err(|e| SyncError::Connection(format!("Failed to acknowledge message: {e}")))?;

        Ok(())
    }

    pub async fn nack_message(&self, acker: &lapin::acker::Acker, requeue: bool) -> Result<()> {
        acker
            .nack(BasicNackOptions {
                requeue,
                multiple: false,
            })
            .await
            .map_err(|e| SyncError::Connection(format!("Failed to nack message: {e}")))?;

        Ok(())
    }

    fn parse_message(
        &self,
        data: &[u8],
        queue_name: &str,
        acker: &lapin::acker::Acker,
    ) -> Result<Vec<StreamMessage>> {
        let json_str = String::from_utf8_lossy(data);

        debug!("Parsing message from queue {}: {}", queue_name, json_str);

        let event: RindexerEvent = serde_json::from_str(&json_str)
            .map_err(|e| SyncError::ParseError(format!("Failed to parse RindexerEvent: {e}")))?;

        debug!(
            "Parsed event: {} with signature: {}",
            event.event_name, event.event_signature_hash
        );

        let mut messages = Vec::new();

        // Handle array event_data - create one StreamMessage per blockchain event
        if let Some(array) = event.event_data.as_array() {
            if !array.is_empty() {
                debug!(
                    "Event data is an array with {} blockchain events, creating individual StreamMessages",
                    array.len()
                );

                // Process each blockchain event in the array
                // Only attach acker to the LAST message to prevent "already used Acker" errors
                // (RabbitMQ only allows one ack/nack per delivery)
                let last_index = array.len() - 1;
                for (index, event_data) in array.iter().enumerate() {
                    let mut individual_event = event.clone();
                    individual_event.event_data = event_data.clone();

                    messages.push(StreamMessage {
                        id: format!("{queue_name}-{index}"),
                        event: individual_event,
                        source_stream: queue_name.to_string(),
                        // Only the last message gets the acker
                        acker: if index == last_index {
                            Some(acker.clone())
                        } else {
                            None
                        },
                    });
                }
            }
        } else {
            // Single event case
            messages.push(StreamMessage {
                id: queue_name.to_string(),
                event,
                source_stream: queue_name.to_string(),
                acker: Some(acker.clone()),
            });
        }

        Ok(messages)
    }

    pub async fn reconnect(&mut self, rabbitmq_url: &str, prefetch_count: u16) -> Result<()> {
        warn!("Attempting to reconnect to RabbitMQ...");

        let connection = Connection::connect(rabbitmq_url, ConnectionProperties::default())
            .await
            .map_err(|e| SyncError::Connection(format!("Failed to reconnect to RabbitMQ: {e}")))?;

        let mut channels = Vec::new();
        let mut consumers = Vec::new();

        // Recreate channels and consumers
        for (exchange, routing_key) in self.exchanges.iter().zip(self.routing_keys.iter()) {
            let channel = connection.create_channel().await.map_err(|e| {
                SyncError::Connection(format!("Failed to create channel on reconnect: {e}"))
            })?;

            channel
                .basic_qos(prefetch_count, BasicQosOptions::default())
                .await
                .map_err(|e| {
                    SyncError::Connection(format!("Failed to set prefetch count on reconnect: {e}"))
                })?;

            // Re-declare exchange (in case it was deleted during downtime)
            channel
                .exchange_declare(
                    exchange,
                    lapin::ExchangeKind::Direct,
                    ExchangeDeclareOptions {
                        durable: false,
                        auto_delete: false,
                        internal: false,
                        nowait: false,
                        passive: false,
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!(
                        "Failed to declare exchange {exchange} on reconnect: {e}"
                    ))
                })?;

            let queue_name = format!("{}.{exchange}", self.queue_prefix);

            // Re-declare queue (in case it was deleted during downtime)
            channel
                .queue_declare(
                    &queue_name,
                    QueueDeclareOptions {
                        durable: true,
                        exclusive: false,
                        auto_delete: false,
                        nowait: false,
                        passive: false,
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!(
                        "Failed to declare queue {queue_name} on reconnect: {e}"
                    ))
                })?;

            // Re-bind queue to exchange
            channel
                .queue_bind(
                    &queue_name,
                    exchange,
                    routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!(
                        "Failed to bind queue {queue_name} to exchange {exchange} on reconnect: {e}"
                    ))
                })?;

            // Start consuming
            // Use UUID to ensure consumer tag uniqueness across restarts
            let consumer_tag = format!("{}_{exchange}_{}", self.queue_prefix, Uuid::new_v4());
            let consumer = channel
                .basic_consume(
                    &queue_name,
                    &consumer_tag,
                    BasicConsumeOptions {
                        no_ack: false,
                        exclusive: false,
                        no_local: false,
                        nowait: false,
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    SyncError::Connection(format!(
                        "Failed to start consumer for queue {queue_name} on reconnect: {e}"
                    ))
                })?;

            channels.push(Arc::new(Mutex::new(channel)));
            consumers.push(Arc::new(Mutex::new(consumer)));
        }

        self.connection = Arc::new(connection);
        self.channels = channels;
        self.consumers = consumers;

        info!("Successfully reconnected to RabbitMQ");
        Ok(())
    }

    /// Get the list of queue names being consumed
    pub fn get_queue_names(&self) -> Vec<String> {
        self.exchanges
            .iter()
            .map(|e| format!("{}.{}", self.queue_prefix, e))
            .collect()
    }

    /// Get the depth (message count) of a queue
    pub async fn get_queue_depth(&self, queue_name: &str) -> Result<u32> {
        // Get the first channel to query queue info
        if let Some(channel) = self.channels.first() {
            let channel = channel.lock().await;

            // Use queue_declare with passive=true to get queue info without modifying it
            match channel
                .queue_declare(
                    queue_name,
                    QueueDeclareOptions {
                        passive: true, // Just query, don't create
                        durable: true, // Must match actual queue configuration
                        exclusive: false,
                        auto_delete: false,
                        nowait: false,
                    },
                    FieldTable::default(),
                )
                .await
            {
                Ok(queue) => Ok(queue.message_count()),
                Err(e) => {
                    warn!("Failed to get queue depth for {}: {}", queue_name, e);
                    Ok(0) // Return 0 on error instead of failing
                }
            }
        } else {
            warn!("No channels available to query queue depth");
            Ok(0)
        }
    }
}
