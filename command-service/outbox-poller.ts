import { pool } from './db';
import amqp from 'amqplib';
import dotenv from 'dotenv';
dotenv.config();

let connection: any;
let channel: any;

export async function initRabbitMQ() {
  const brokerUrl = process.env.BROKER_URL || 'amqp://localhost:5672';
  try {
    connection = await amqp.connect(brokerUrl);
    channel = await connection.createChannel();
    await channel.assertExchange('events', 'topic', { durable: true });
    console.log('Connected to RabbitMQ');
  } catch (error) {
    console.error('RabbitMQ connection error:', error);
    setTimeout(initRabbitMQ, 5000);
  }
}

export async function pollOutbox() {
  if (!channel) return;
  
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const res = await client.query(`
      SELECT id, topic, payload FROM outbox 
      WHERE published_at IS NULL 
      ORDER BY created_at ASC 
      FOR UPDATE SKIP LOCKED 
      LIMIT 100
    `);
    
    if (res.rows.length > 0) {
      for (const row of res.rows) {
        const published = channel.publish(
          'events', 
          row.topic, 
          Buffer.from(JSON.stringify(row.payload)),
          { messageId: row.id.toString(), persistent: true }
        );
        
        if (published) {
          await client.query('UPDATE outbox SET published_at = NOW() WHERE id = $1', [row.id]);
        }
      }
    }
    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error polling outbox:', err);
  } finally {
    client.release();
  }
}

export function startPoller() {
  setInterval(pollOutbox, 1000);
}
