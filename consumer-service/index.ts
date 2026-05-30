import { pool } from './db';
import amqp from 'amqplib';
import dotenv from 'dotenv';
dotenv.config();

let connection: any;
let channel: any;

async function initRabbitMQ() {
  const brokerUrl = process.env.BROKER_URL || 'amqp://localhost:5672';
  try {
    connection = await amqp.connect(brokerUrl);
    channel = await connection.createChannel();
    await channel.assertExchange('events', 'topic', { durable: true });
    
    const q = await channel.assertQueue('analytics_queue', { durable: true });
    await channel.bindQueue(q.queue, 'events', 'order-events');
    
    channel.consume(q.queue, async (msg) => {
      if (msg) {
        await processMessage(msg);
        channel.ack(msg);
      }
    });
    
    console.log('Consumer connected to RabbitMQ and waiting for messages...');
  } catch (error) {
    console.error('RabbitMQ connection error:', error);
    setTimeout(initRabbitMQ, 5000);
  }
}

async function processMessage(msg: amqp.ConsumeMessage) {
  const payload = JSON.parse(msg.content.toString());
  const messageId = msg.properties.messageId;
  
  if (!messageId) {
    console.warn('Message received without messageId, skipping.');
    return;
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    
    // Check idempotency
    const processedCheck = await client.query('SELECT id FROM processed_events WHERE id = $1', [messageId]);
    if (processedCheck.rows.length > 0) {
      console.log(`Message ${messageId} already processed, skipping.`);
      await client.query('ROLLBACK');
      return;
    }
    
    if (payload.eventType === 'OrderCreated') {
      const { customerId, items, total, timestamp } = payload;
      
      // Update customer_ltv_view
      await client.query(`
        INSERT INTO customer_ltv_view (customer_id, total_spent, order_count, last_order_date)
        VALUES ($1, $2, 1, $3)
        ON CONFLICT (customer_id) DO UPDATE 
        SET total_spent = customer_ltv_view.total_spent + $2,
            order_count = customer_ltv_view.order_count + 1,
            last_order_date = GREATEST(customer_ltv_view.last_order_date, $3)
      `, [customerId, total, timestamp]);
      
      // Update hourly_sales_view
      const hourTimestamp = new Date(timestamp);
      hourTimestamp.setMinutes(0, 0, 0);
      await client.query(`
        INSERT INTO hourly_sales_view (hour_timestamp, total_orders, total_revenue)
        VALUES ($1, 1, $2)
        ON CONFLICT (hour_timestamp) DO UPDATE 
        SET total_orders = hourly_sales_view.total_orders + 1,
            total_revenue = hourly_sales_view.total_revenue + $2
      `, [hourTimestamp.toISOString(), total]);
      
      // Update product_sales_view and category_metrics_view for each item
      for (const item of items) {
        // Fetch product category (in a real system, you might include category in the event payload)
        const productRes = await client.query('SELECT category FROM products WHERE id = $1', [item.productId]);
        const category = productRes.rows[0]?.category || 'Unknown';
        
        await client.query(`
          INSERT INTO product_sales_view (product_id, total_quantity_sold, total_revenue, order_count)
          VALUES ($1, $2, $3, 1)
          ON CONFLICT (product_id) DO UPDATE 
          SET total_quantity_sold = product_sales_view.total_quantity_sold + $2,
              total_revenue = product_sales_view.total_revenue + $3,
              order_count = product_sales_view.order_count + 1
        `, [item.productId, item.quantity, item.price * item.quantity]);
        
        await client.query(`
          INSERT INTO category_metrics_view (category_name, total_revenue, total_orders)
          VALUES ($1, $2, 1)
          ON CONFLICT (category_name) DO UPDATE 
          SET total_revenue = category_metrics_view.total_revenue + $2,
              total_orders = category_metrics_view.total_orders + 1
        `, [category, item.price * item.quantity]);
      }
      
      // Mark event as processed
      await client.query('INSERT INTO processed_events (id) VALUES ($1)', [messageId]);
      
      // Update sync status
      await client.query('UPDATE sync_status SET last_processed_event_timestamp = $1 WHERE id = 1', [timestamp]);
      
    }
    
    await client.query('COMMIT');
    console.log(`Successfully processed message ${messageId}`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`Error processing message ${messageId}:`, error);
  } finally {
    client.release();
  }
}

initRabbitMQ();
