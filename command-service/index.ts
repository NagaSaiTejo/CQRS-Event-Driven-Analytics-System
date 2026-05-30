import express from 'express';
import { pool } from './db';
import { initRabbitMQ, startPoller } from './outbox-poller';

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

app.post('/api/products', async (req, res) => {
  const { name, category, price, stock } = req.body;
  try {
    const result = await pool.query(
      'INSERT INTO products (name, category, price, stock) VALUES ($1, $2, $3, $4) RETURNING id',
      [name, category, price, stock]
    );
    res.status(201).json({ productId: result.rows[0].id });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.post('/api/orders', async (req, res) => {
  const { customerId, items } = req.body;
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    let total = 0;
    for (const item of items) {
      total += item.price * item.quantity;
    }
    
    const orderResult = await client.query(
      'INSERT INTO orders (customer_id, total, status) VALUES ($1, $2, $3) RETURNING id, created_at',
      [customerId, total, 'CREATED']
    );
    const orderId = orderResult.rows[0].id;
    const createdAt = orderResult.rows[0].created_at;
    
    for (const item of items) {
      await client.query(
        'INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)',
        [orderId, item.productId, item.quantity, item.price]
      );
    }
    
    const payload = {
      eventType: 'OrderCreated',
      orderId,
      customerId,
      items,
      total,
      timestamp: createdAt.toISOString()
    };
    
    await client.query(
      'INSERT INTO outbox (topic, payload) VALUES ($1, $2)',
      ['order-events', payload]
    );
    
    await client.query('COMMIT');
    res.status(201).json({ orderId });
  } catch (error) {
    await client.query('ROLLBACK');
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  } finally {
    client.release();
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, async () => {
  console.log(`Command service listening on port ${PORT}`);
  await initRabbitMQ();
  startPoller();
});
