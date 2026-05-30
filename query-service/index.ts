import express from 'express';
import { pool } from './db';

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

app.get('/api/analytics/products/:productId/sales', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM product_sales_view WHERE product_id = $1', [req.params.productId]);
    if (result.rows.length === 0) {
      return res.status(200).json({
        productId: parseInt(req.params.productId),
        totalQuantitySold: 0,
        totalRevenue: 0,
        orderCount: 0
      });
    }
    const row = result.rows[0];
    res.status(200).json({
      productId: row.product_id,
      totalQuantitySold: parseInt(row.total_quantity_sold),
      totalRevenue: parseFloat(row.total_revenue),
      orderCount: parseInt(row.order_count)
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/analytics/categories/:category/revenue', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM category_metrics_view WHERE category_name = $1', [req.params.category]);
    if (result.rows.length === 0) {
      return res.status(200).json({
        category: req.params.category,
        totalRevenue: 0,
        totalOrders: 0
      });
    }
    const row = result.rows[0];
    res.status(200).json({
      category: row.category_name,
      totalRevenue: parseFloat(row.total_revenue),
      totalOrders: parseInt(row.total_orders)
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/analytics/customers/:customerId/lifetime-value', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM customer_ltv_view WHERE customer_id = $1', [req.params.customerId]);
    if (result.rows.length === 0) {
      return res.status(200).json({
        customerId: parseInt(req.params.customerId),
        totalSpent: 0,
        orderCount: 0,
        lastOrderDate: null
      });
    }
    const row = result.rows[0];
    res.status(200).json({
      customerId: row.customer_id,
      totalSpent: parseFloat(row.total_spent),
      orderCount: parseInt(row.order_count),
      lastOrderDate: row.last_order_date
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.get('/api/analytics/sync-status', async (req, res) => {
  try {
    const statusResult = await pool.query('SELECT last_processed_event_timestamp FROM sync_status WHERE id = 1');
    const outboxResult = await pool.query('SELECT MAX(created_at) as latest_event FROM outbox');
    
    const lastProcessed = statusResult.rows[0]?.last_processed_event_timestamp || null;
    const latestCreated = outboxResult.rows[0]?.latest_event || null;
    
    let lagSeconds = 0;
    if (latestCreated && lastProcessed) {
      lagSeconds = (new Date(latestCreated).getTime() - new Date(lastProcessed).getTime()) / 1000;
      if (lagSeconds < 0) lagSeconds = 0;
    } else if (latestCreated && !lastProcessed) {
      lagSeconds = (new Date().getTime() - new Date(latestCreated).getTime()) / 1000;
    }

    res.status(200).json({
      lastProcessedEventTimestamp: lastProcessed,
      lagSeconds: parseFloat(lagSeconds.toFixed(2))
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

const PORT = process.env.PORT || 8081;
app.listen(PORT, () => {
  console.log(`Query service listening on port ${PORT}`);
});
