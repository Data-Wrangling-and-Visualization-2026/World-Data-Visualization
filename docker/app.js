const express = require('express');
const dotenv = require('dotenv');
const { Pool } = require('pg');


dotenv.config();

const app = express();
const PORT = process.env.PORT || 8000;

app.use(express.json());


const pool = new Pool({
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

// Эндпоинт: GET /data — возвращает очищенные данные
app.get('/data', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM country'); // ← ваша очищенная таблица
    const data = result.rows;
    
    res.json({
      status: 'success',
      data: data,
      count: data.length
    });
  } catch (error) {
    console.error('Error of DB query:', error);
    res.status(500).json({
      status: 'error',
      data: [],
      count: 0,
      message: error.message
    });
  }
});

app.get('/', (req, res) => {
  res.json({
    message: 'Data Reader API is running',
    endpoints: ['/data']
  });
});

app.listen(PORT, () => {
  console.log(`Server has launched at http://localhost:${PORT}`);
});

process.on('SIGTERM', () => {
  console.log('Interruption of server...');
  pool.end().then(() => process.exit(0));
});
