module.exports = {
  apps: [{
    name: 'xdialcore-api',
    cwd: '/root/xdialcoreapi',
    script: '.venv/bin/python',
    args: '-m uvicorn main:app --host 0.0.0.0 --port 8001',
    instances: 1,
    exec_mode: 'fork',
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      // Database Configuration
      DATABASE_URL: 'postgresql://xdialcore:xdialcore@localhost:5432/xdialcore',
      
      // JWT Configuration
      JWT_SECRET_KEY: 'your-secret-key-change-in-production-min-32-chars-long',
      JWT_ALGORITHM: 'HS256',
      ACCESS_TOKEN_EXPIRE_MINUTES: '1440',
      
      // Application Configuration
      APP_NAME: 'Xdial Core API',
      DEBUG: 'True',
      API_PREFIX: '/api/v1',
      ALLOWED_ORIGINS: '*',
      
      // Python environment
      PYTHONUNBUFFERED: '1'
    },
    error_file: '/root/xdialcoreapi/logs/pm2-error.log',
    out_file: '/root/xdialcoreapi/logs/pm2-out.log',
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: true,
    min_uptime: '10s',
    max_restarts: 10,
    restart_delay: 4000
  }]
};