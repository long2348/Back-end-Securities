import { registerAs } from '@nestjs/config';

export default registerAs('database', () => ({
  host: process.env.DB_HOST ?? 'localhost',
  port: parseInt(process.env.DB_PORT ?? '5432', 10),
  name: process.env.DB_NAME ?? 'be_securities',
  user: process.env.DB_USER ?? 'securities_user',
  pass: process.env.DB_PASS ?? 'securities_pass',
}));
