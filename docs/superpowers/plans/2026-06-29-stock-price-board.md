# Stock Price Board (Bảng Giá Chứng Khoán) — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Xây dựng backend hệ thống bảng giá chứng khoán real-time Phase 1 với NestJS — bao gồm ingestion giá từ Kafka, cache bằng Redis, REST API để tải dữ liệu ban đầu, và WebSocket để đẩy cập nhật giá real-time.

**Architecture:** Kafka consumer nhận price-update events và lưu vào Redis (single source of truth cho giá live). PriceBoardService tổng hợp thông tin cổ phiếu từ PostgreSQL (metadata) và Redis (giá live) để phục vụ REST API và WebSocket gateway. WebSocket gateway broadcast giá cập nhật đến clients theo cơ chế subscribe theo room (ticker/toàn thị trường).

**Tech Stack:** NestJS 10, TypeScript, TypeORM + PostgreSQL 15 (metadata cổ phiếu), ioredis + Redis 7 (price cache), kafkajs / @nestjs/microservices Kafka transport (event ingestion), @nestjs/websockets + socket.io (real-time), Docker Compose (local infra).

---

## Global Constraints

- Node.js >= 20 LTS
- NestJS >= 10.x
- TypeScript strict mode bật
- Tất cả DTO phải có class-validator decorators
- Tất cả response phải bọc trong wrapper `{ success, data, message }`
- Giá chứng khoán VN: đơn vị VND, bước giá 100 VND (HOSE), làm tròn đến hàng trăm
- Giá trần HOSE = reference_price * 1.07, giá sàn = reference_price * 0.93
- Giá trần HNX = reference_price * 1.10, giá sàn = reference_price * 0.90
- Tên Redis key: `price:{ticker}` (e.g. `price:VNM`)
- Kafka topic: `market.price-updates`
- Không dùng `any` type trong TypeScript
- Test phải chạy `npm run test` không lỗi

---

## Quan Hệ Giữa Các Module

```
┌─────────────────────────────────────────────────────────────────┐
│                         AppModule                               │
│                                                                 │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐   │
│  │ StocksModule │   │MarketData    │   │ PriceBoardModule │   │
│  │              │   │Module        │   │                  │   │
│  │ - Entity     │   │              │   │ - REST /price-   │   │
│  │ - CRUD API   │   │ - Kafka      │   │   board          │   │
│  │ - Seed data  │   │   Consumer   │   │ - Tổng hợp data  │   │
│  └──────┬───────┘   │ - Update     │   └────────┬─────────┘   │
│         │           │   Redis      │            │              │
│         │           └──────┬───────┘            │              │
│         │                  │                    │              │
│         │           ┌──────▼───────────────────▼─────────┐    │
│         └──────────►│         PriceCacheModule            │    │
│                     │    (Redis — ioredis wrapper)        │    │
│                     └─────────────────────────────────────┘    │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    GatewayModule                        │   │
│  │   - WebSocket Gateway (socket.io)                       │   │
│  │   - Subscribe room: "board" (toàn bộ bảng giá)          │   │
│  │   - Subscribe room: "ticker:{symbol}" (1 mã cụ thể)     │   │
│  │   - MarketDataModule emit event → Gateway broadcast     │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘

External:
  Kafka (market.price-updates) ──► MarketDataModule Consumer
  PostgreSQL ──────────────────────► StocksModule Repository
  Redis ───────────────────────────► PriceCacheModule
```

---

## Cấu Trúc Thư Mục Dự Án

```
BESecurities/
├── src/
│   ├── main.ts
│   ├── app.module.ts
│   ├── config/
│   │   ├── app.config.ts
│   │   ├── database.config.ts
│   │   ├── redis.config.ts
│   │   └── kafka.config.ts
│   ├── common/
│   │   ├── constants/
│   │   │   └── kafka-topics.constant.ts
│   │   ├── dto/
│   │   │   └── api-response.dto.ts
│   │   ├── filters/
│   │   │   └── ws-exception.filter.ts
│   │   └── types/
│   │       └── price-data.type.ts
│   └── modules/
│       ├── stocks/
│       │   ├── entities/stock.entity.ts
│       │   ├── dto/create-stock.dto.ts
│       │   ├── dto/stock-response.dto.ts
│       │   ├── stocks.controller.ts
│       │   ├── stocks.service.ts
│       │   ├── stocks.repository.ts
│       │   └── stocks.module.ts
│       ├── market-data/
│       │   ├── dto/price-update.dto.ts
│       │   ├── market-data.consumer.ts
│       │   ├── market-data.service.ts
│       │   └── market-data.module.ts
│       ├── price-cache/
│       │   ├── price-cache.service.ts
│       │   └── price-cache.module.ts
│       ├── price-board/
│       │   ├── dto/price-board-item.dto.ts
│       │   ├── price-board.controller.ts
│       │   ├── price-board.service.ts
│       │   └── price-board.module.ts
│       └── gateway/
│           ├── price-board.gateway.ts
│           └── gateway.module.ts
├── test/
│   └── app.e2e-spec.ts
├── scripts/
│   └── market-simulator.ts
├── docker/
│   └── docker-compose.yml
├── .env.example
├── package.json
├── tsconfig.json
└── nest-cli.json
```

---

## Yêu Cầu Chức Năng (Functional Requirements)

| ID   | Yêu cầu |
|------|---------|
| FR01 | API trả về danh sách toàn bộ cổ phiếu với giá live từ Redis |
| FR02 | API trả về chi tiết 1 cổ phiếu theo ticker |
| FR03 | Kafka consumer nhận price-update event và cập nhật Redis |
| FR04 | WebSocket clients subscribe room "board" nhận broadcast khi có giá cập nhật |
| FR05 | WebSocket clients subscribe room "ticker:{symbol}" chỉ nhận update của mã đó |
| FR06 | Bảng giá hiển thị: ticker, tên, giá tham chiếu, giá trần, giá sàn, giá hiện tại, %, KL |
| FR07 | Color flag: INCREASE / DECREASE / CEILING / FLOOR / REFERENCE / NO_CHANGE |
| FR08 | Seed 20 cổ phiếu HOSE phổ biến (VNM, VIC, VHM, HPG, MWG, SSI, VCB, BID, CTG, FPT...) |
| FR09 | Health check endpoint `GET /health` |
| FR10 | Market Data Simulator: script publish Kafka messages giả lập biến động giá |

## Yêu Cầu Phi Chức Năng (Non-Functional Requirements)

| ID    | Yêu cầu |
|-------|---------|
| NFR01 | Latency price update Kafka → WebSocket client < 100ms |
| NFR02 | Redis cache không dùng TTL cố định — cập nhật theo event |
| NFR03 | Kafka consumer group: `be-securities-group` |
| NFR04 | Graceful shutdown |
| NFR05 | Tất cả infra chạy qua Docker Compose |
| NFR06 | Unit test coverage >= 80% cho services |
| NFR07 | API response time < 50ms |

---

### Task 1: Project Bootstrap & Docker Infrastructure

**Files:**
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `nest-cli.json`
- Create: `.env.example`
- Create: `docker/docker-compose.yml`
- Create: `src/main.ts`
- Create: `src/app.module.ts`
- Create: `src/config/app.config.ts`

**Interfaces:**
- Produces: NestJS app chạy được trên port 3000, Docker Compose bring up PG + Redis + Kafka

- [ ] **Step 1: Khởi tạo NestJS project**

```bash
cd c:\NestJSFolder\BESecurities
npm i -g @nestjs/cli
nest new . --package-manager npm --skip-git
```

Expected: project scaffold với `src/main.ts`, `src/app.module.ts`

- [ ] **Step 2: Cài đặt dependencies**

```bash
npm install @nestjs/config @nestjs/typeorm typeorm pg \
  @nestjs/microservices kafkajs \
  ioredis \
  @nestjs/websockets @nestjs/platform-socket.io socket.io \
  @nestjs/event-emitter \
  class-validator class-transformer \
  @nestjs/terminus

npm install -D @types/node
```

- [ ] **Step 3: Tạo `docker/docker-compose.yml`**

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: be_securities
      POSTGRES_USER: securities_user
      POSTGRES_PASSWORD: securities_pass
    ports:
      - "5432:5432"
    volumes:
      - pg_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'

volumes:
  pg_data:
```

- [ ] **Step 4: Tạo `.env.example`**

```
APP_PORT=3000
NODE_ENV=development
DB_HOST=localhost
DB_PORT=5432
DB_NAME=be_securities
DB_USER=securities_user
DB_PASS=securities_pass
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
KAFKA_BROKERS=localhost:9092
KAFKA_GROUP_ID=be-securities-group
```

- [ ] **Step 5: Tạo `src/config/app.config.ts`**

```typescript
import { registerAs } from '@nestjs/config';
export default registerAs('app', () => ({
  port: parseInt(process.env.APP_PORT ?? '3000', 10),
  env: process.env.NODE_ENV ?? 'development',
}));
```

- [ ] **Step 6: Cập nhật `src/app.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { EventEmitterModule } from '@nestjs/event-emitter';
import appConfig from './config/app.config';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, load: [appConfig] }),
    EventEmitterModule.forRoot(),
  ],
})
export class AppModule {}
```

- [ ] **Step 7: Cập nhật `src/main.ts`**

```typescript
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.setGlobalPrefix('api');
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: { brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',') },
      consumer: { groupId: process.env.KAFKA_GROUP_ID ?? 'be-securities-group' },
    },
  });
  await app.startAllMicroservices();
  await app.listen(process.env.APP_PORT ?? 3000);
  console.log(`Application running on port ${process.env.APP_PORT ?? 3000}`);
}
bootstrap();
```

- [ ] **Step 8: Chạy Docker Compose**

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
```

Expected: postgres, redis, zookeeper, kafka đều `Up`

- [ ] **Step 9: Xác nhận app khởi động**

```bash
npm run start:dev
```

Expected: `Application running on port 3000`

- [ ] **Step 10: Dọn file scaffold dư** — xóa `app.controller.ts`, `app.service.ts`, `app.controller.spec.ts`

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "feat: bootstrap NestJS project with Docker Compose infra (PG + Redis + Kafka)"
```

---

### Task 2: Common Types, Constants & Response Wrapper

**Files:**
- Create: `src/common/types/price-data.type.ts`
- Create: `src/common/constants/kafka-topics.constant.ts`
- Create: `src/common/dto/api-response.dto.ts`
- Create: `src/common/dto/api-response.dto.spec.ts`
- Create: `src/common/filters/ws-exception.filter.ts`

**Interfaces:**
- Produces:
  - `PriceColorFlag` enum: `CEILING | FLOOR | INCREASE | DECREASE | REFERENCE | NO_CHANGE`
  - `PriceData` interface: `{ ticker, currentPrice, matchedPrice, volume, totalVolume, change, changePercent, colorFlag, updatedAt }`
  - `KafkaTopics.PRICE_UPDATES = 'market.price-updates'`
  - `ApiResponse<T>.ok(data)` / `ApiResponse<T>.fail(message)`

- [ ] **Step 1: Viết test trước cho ApiResponse**

```typescript
// src/common/dto/api-response.dto.spec.ts
import { ApiResponse } from './api-response.dto';

describe('ApiResponse', () => {
  it('ok() returns success=true with data', () => {
    const res = ApiResponse.ok({ id: 1 });
    expect(res.success).toBe(true);
    expect(res.data).toEqual({ id: 1 });
    expect(res.message).toBe('Success');
  });

  it('fail() returns success=false with null data', () => {
    const res = ApiResponse.fail('Not found');
    expect(res.success).toBe(false);
    expect(res.data).toBeNull();
    expect(res.message).toBe('Not found');
  });

  it('ok() accepts custom message', () => {
    const res = ApiResponse.ok([], 'Empty list');
    expect(res.message).toBe('Empty list');
  });
});
```

- [ ] **Step 2: Chạy test — phải FAIL**

```bash
npm run test -- --testPathPattern=api-response
```

Expected: FAIL — module not found

- [ ] **Step 3: Tạo `src/common/dto/api-response.dto.ts`**

```typescript
export class ApiResponse<T> {
  success: boolean;
  data: T | null;
  message: string;

  static ok<T>(data: T, message = 'Success'): ApiResponse<T> {
    return { success: true, data, message };
  }

  static fail<T>(message: string): ApiResponse<T> {
    return { success: false, data: null, message };
  }
}
```

- [ ] **Step 4: Chạy test — phải PASS**

```bash
npm run test -- --testPathPattern=api-response
```

Expected: 3 tests PASS

- [ ] **Step 5: Tạo `src/common/types/price-data.type.ts`**

```typescript
export enum PriceColorFlag {
  CEILING = 'CEILING',
  FLOOR = 'FLOOR',
  INCREASE = 'INCREASE',
  DECREASE = 'DECREASE',
  REFERENCE = 'REFERENCE',
  NO_CHANGE = 'NO_CHANGE',
}

export interface PriceData {
  ticker: string;
  currentPrice: number;
  matchedPrice: number;
  volume: number;
  totalVolume: number;
  change: number;
  changePercent: number;
  colorFlag: PriceColorFlag;
  updatedAt: string;
}
```

- [ ] **Step 6: Tạo `src/common/constants/kafka-topics.constant.ts`**

```typescript
export enum KafkaTopics {
  PRICE_UPDATES = 'market.price-updates',
}
```

- [ ] **Step 7: Tạo `src/common/filters/ws-exception.filter.ts`**

```typescript
import { Catch, ArgumentsHost } from '@nestjs/common';
import { BaseWsExceptionFilter, WsException } from '@nestjs/websockets';

@Catch(WsException)
export class WsExceptionFilter extends BaseWsExceptionFilter {
  catch(exception: WsException, host: ArgumentsHost) {
    const client = host.switchToWs().getClient();
    client.emit('error', { message: exception.message });
  }
}
```

- [ ] **Step 8: Commit**

```bash
git add src/common/
git commit -m "feat: add common types (PriceData, PriceColorFlag), constants, ApiResponse wrapper, WsExceptionFilter"
```

---

### Task 3: Database Setup, Stock Entity & Seed Data

**Files:**
- Create: `src/config/database.config.ts`
- Create: `src/modules/stocks/entities/stock.entity.ts`
- Create: `src/modules/stocks/dto/create-stock.dto.ts`
- Create: `src/modules/stocks/dto/stock-response.dto.ts`
- Create: `src/database/seeds/stocks.seed.ts`
- Modify: `src/app.module.ts` (thêm TypeOrmModule)
- Modify: `package.json` (thêm script seed)

**Interfaces:**
- Produces:
  - `Stock` entity: `{ id, ticker, name, exchange(HOSE|HNX|UPCOM), sector, referencePrice, ceilingPrice, floorPrice, lotSize, isActive }`
  - `Exchange` enum
  - Seed 20 mã HOSE trong PostgreSQL

- [ ] **Step 1: Tạo `src/config/database.config.ts`**

```typescript
import { registerAs } from '@nestjs/config';
export default registerAs('database', () => ({
  host: process.env.DB_HOST ?? 'localhost',
  port: parseInt(process.env.DB_PORT ?? '5432', 10),
  name: process.env.DB_NAME ?? 'be_securities',
  user: process.env.DB_USER ?? 'securities_user',
  pass: process.env.DB_PASS ?? 'securities_pass',
}));
```

- [ ] **Step 2: Tạo `src/modules/stocks/entities/stock.entity.ts`**

```typescript
import { Column, Entity, PrimaryGeneratedColumn, Index } from 'typeorm';

export enum Exchange {
  HOSE = 'HOSE',
  HNX = 'HNX',
  UPCOM = 'UPCOM',
}

@Entity('stocks')
export class Stock {
  @PrimaryGeneratedColumn()
  id: number;

  @Index({ unique: true })
  @Column({ length: 10 })
  ticker: string;

  @Column({ length: 200 })
  name: string;

  @Column({ type: 'enum', enum: Exchange, default: Exchange.HOSE })
  exchange: Exchange;

  @Column({ length: 100, nullable: true })
  sector: string;

  @Column({ type: 'decimal', precision: 15, scale: 2, name: 'reference_price' })
  referencePrice: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, name: 'ceiling_price' })
  ceilingPrice: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, name: 'floor_price' })
  floorPrice: number;

  @Column({ default: 100, name: 'lot_size' })
  lotSize: number;

  @Column({ default: true, name: 'is_active' })
  isActive: boolean;
}
```

- [ ] **Step 3: Tạo `src/modules/stocks/dto/stock-response.dto.ts`**

```typescript
import { Exchange } from '../entities/stock.entity';

export class StockResponseDto {
  id: number;
  ticker: string;
  name: string;
  exchange: Exchange;
  sector: string;
  referencePrice: number;
  ceilingPrice: number;
  floorPrice: number;
  lotSize: number;
}
```

- [ ] **Step 4: Tạo `src/modules/stocks/dto/create-stock.dto.ts`**

```typescript
import { IsString, IsNumber, IsEnum, IsOptional, Min } from 'class-validator';
import { Exchange } from '../entities/stock.entity';

export class CreateStockDto {
  @IsString() ticker: string;
  @IsString() name: string;
  @IsEnum(Exchange) exchange: Exchange;
  @IsString() @IsOptional() sector?: string;
  @IsNumber() @Min(100) referencePrice: number;
  @IsNumber() @IsOptional() lotSize?: number;
}
```

- [ ] **Step 5: Cập nhật `src/app.module.ts` thêm TypeORM**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { EventEmitterModule } from '@nestjs/event-emitter';
import appConfig from './config/app.config';
import databaseConfig from './config/database.config';
import { Stock } from './modules/stocks/entities/stock.entity';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true, load: [appConfig, databaseConfig] }),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (cfg: ConfigService) => ({
        type: 'postgres',
        host: cfg.get('database.host'),
        port: cfg.get<number>('database.port'),
        database: cfg.get('database.name'),
        username: cfg.get('database.user'),
        password: cfg.get('database.pass'),
        entities: [Stock],
        synchronize: true,
      }),
    }),
    EventEmitterModule.forRoot(),
  ],
})
export class AppModule {}
```

- [ ] **Step 6: Tạo seed script `src/database/seeds/stocks.seed.ts`**

```typescript
import { DataSource } from 'typeorm';
import { Stock, Exchange } from '../../modules/stocks/entities/stock.entity';
import * as dotenv from 'dotenv';
dotenv.config();

const HOSE_STOCKS = [
  { ticker: 'VNM', name: 'Công ty CP Sữa Việt Nam', sector: 'Hàng tiêu dùng', referencePrice: 72000 },
  { ticker: 'VIC', name: 'Tập đoàn Vingroup', sector: 'Bất động sản', referencePrice: 45000 },
  { ticker: 'VHM', name: 'Công ty CP Vinhomes', sector: 'Bất động sản', referencePrice: 38000 },
  { ticker: 'HPG', name: 'Công ty CP Tập đoàn Hòa Phát', sector: 'Thép', referencePrice: 26000 },
  { ticker: 'MWG', name: 'Công ty CP Đầu tư Thế Giới Di Động', sector: 'Bán lẻ', referencePrice: 58000 },
  { ticker: 'SSI', name: 'Công ty CP Chứng khoán SSI', sector: 'Chứng khoán', referencePrice: 18000 },
  { ticker: 'VCB', name: 'Ngân hàng TMCP Ngoại thương Việt Nam', sector: 'Ngân hàng', referencePrice: 92000 },
  { ticker: 'BID', name: 'Ngân hàng TMCP Đầu tư và Phát triển VN', sector: 'Ngân hàng', referencePrice: 41000 },
  { ticker: 'CTG', name: 'Ngân hàng TMCP Công thương Việt Nam', sector: 'Ngân hàng', referencePrice: 32000 },
  { ticker: 'FPT', name: 'Công ty CP FPT', sector: 'Công nghệ', referencePrice: 115000 },
  { ticker: 'MSN', name: 'Công ty CP Tập đoàn Masan', sector: 'Hàng tiêu dùng', referencePrice: 65000 },
  { ticker: 'GVR', name: 'Tập đoàn Công nghiệp Cao su VN', sector: 'Nông nghiệp', referencePrice: 15000 },
  { ticker: 'ACB', name: 'Ngân hàng TMCP Á Châu', sector: 'Ngân hàng', referencePrice: 22000 },
  { ticker: 'MBB', name: 'Ngân hàng TMCP Quân đội', sector: 'Ngân hàng', referencePrice: 18000 },
  { ticker: 'TCB', name: 'Ngân hàng TMCP Kỹ Thương Việt Nam', sector: 'Ngân hàng', referencePrice: 24000 },
  { ticker: 'VPB', name: 'Ngân hàng TMCP Việt Nam Thịnh Vượng', sector: 'Ngân hàng', referencePrice: 17000 },
  { ticker: 'PLX', name: 'Tập đoàn Xăng dầu Việt Nam', sector: 'Năng lượng', referencePrice: 42000 },
  { ticker: 'GAS', name: 'Tổng Công ty Khí Việt Nam', sector: 'Năng lượng', referencePrice: 85000 },
  { ticker: 'POW', name: 'Tổng Công ty Điện lực Dầu khí VN', sector: 'Điện', referencePrice: 12000 },
  { ticker: 'PNJ', name: 'Công ty CP Vàng bạc Đá quý Phú Nhuận', sector: 'Bán lẻ', referencePrice: 82000 },
];

async function seed() {
  const dataSource = new DataSource({
    type: 'postgres',
    host: process.env.DB_HOST ?? 'localhost',
    port: parseInt(process.env.DB_PORT ?? '5432', 10),
    database: process.env.DB_NAME ?? 'be_securities',
    username: process.env.DB_USER ?? 'securities_user',
    password: process.env.DB_PASS ?? 'securities_pass',
    entities: [Stock],
    synchronize: true,
  });
  await dataSource.initialize();
  const repo = dataSource.getRepository(Stock);
  for (const s of HOSE_STOCKS) {
    const exists = await repo.findOneBy({ ticker: s.ticker });
    if (!exists) {
      const ceiling = Math.round(s.referencePrice * 1.07 / 100) * 100;
      const floor = Math.round(s.referencePrice * 0.93 / 100) * 100;
      await repo.save(repo.create({ ...s, exchange: Exchange.HOSE, ceilingPrice: ceiling, floorPrice: floor, lotSize: 100 }));
      console.log(`Seeded: ${s.ticker}`);
    }
  }
  await dataSource.destroy();
  console.log('Seed complete.');
}
seed().catch(console.error);
```

- [ ] **Step 7: Thêm script seed vào `package.json`**

```json
"seed": "ts-node -r tsconfig-paths/register src/database/seeds/stocks.seed.ts"
```

Cài thêm: `npm install -D tsconfig-paths ts-node`

- [ ] **Step 8: Chạy seed**

```bash
npm run seed
```

Expected: 20 dòng `Seeded: VNM`, ..., `Seed complete.`

- [ ] **Step 9: Commit**

```bash
git add src/modules/stocks/ src/database/ src/config/database.config.ts src/app.module.ts package.json
git commit -m "feat: add Stock entity (HOSE/HNX/UPCOM), TypeORM setup, seed 20 HOSE stocks"
```

---

### Task 4: Redis Price Cache Module

**Files:**
- Create: `src/config/redis.config.ts`
- Create: `src/modules/price-cache/price-cache.service.ts`
- Create: `src/modules/price-cache/price-cache.module.ts`
- Create: `src/modules/price-cache/price-cache.service.spec.ts`
- Modify: `src/app.module.ts` (thêm PriceCacheModule)

**Interfaces:**
- Produces:
  - `REDIS_CLIENT` injection token
  - `PriceCacheService.setPrice(ticker: string, data: PriceData): Promise<void>`
  - `PriceCacheService.getPrice(ticker: string): Promise<PriceData | null>`
  - `PriceCacheService.getAllPrices(): Promise<PriceData[]>`
  - `PriceCacheService.deletePrice(ticker: string): Promise<void>`
  - Redis key pattern: `price:{ticker}`, set tracking: `prices:tickers`

- [ ] **Step 1: Tạo `src/config/redis.config.ts`**

```typescript
import { registerAs } from '@nestjs/config';
export default registerAs('redis', () => ({
  host: process.env.REDIS_HOST ?? 'localhost',
  port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
  db: parseInt(process.env.REDIS_DB ?? '0', 10),
}));
```

- [ ] **Step 2: Viết test trước**

```typescript
// src/modules/price-cache/price-cache.service.spec.ts
import { Test } from '@nestjs/testing';
import { PriceCacheService } from './price-cache.service';
import { REDIS_CLIENT } from './price-cache.module';
import { PriceColorFlag, PriceData } from '../../common/types/price-data.type';

const mockRedis = {
  set: jest.fn(), get: jest.fn(), sadd: jest.fn(),
  smembers: jest.fn(), pipeline: jest.fn(), del: jest.fn(), srem: jest.fn(),
};

const samplePrice: PriceData = {
  ticker: 'VNM', currentPrice: 72000, matchedPrice: 72000,
  volume: 1000, totalVolume: 5000, change: 0, changePercent: 0,
  colorFlag: PriceColorFlag.REFERENCE, updatedAt: new Date().toISOString(),
};

describe('PriceCacheService', () => {
  let service: PriceCacheService;
  beforeEach(async () => {
    const module = await Test.createTestingModule({
      providers: [PriceCacheService, { provide: REDIS_CLIENT, useValue: mockRedis }],
    }).compile();
    service = module.get(PriceCacheService);
    jest.clearAllMocks();
  });

  it('setPrice() calls redis.set with key price:VNM and sadd', async () => {
    await service.setPrice('VNM', samplePrice);
    expect(mockRedis.set).toHaveBeenCalledWith('price:VNM', JSON.stringify(samplePrice));
    expect(mockRedis.sadd).toHaveBeenCalledWith('prices:tickers', 'VNM');
  });

  it('getPrice() returns parsed PriceData when key exists', async () => {
    mockRedis.get.mockResolvedValue(JSON.stringify(samplePrice));
    const result = await service.getPrice('VNM');
    expect(result).toEqual(samplePrice);
  });

  it('getPrice() returns null when key missing', async () => {
    mockRedis.get.mockResolvedValue(null);
    expect(await service.getPrice('UNKNOWN')).toBeNull();
  });

  it('deletePrice() calls del and srem', async () => {
    await service.deletePrice('VNM');
    expect(mockRedis.del).toHaveBeenCalledWith('price:VNM');
    expect(mockRedis.srem).toHaveBeenCalledWith('prices:tickers', 'VNM');
  });
});
```

- [ ] **Step 3: Chạy test — phải FAIL**

```bash
npm run test -- --testPathPattern=price-cache
```

- [ ] **Step 4: Tạo `src/modules/price-cache/price-cache.module.ts`**

```typescript
import { Module, Global } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import Redis from 'ioredis';
import { PriceCacheService } from './price-cache.service';
import redisConfig from '../../config/redis.config';

export const REDIS_CLIENT = 'REDIS_CLIENT';

@Global()
@Module({
  imports: [ConfigModule.forFeature(redisConfig)],
  providers: [
    {
      provide: REDIS_CLIENT,
      inject: [ConfigService],
      useFactory: (cfg: ConfigService) =>
        new Redis({ host: cfg.get('redis.host'), port: cfg.get<number>('redis.port'), db: cfg.get<number>('redis.db') }),
    },
    PriceCacheService,
  ],
  exports: [PriceCacheService],
})
export class PriceCacheModule {}
```

- [ ] **Step 5: Tạo `src/modules/price-cache/price-cache.service.ts`**

```typescript
import { Inject, Injectable } from '@nestjs/common';
import Redis from 'ioredis';
import { PriceData } from '../../common/types/price-data.type';
import { REDIS_CLIENT } from './price-cache.module';

const PRICE_KEY = (ticker: string) => `price:${ticker}`;
const ALL_TICKERS_SET = 'prices:tickers';

@Injectable()
export class PriceCacheService {
  constructor(@Inject(REDIS_CLIENT) private readonly redis: Redis) {}

  async setPrice(ticker: string, data: PriceData): Promise<void> {
    await this.redis.set(PRICE_KEY(ticker), JSON.stringify(data));
    await this.redis.sadd(ALL_TICKERS_SET, ticker);
  }

  async getPrice(ticker: string): Promise<PriceData | null> {
    const raw = await this.redis.get(PRICE_KEY(ticker));
    return raw ? (JSON.parse(raw) as PriceData) : null;
  }

  async getAllPrices(): Promise<PriceData[]> {
    const tickers = await this.redis.smembers(ALL_TICKERS_SET);
    if (!tickers.length) return [];
    const pipeline = this.redis.pipeline();
    tickers.forEach((t) => pipeline.get(PRICE_KEY(t)));
    const results = await pipeline.exec();
    return (results ?? [])
      .map(([, val]) => (val ? (JSON.parse(val as string) as PriceData) : null))
      .filter((p): p is PriceData => p !== null);
  }

  async deletePrice(ticker: string): Promise<void> {
    await this.redis.del(PRICE_KEY(ticker));
    await this.redis.srem(ALL_TICKERS_SET, ticker);
  }
}
```

- [ ] **Step 6: Chạy test — phải PASS**

```bash
npm run test -- --testPathPattern=price-cache
```

Expected: 4 tests PASS

- [ ] **Step 7: Thêm PriceCacheModule vào AppModule**

- [ ] **Step 8: Commit**

```bash
git add src/modules/price-cache/ src/config/redis.config.ts src/app.module.ts
git commit -m "feat: add Redis PriceCacheModule (setPrice/getPrice/getAllPrices/deletePrice)"
```

---

### Task 5: Market Data Kafka Consumer

**Files:**
- Create: `src/config/kafka.config.ts`
- Create: `src/modules/market-data/dto/price-update.dto.ts`
- Create: `src/modules/market-data/market-data.service.ts`
- Create: `src/modules/market-data/market-data.service.spec.ts`
- Create: `src/modules/market-data/market-data.consumer.ts`
- Create: `src/modules/market-data/market-data.module.ts`

**Interfaces:**
- Consumes: `PriceCacheService` (Task 4), `StocksRepository.findByTicker()` (Task 6 — nhưng cần interface trước)
- Produces:
  - `PriceUpdateDto`: `{ ticker, price, volume, totalVolume, matchedPrice, timestamp }`
  - `MarketDataService.processUpdate(dto): Promise<PriceData>` — tính colorFlag, lưu Redis, emit `price.updated`
  - `MarketDataService.resolveColorFlag(price, ceiling, floor, reference): PriceColorFlag`
  - Kafka consumer `@EventPattern('market.price-updates')`

**Lưu ý quan trọng:** MarketDataService phụ thuộc StocksRepository (Task 6). Để tránh circular dependency, truyền `StocksRepository` qua constructor nhưng tạo interface `IStocksRepository` với method `findByTicker(ticker: string): Promise<{ referencePrice: number; ceilingPrice: number; floorPrice: number } | null>` để MarketDataModule không import StocksModule trực tiếp — StocksModule sẽ export interface này ở Task 6.

- [ ] **Step 1: Tạo `src/config/kafka.config.ts`**

```typescript
import { registerAs } from '@nestjs/config';
export default registerAs('kafka', () => ({
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  groupId: process.env.KAFKA_GROUP_ID ?? 'be-securities-group',
}));
```

- [ ] **Step 2: Tạo `src/modules/market-data/dto/price-update.dto.ts`**

```typescript
import { IsString, IsNumber, IsISO8601, Min } from 'class-validator';

export class PriceUpdateDto {
  @IsString() ticker: string;
  @IsNumber() @Min(0) price: number;
  @IsNumber() @Min(0) volume: number;
  @IsNumber() @Min(0) totalVolume: number;
  @IsNumber() @Min(0) matchedPrice: number;
  @IsISO8601() timestamp: string;
}
```

- [ ] **Step 3: Viết test trước cho MarketDataService**

```typescript
// src/modules/market-data/market-data.service.spec.ts
import { MarketDataService } from './market-data.service';
import { PriceColorFlag } from '../../common/types/price-data.type';

const mockPriceCache = { setPrice: jest.fn() };
const mockStocksRepo = { findByTicker: jest.fn() };
const mockEventEmitter = { emit: jest.fn() };

describe('MarketDataService', () => {
  let service: MarketDataService;
  beforeEach(() => {
    service = new MarketDataService(mockPriceCache as any, mockStocksRepo as any, mockEventEmitter as any);
    jest.clearAllMocks();
  });

  describe('resolveColorFlag', () => {
    it('CEILING when price >= ceiling', () => {
      expect(service.resolveColorFlag(77000, 77000, 65000, 72000)).toBe(PriceColorFlag.CEILING);
    });
    it('FLOOR when price <= floor', () => {
      expect(service.resolveColorFlag(65000, 77000, 65000, 72000)).toBe(PriceColorFlag.FLOOR);
    });
    it('INCREASE when price > reference', () => {
      expect(service.resolveColorFlag(74000, 77000, 65000, 72000)).toBe(PriceColorFlag.INCREASE);
    });
    it('DECREASE when price < reference', () => {
      expect(service.resolveColorFlag(70000, 77000, 65000, 72000)).toBe(PriceColorFlag.DECREASE);
    });
    it('REFERENCE when price == reference', () => {
      expect(service.resolveColorFlag(72000, 77000, 65000, 72000)).toBe(PriceColorFlag.REFERENCE);
    });
  });

  it('processUpdate() caches price and emits price.updated', async () => {
    mockStocksRepo.findByTicker.mockResolvedValue({ referencePrice: 72000, ceilingPrice: 77000, floorPrice: 65000 });
    const dto = { ticker: 'VNM', price: 74000, volume: 500, totalVolume: 2000, matchedPrice: 74000, timestamp: new Date().toISOString() };
    const result = await service.processUpdate(dto);
    expect(result.colorFlag).toBe(PriceColorFlag.INCREASE);
    expect(result.change).toBe(2000);
    expect(mockPriceCache.setPrice).toHaveBeenCalledWith('VNM', result);
    expect(mockEventEmitter.emit).toHaveBeenCalledWith('price.updated', result);
  });
});
```

- [ ] **Step 4: Chạy test — phải FAIL**

```bash
npm run test -- --testPathPattern=market-data.service
```

- [ ] **Step 5: Tạo `src/modules/market-data/market-data.service.ts`**

```typescript
import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { PriceCacheService } from '../price-cache/price-cache.service';
import { PriceColorFlag, PriceData } from '../../common/types/price-data.type';
import { PriceUpdateDto } from './dto/price-update.dto';

interface StockPriceRef {
  referencePrice: number;
  ceilingPrice: number;
  floorPrice: number;
}

interface IStocksLookup {
  findByTicker(ticker: string): Promise<StockPriceRef | null>;
}

@Injectable()
export class MarketDataService {
  constructor(
    private readonly priceCache: PriceCacheService,
    private readonly stocksLookup: IStocksLookup,
    private readonly eventEmitter: EventEmitter2,
  ) {}

  async processUpdate(dto: PriceUpdateDto): Promise<PriceData> {
    const stock = await this.stocksLookup.findByTicker(dto.ticker);
    const refPrice = stock ? Number(stock.referencePrice) : dto.price;
    const ceilingPrice = stock ? Number(stock.ceilingPrice) : Math.round(refPrice * 1.07 / 100) * 100;
    const floorPrice = stock ? Number(stock.floorPrice) : Math.round(refPrice * 0.93 / 100) * 100;
    const change = dto.price - refPrice;
    const changePercent = refPrice > 0 ? (change / refPrice) * 100 : 0;
    const colorFlag = this.resolveColorFlag(dto.price, ceilingPrice, floorPrice, refPrice);
    const priceData: PriceData = {
      ticker: dto.ticker, currentPrice: dto.price, matchedPrice: dto.matchedPrice,
      volume: dto.volume, totalVolume: dto.totalVolume,
      change: Math.round(change), changePercent: Math.round(changePercent * 100) / 100,
      colorFlag, updatedAt: dto.timestamp,
    };
    await this.priceCache.setPrice(dto.ticker, priceData);
    this.eventEmitter.emit('price.updated', priceData);
    return priceData;
  }

  resolveColorFlag(price: number, ceiling: number, floor: number, reference: number): PriceColorFlag {
    if (price >= ceiling) return PriceColorFlag.CEILING;
    if (price <= floor) return PriceColorFlag.FLOOR;
    if (price > reference) return PriceColorFlag.INCREASE;
    if (price < reference) return PriceColorFlag.DECREASE;
    return PriceColorFlag.REFERENCE;
  }
}
```

- [ ] **Step 6: Chạy test — phải PASS**

```bash
npm run test -- --testPathPattern=market-data.service
```

Expected: 6 tests PASS

- [ ] **Step 7: Tạo `src/modules/market-data/market-data.consumer.ts`**

```typescript
import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { KafkaTopics } from '../../common/constants/kafka-topics.constant';
import { PriceUpdateDto } from './dto/price-update.dto';
import { MarketDataService } from './market-data.service';

@Controller()
export class MarketDataConsumer {
  constructor(private readonly marketDataService: MarketDataService) {}

  @EventPattern(KafkaTopics.PRICE_UPDATES)
  async handlePriceUpdate(@Payload() message: unknown): Promise<void> {
    const dto = plainToInstance(PriceUpdateDto, message);
    const errors = await validate(dto);
    if (errors.length > 0) return;
    await this.marketDataService.processUpdate(dto);
  }
}
```

- [ ] **Step 8: Tạo `src/modules/market-data/market-data.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import kafkaConfig from '../../config/kafka.config';
import { MarketDataConsumer } from './market-data.consumer';
import { MarketDataService } from './market-data.service';

@Module({
  imports: [ConfigModule.forFeature(kafkaConfig)],
  controllers: [MarketDataConsumer],
  providers: [MarketDataService],
  exports: [MarketDataService],
})
export class MarketDataModule {}
```

**Lưu ý:** MarketDataModule sẽ được hoàn chỉnh inject StocksRepository sau khi Task 6 xong — ở bước này MarketDataService nhận `IStocksLookup` qua DI token, sẽ được provide bởi StocksModule.

- [ ] **Step 9: Commit**

```bash
git add src/modules/market-data/ src/config/kafka.config.ts
git commit -m "feat: add Kafka consumer MarketDataModule with colorFlag logic and price.updated event"
```

---

### Task 6: Stocks Module (REST API) & Wire MarketData

**Files:**
- Create: `src/modules/stocks/stocks.repository.ts`
- Create: `src/modules/stocks/stocks.service.ts`
- Create: `src/modules/stocks/stocks.service.spec.ts`
- Create: `src/modules/stocks/stocks.controller.ts`
- Create: `src/modules/stocks/stocks.module.ts`
- Modify: `src/modules/market-data/market-data.module.ts` (inject StocksRepository)
- Modify: `src/app.module.ts` (thêm StocksModule, MarketDataModule)

**Interfaces:**
- Produces:
  - `StocksRepository.findAll(): Promise<Stock[]>`
  - `StocksRepository.findByTicker(ticker: string): Promise<Stock | null>`
  - `StocksService.findAll(): Promise<StockResponseDto[]>`
  - `StocksService.findByTicker(ticker: string): Promise<StockResponseDto>`
  - `GET /api/stocks` → `ApiResponse<StockResponseDto[]>`
  - `GET /api/stocks/:ticker` → `ApiResponse<StockResponseDto>`

- [ ] **Step 1: Tạo `src/modules/stocks/stocks.repository.ts`**

```typescript
import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Stock } from './entities/stock.entity';

@Injectable()
export class StocksRepository {
  constructor(@InjectRepository(Stock) private readonly repo: Repository<Stock>) {}

  findAll(): Promise<Stock[]> {
    return this.repo.find({ where: { isActive: true }, order: { ticker: 'ASC' } });
  }

  findByTicker(ticker: string): Promise<Stock | null> {
    return this.repo.findOneBy({ ticker: ticker.toUpperCase(), isActive: true });
  }
}
```

- [ ] **Step 2: Viết test trước cho StocksService**

```typescript
// src/modules/stocks/stocks.service.spec.ts
import { NotFoundException } from '@nestjs/common';
import { StocksService } from './stocks.service';
import { Exchange } from './entities/stock.entity';

const mockStock = {
  id: 1, ticker: 'VNM', name: 'Sữa Việt Nam', exchange: Exchange.HOSE,
  sector: 'Hàng tiêu dùng', referencePrice: 72000, ceilingPrice: 77000,
  floorPrice: 65000, lotSize: 100, isActive: true,
};
const mockRepo = { findAll: jest.fn(), findByTicker: jest.fn() };

describe('StocksService', () => {
  let service: StocksService;
  beforeEach(() => {
    service = new StocksService(mockRepo as any);
    jest.clearAllMocks();
  });

  it('findAll() maps stocks to DTOs with numeric prices', async () => {
    mockRepo.findAll.mockResolvedValue([mockStock]);
    const result = await service.findAll();
    expect(result).toHaveLength(1);
    expect(result[0].ticker).toBe('VNM');
    expect(typeof result[0].referencePrice).toBe('number');
  });

  it('findByTicker() returns DTO when found', async () => {
    mockRepo.findByTicker.mockResolvedValue(mockStock);
    const result = await service.findByTicker('VNM');
    expect(result.ticker).toBe('VNM');
  });

  it('findByTicker() throws NotFoundException when not found', async () => {
    mockRepo.findByTicker.mockResolvedValue(null);
    await expect(service.findByTicker('XXX')).rejects.toThrow(NotFoundException);
  });
});
```

- [ ] **Step 3: Chạy test — phải FAIL**

```bash
npm run test -- --testPathPattern=stocks.service
```

- [ ] **Step 4: Tạo `src/modules/stocks/stocks.service.ts`**

```typescript
import { Injectable, NotFoundException } from '@nestjs/common';
import { StocksRepository } from './stocks.repository';
import { StockResponseDto } from './dto/stock-response.dto';
import { Stock } from './entities/stock.entity';

@Injectable()
export class StocksService {
  constructor(private readonly repo: StocksRepository) {}

  async findAll(): Promise<StockResponseDto[]> {
    const stocks = await this.repo.findAll();
    return stocks.map(this.toDto);
  }

  async findByTicker(ticker: string): Promise<StockResponseDto> {
    const stock = await this.repo.findByTicker(ticker);
    if (!stock) throw new NotFoundException(`Không tìm thấy mã ${ticker}`);
    return this.toDto(stock);
  }

  private toDto(stock: Stock): StockResponseDto {
    return {
      id: stock.id,
      ticker: stock.ticker,
      name: stock.name,
      exchange: stock.exchange,
      sector: stock.sector,
      referencePrice: Number(stock.referencePrice),
      ceilingPrice: Number(stock.ceilingPrice),
      floorPrice: Number(stock.floorPrice),
      lotSize: stock.lotSize,
    };
  }
}
```

- [ ] **Step 5: Chạy test — phải PASS**

```bash
npm run test -- --testPathPattern=stocks.service
```

Expected: 3 tests PASS

- [ ] **Step 6: Tạo `src/modules/stocks/stocks.controller.ts`**

```typescript
import { Controller, Get, Param } from '@nestjs/common';
import { StocksService } from './stocks.service';
import { ApiResponse } from '../../common/dto/api-response.dto';

@Controller('stocks')
export class StocksController {
  constructor(private readonly stocksService: StocksService) {}

  @Get()
  async findAll(): Promise<ApiResponse<unknown>> {
    return ApiResponse.ok(await this.stocksService.findAll());
  }

  @Get(':ticker')
  async findOne(@Param('ticker') ticker: string): Promise<ApiResponse<unknown>> {
    return ApiResponse.ok(await this.stocksService.findByTicker(ticker.toUpperCase()));
  }
}
```

- [ ] **Step 7: Tạo `src/modules/stocks/stocks.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Stock } from './entities/stock.entity';
import { StocksRepository } from './stocks.repository';
import { StocksService } from './stocks.service';
import { StocksController } from './stocks.controller';

@Module({
  imports: [TypeOrmModule.forFeature([Stock])],
  providers: [StocksRepository, StocksService],
  controllers: [StocksController],
  exports: [StocksRepository, StocksService],
})
export class StocksModule {}
```

- [ ] **Step 8: Cập nhật `src/modules/market-data/market-data.module.ts` — inject StocksRepository**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import kafkaConfig from '../../config/kafka.config';
import { MarketDataConsumer } from './market-data.consumer';
import { MarketDataService } from './market-data.service';
import { StocksModule } from '../stocks/stocks.module';
import { StocksRepository } from '../stocks/stocks.repository';

@Module({
  imports: [ConfigModule.forFeature(kafkaConfig), StocksModule],
  controllers: [MarketDataConsumer],
  providers: [
    {
      provide: MarketDataService,
      inject: [PriceCacheService, StocksRepository, EventEmitter2],
      useFactory: (cache, repo, emitter) => new MarketDataService(cache, repo, emitter),
    },
  ],
  exports: [MarketDataService],
})
export class MarketDataModule {}
```

*(Import PriceCacheService từ PriceCacheModule — vì PriceCacheModule là @Global() nên không cần import)*

- [ ] **Step 9: Thêm StocksModule, MarketDataModule vào AppModule**

- [ ] **Step 10: Test manual**

```bash
npm run start:dev
curl http://localhost:3000/api/stocks
```

Expected: `{ "success": true, "data": [...20 stocks...] }`

- [ ] **Step 11: Commit**

```bash
git add src/modules/stocks/ src/modules/market-data/market-data.module.ts src/app.module.ts
git commit -m "feat: add StocksModule REST API and wire MarketDataModule with StocksRepository"
```

---

### Task 7: Price Board REST API

**Files:**
- Create: `src/modules/price-board/dto/price-board-item.dto.ts`
- Create: `src/modules/price-board/price-board.service.ts`
- Create: `src/modules/price-board/price-board.service.spec.ts`
- Create: `src/modules/price-board/price-board.controller.ts`
- Create: `src/modules/price-board/price-board.module.ts`
- Modify: `src/app.module.ts` (thêm PriceBoardModule)

**Interfaces:**
- Consumes: `StocksService.findAll()`, `StocksService.findByTicker()`, `PriceCacheService.getPrice()`
- Produces:
  - `PriceBoardItemDto`: merge stock metadata + PriceData (default NO_CHANGE khi chưa có giá)
  - `GET /api/price-board` → `ApiResponse<PriceBoardItemDto[]>`
  - `GET /api/price-board/:ticker` → `ApiResponse<PriceBoardItemDto>`

- [ ] **Step 1: Tạo `src/modules/price-board/dto/price-board-item.dto.ts`**

```typescript
import { Exchange } from '../../stocks/entities/stock.entity';
import { PriceColorFlag } from '../../../common/types/price-data.type';

export class PriceBoardItemDto {
  ticker: string;
  name: string;
  exchange: Exchange;
  sector: string;
  referencePrice: number;
  ceilingPrice: number;
  floorPrice: number;
  currentPrice: number | null;
  matchedPrice: number | null;
  volume: number;
  totalVolume: number;
  change: number;
  changePercent: number;
  colorFlag: PriceColorFlag;
  updatedAt: string | null;
}
```

- [ ] **Step 2: Viết test trước**

```typescript
// src/modules/price-board/price-board.service.spec.ts
import { PriceBoardService } from './price-board.service';
import { PriceColorFlag } from '../../common/types/price-data.type';
import { Exchange } from '../stocks/entities/stock.entity';

const mockStock = {
  id: 1, ticker: 'VNM', name: 'Sữa Việt Nam', exchange: Exchange.HOSE,
  sector: 'Hàng tiêu dùng', referencePrice: 72000, ceilingPrice: 77000, floorPrice: 65000, lotSize: 100,
};
const mockPrice = {
  ticker: 'VNM', currentPrice: 74000, matchedPrice: 74000, volume: 500,
  totalVolume: 2000, change: 2000, changePercent: 2.78,
  colorFlag: PriceColorFlag.INCREASE, updatedAt: new Date().toISOString(),
};
const mockStocksService = { findAll: jest.fn(), findByTicker: jest.fn() };
const mockPriceCache = { getPrice: jest.fn() };

describe('PriceBoardService', () => {
  let service: PriceBoardService;
  beforeEach(() => {
    service = new PriceBoardService(mockStocksService as any, mockPriceCache as any);
    jest.clearAllMocks();
  });

  it('getBoard() merges stock with live price from Redis', async () => {
    mockStocksService.findAll.mockResolvedValue([mockStock]);
    mockPriceCache.getPrice.mockResolvedValue(mockPrice);
    const board = await service.getBoard();
    expect(board).toHaveLength(1);
    expect(board[0].currentPrice).toBe(74000);
    expect(board[0].colorFlag).toBe(PriceColorFlag.INCREASE);
  });

  it('getBoard() returns NO_CHANGE defaults when no price in Redis', async () => {
    mockStocksService.findAll.mockResolvedValue([mockStock]);
    mockPriceCache.getPrice.mockResolvedValue(null);
    const board = await service.getBoard();
    expect(board[0].currentPrice).toBeNull();
    expect(board[0].colorFlag).toBe(PriceColorFlag.NO_CHANGE);
    expect(board[0].volume).toBe(0);
  });
});
```

- [ ] **Step 3: Chạy test — phải FAIL**

```bash
npm run test -- --testPathPattern=price-board.service
```

- [ ] **Step 4: Tạo `src/modules/price-board/price-board.service.ts`**

```typescript
import { Injectable } from '@nestjs/common';
import { StocksService } from '../stocks/stocks.service';
import { PriceCacheService } from '../price-cache/price-cache.service';
import { PriceBoardItemDto } from './dto/price-board-item.dto';
import { PriceColorFlag } from '../../common/types/price-data.type';
import { StockResponseDto } from '../stocks/dto/stock-response.dto';

@Injectable()
export class PriceBoardService {
  constructor(
    private readonly stocksService: StocksService,
    private readonly priceCache: PriceCacheService,
  ) {}

  async getBoard(): Promise<PriceBoardItemDto[]> {
    const stocks = await this.stocksService.findAll();
    return Promise.all(stocks.map((s) => this.mergeWithPrice(s)));
  }

  async getBoardItem(ticker: string): Promise<PriceBoardItemDto> {
    const stock = await this.stocksService.findByTicker(ticker);
    return this.mergeWithPrice(stock);
  }

  private async mergeWithPrice(stock: StockResponseDto): Promise<PriceBoardItemDto> {
    const price = await this.priceCache.getPrice(stock.ticker);
    return {
      ticker: stock.ticker, name: stock.name, exchange: stock.exchange, sector: stock.sector,
      referencePrice: stock.referencePrice, ceilingPrice: stock.ceilingPrice, floorPrice: stock.floorPrice,
      currentPrice: price?.currentPrice ?? null,
      matchedPrice: price?.matchedPrice ?? null,
      volume: price?.volume ?? 0,
      totalVolume: price?.totalVolume ?? 0,
      change: price?.change ?? 0,
      changePercent: price?.changePercent ?? 0,
      colorFlag: price?.colorFlag ?? PriceColorFlag.NO_CHANGE,
      updatedAt: price?.updatedAt ?? null,
    };
  }
}
```

- [ ] **Step 5: Chạy test — phải PASS**

```bash
npm run test -- --testPathPattern=price-board.service
```

Expected: 2 tests PASS

- [ ] **Step 6: Tạo controller và module**

```typescript
// src/modules/price-board/price-board.controller.ts
import { Controller, Get, Param } from '@nestjs/common';
import { PriceBoardService } from './price-board.service';
import { ApiResponse } from '../../common/dto/api-response.dto';

@Controller('price-board')
export class PriceBoardController {
  constructor(private readonly service: PriceBoardService) {}

  @Get()
  async getBoard(): Promise<ApiResponse<unknown>> {
    return ApiResponse.ok(await this.service.getBoard());
  }

  @Get(':ticker')
  async getBoardItem(@Param('ticker') ticker: string): Promise<ApiResponse<unknown>> {
    return ApiResponse.ok(await this.service.getBoardItem(ticker.toUpperCase()));
  }
}
```

```typescript
// src/modules/price-board/price-board.module.ts
import { Module } from '@nestjs/common';
import { PriceBoardService } from './price-board.service';
import { PriceBoardController } from './price-board.controller';
import { StocksModule } from '../stocks/stocks.module';

@Module({
  imports: [StocksModule],
  providers: [PriceBoardService],
  controllers: [PriceBoardController],
  exports: [PriceBoardService],
})
export class PriceBoardModule {}
```

- [ ] **Step 7: Test manual**

```bash
curl http://localhost:3000/api/price-board
```

Expected: 20 items, `currentPrice: null`, `colorFlag: "NO_CHANGE"`

- [ ] **Step 8: Commit**

```bash
git add src/modules/price-board/ src/app.module.ts
git commit -m "feat: add PriceBoardModule REST API merging stock metadata with Redis prices"
```

---

### Task 8: WebSocket Gateway

**Files:**
- Create: `src/modules/gateway/price-board.gateway.ts`
- Create: `src/modules/gateway/gateway.module.ts`
- Modify: `src/app.module.ts` (thêm GatewayModule)

**Interfaces:**
- Consumes: EventEmitter2 event `price.updated` với payload `PriceData`
- Produces:
  - Client gửi `subscribeBoard` → join room `board`, nhận `{ event: 'subscribed', data: { room: 'board' } }`
  - Client gửi `subscribeTicker` với `{ ticker: 'VNM' }` → join room `ticker:VNM`
  - Server broadcast `priceUpdate` với payload `PriceData` đến room `board` và `ticker:{ticker}`

- [ ] **Step 1: Tạo `src/modules/gateway/price-board.gateway.ts`**

```typescript
import {
  WebSocketGateway, WebSocketServer, SubscribeMessage,
  MessageBody, ConnectedSocket, OnGatewayConnection, OnGatewayDisconnect,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { OnEvent } from '@nestjs/event-emitter';
import { UseFilters } from '@nestjs/common';
import { PriceData } from '../../common/types/price-data.type';
import { WsExceptionFilter } from '../../common/filters/ws-exception.filter';

@UseFilters(new WsExceptionFilter())
@WebSocketGateway({ cors: { origin: '*' }, namespace: '/' })
export class PriceBoardGateway implements OnGatewayConnection, OnGatewayDisconnect {
  @WebSocketServer() private server: Server;

  handleConnection(client: Socket) { console.log(`WS connected: ${client.id}`); }
  handleDisconnect(client: Socket) { console.log(`WS disconnected: ${client.id}`); }

  @SubscribeMessage('subscribeBoard')
  async handleSubscribeBoard(@ConnectedSocket() client: Socket): Promise<void> {
    await client.join('board');
    client.emit('subscribed', { room: 'board' });
  }

  @SubscribeMessage('subscribeTicker')
  async handleSubscribeTicker(
    @MessageBody() payload: { ticker: string },
    @ConnectedSocket() client: Socket,
  ): Promise<void> {
    const room = `ticker:${payload.ticker.toUpperCase()}`;
    await client.join(room);
    client.emit('subscribed', { room });
  }

  @OnEvent('price.updated')
  broadcastPriceUpdate(priceData: PriceData): void {
    this.server.to('board').emit('priceUpdate', priceData);
    this.server.to(`ticker:${priceData.ticker}`).emit('priceUpdate', priceData);
  }
}
```

- [ ] **Step 2: Tạo `src/modules/gateway/gateway.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { PriceBoardGateway } from './price-board.gateway';

@Module({ providers: [PriceBoardGateway] })
export class GatewayModule {}
```

- [ ] **Step 3: Thêm GatewayModule vào AppModule**

- [ ] **Step 4: Test WebSocket**

```bash
npm run start:dev
npx wscat -c ws://localhost:3000
```

Gửi: `{"event":"subscribeBoard","data":{}}`
Expected: `{"event":"subscribed","data":{"room":"board"}}`

- [ ] **Step 5: Commit**

```bash
git add src/modules/gateway/ src/app.module.ts
git commit -m "feat: add WebSocket gateway with subscribeBoard/subscribeTicker and price.updated broadcast"
```

---

### Task 9: Market Data Simulator Script

**Files:**
- Create: `scripts/market-simulator.ts`
- Modify: `package.json` (thêm script simulate)

**Interfaces:**
- Produces: Kafka producer publish random price updates cho 10 mã HOSE mỗi 500ms đến `market.price-updates`
- Mỗi message: `{ ticker, price, matchedPrice, volume, totalVolume, timestamp }`
- Giá biến động ngẫu nhiên ±1-5 bước (100 VND/bước), không vượt trần/sàn

- [ ] **Step 1: Tạo `scripts/market-simulator.ts`**

```typescript
import { Kafka } from 'kafkajs';
import * as dotenv from 'dotenv';
dotenv.config();

const kafka = new Kafka({
  clientId: 'market-simulator',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
});

const producer = kafka.producer();

const TICKERS = [
  { ticker: 'VNM', refPrice: 72000, ceiling: 77000, floor: 65000 },
  { ticker: 'VIC', refPrice: 45000, ceiling: 48100, floor: 41800 },
  { ticker: 'VHM', refPrice: 38000, ceiling: 40600, floor: 35300 },
  { ticker: 'HPG', refPrice: 26000, ceiling: 27800, floor: 24100 },
  { ticker: 'MWG', refPrice: 58000, ceiling: 62000, floor: 53900 },
  { ticker: 'SSI', refPrice: 18000, ceiling: 19200, floor: 16700 },
  { ticker: 'VCB', refPrice: 92000, ceiling: 98400, floor: 85500 },
  { ticker: 'BID', refPrice: 41000, ceiling: 43800, floor: 38100 },
  { ticker: 'FPT', refPrice: 115000, ceiling: 123000, floor: 106900 },
  { ticker: 'MBB', refPrice: 18000, ceiling: 19200, floor: 16700 },
];

const currentPriceMap: Record<string, number> = {};
const totalVolumeMap: Record<string, number> = {};
TICKERS.forEach(({ ticker, refPrice }) => {
  currentPriceMap[ticker] = refPrice;
  totalVolumeMap[ticker] = 0;
});

function randomStep(current: number, floor: number, ceiling: number): number {
  const steps = (Math.floor(Math.random() * 5) + 1) * 100;
  const direction = Math.random() > 0.5 ? 1 : -1;
  return Math.min(ceiling, Math.max(floor, current + direction * steps));
}

async function run() {
  await producer.connect();
  console.log('Market simulator started — publishing to market.price-updates every 500ms');

  const interval = setInterval(async () => {
    const { ticker, floor, ceiling } = TICKERS[Math.floor(Math.random() * TICKERS.length)];
    const newPrice = randomStep(currentPriceMap[ticker], floor, ceiling);
    currentPriceMap[ticker] = newPrice;
    const volume = Math.floor(Math.random() * 1000) + 100;
    totalVolumeMap[ticker] += volume;

    const message = {
      ticker, price: newPrice, matchedPrice: newPrice,
      volume, totalVolume: totalVolumeMap[ticker],
      timestamp: new Date().toISOString(),
    };

    await producer.send({
      topic: 'market.price-updates',
      messages: [{ key: ticker, value: JSON.stringify(message) }],
    });
    console.log(`[${new Date().toLocaleTimeString()}] ${ticker} price=${newPrice} vol=${volume}`);
  }, 500);

  process.on('SIGINT', async () => {
    clearInterval(interval);
    await producer.disconnect();
    console.log('Simulator stopped.');
    process.exit(0);
  });
}

run().catch(console.error);
```

- [ ] **Step 2: Thêm script vào `package.json`**

```json
"simulate": "ts-node -r tsconfig-paths/register scripts/market-simulator.ts"
```

- [ ] **Step 3: End-to-end test**

Terminal 1: `npm run start:dev`
Terminal 2: `npm run simulate`
Terminal 3: `npx wscat -c ws://localhost:3000` → gửi `{"event":"subscribeBoard","data":{}}`

Expected: nhận `priceUpdate` events mỗi ~500ms với giá biến động

Terminal 4: `curl http://localhost:3000/api/price-board` — kiểm tra `currentPrice` != null, colorFlag thay đổi

- [ ] **Step 4: Commit**

```bash
git add scripts/ package.json
git commit -m "feat: add market data simulator script publishing random price updates to Kafka"
```

---

### Task 10: Health Check & Final Integration

**Files:**
- Create: `src/modules/health/health.controller.ts`
- Create: `src/modules/health/health.module.ts`
- Modify: `src/app.module.ts` (thêm HealthModule)

**Interfaces:**
- Produces: `GET /api/health` → `{ status: 'ok', info: { database: { status: 'up' } } }`

- [ ] **Step 1: Tạo health module**

```typescript
// src/modules/health/health.controller.ts
import { Controller, Get } from '@nestjs/common';
import { HealthCheck, HealthCheckService, TypeOrmHealthIndicator } from '@nestjs/terminus';

@Controller('health')
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly db: TypeOrmHealthIndicator,
  ) {}

  @Get()
  @HealthCheck()
  check() {
    return this.health.check([() => this.db.pingCheck('database')]);
  }
}
```

```typescript
// src/modules/health/health.module.ts
import { Module } from '@nestjs/common';
import { TerminusModule } from '@nestjs/terminus';
import { HealthController } from './health.controller';

@Module({ imports: [TerminusModule], controllers: [HealthController] })
export class HealthModule {}
```

- [ ] **Step 2: Thêm HealthModule vào AppModule**

- [ ] **Step 3: Chạy toàn bộ test suite**

```bash
npm run test
```

Expected: tất cả tests PASS, không có lỗi

- [ ] **Step 4: Kiểm tra health endpoint**

```bash
curl http://localhost:3000/api/health
```

Expected: `{ "status": "ok", "info": { "database": { "status": "up" } } }`

- [ ] **Step 5: Final commit**

```bash
git add src/modules/health/ src/app.module.ts
git commit -m "feat: add health check endpoint, complete Phase 1 BESecurities price board"
```

---

## Tóm Tắt Luồng Dữ Liệu

```
Kafka (market.price-updates)
  → MarketDataConsumer (@EventPattern)
  → MarketDataService.processUpdate() [tính colorFlag]
  → PriceCacheService.setPrice() [lưu Redis: price:{ticker}]
  → EventEmitter2.emit('price.updated')
  → PriceBoardGateway.broadcastPriceUpdate()
  → socket.io room 'board' + room 'ticker:VNM'
  → WS clients nhận event 'priceUpdate'

REST /api/price-board
  → PriceBoardService.getBoard()
  → StocksService.findAll() [PostgreSQL]
  → PriceCacheService.getPrice() * N [Redis pipeline]
  → merge → PriceBoardItemDto[]
```

*Kế hoạch được tạo: 2026-06-29*
