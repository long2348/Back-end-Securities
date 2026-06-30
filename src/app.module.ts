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
