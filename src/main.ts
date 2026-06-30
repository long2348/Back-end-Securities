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
