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
