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
