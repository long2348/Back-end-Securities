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
