import { Column, Entity, PrimaryGeneratedColumn, Index } from 'typeorm';

export enum Exchange {
  HOSE = 'HOSE',
  HNX = 'HNX',
  UPCOM = 'UPCOM',
}

@Entity('stocks')
export class Stock {
  @PrimaryGeneratedColumn()
  id!: number;

  @Index({ unique: true })
  @Column({ length: 10 })
  ticker!: string;

  @Column({ length: 200 })
  name!: string;

  @Column({ type: 'enum', enum: Exchange, default: Exchange.HOSE })
  exchange!: Exchange;

  @Column({ length: 100, nullable: true })
  sector!: string;

  @Column({ type: 'decimal', precision: 15, scale: 2, name: 'reference_price' })
  referencePrice!: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, name: 'ceiling_price' })
  ceilingPrice!: number;

  @Column({ type: 'decimal', precision: 15, scale: 2, name: 'floor_price' })
  floorPrice!: number;

  @Column({ default: 100, name: 'lot_size' })
  lotSize!: number;

  @Column({ default: true, name: 'is_active' })
  isActive!: boolean;
}
