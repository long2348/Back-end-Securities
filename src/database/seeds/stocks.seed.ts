import { DataSource } from 'typeorm';
import { Stock, Exchange } from '../../modules/stocks/entities/stock.entity';
import * as dotenv from 'dotenv';
dotenv.config();

const HOSE_STOCKS = [
  {
    ticker: 'VNM',
    name: 'Công ty CP Sữa Việt Nam',
    sector: 'Hàng tiêu dùng',
    referencePrice: 72000,
  },
  {
    ticker: 'VIC',
    name: 'Tập đoàn Vingroup',
    sector: 'Bất động sản',
    referencePrice: 45000,
  },
  {
    ticker: 'VHM',
    name: 'Công ty CP Vinhomes',
    sector: 'Bất động sản',
    referencePrice: 38000,
  },
  {
    ticker: 'HPG',
    name: 'Công ty CP Tập đoàn Hòa Phát',
    sector: 'Thép',
    referencePrice: 26000,
  },
  {
    ticker: 'MWG',
    name: 'Công ty CP Đầu tư Thế Giới Di Động',
    sector: 'Bán lẻ',
    referencePrice: 58000,
  },
  {
    ticker: 'SSI',
    name: 'Công ty CP Chứng khoán SSI',
    sector: 'Chứng khoán',
    referencePrice: 18000,
  },
  {
    ticker: 'VCB',
    name: 'Ngân hàng TMCP Ngoại thương Việt Nam',
    sector: 'Ngân hàng',
    referencePrice: 92000,
  },
  {
    ticker: 'BID',
    name: 'Ngân hàng TMCP Đầu tư và Phát triển VN',
    sector: 'Ngân hàng',
    referencePrice: 41000,
  },
  {
    ticker: 'CTG',
    name: 'Ngân hàng TMCP Công thương Việt Nam',
    sector: 'Ngân hàng',
    referencePrice: 32000,
  },
  {
    ticker: 'FPT',
    name: 'Công ty CP FPT',
    sector: 'Công nghệ',
    referencePrice: 115000,
  },
  {
    ticker: 'MSN',
    name: 'Công ty CP Tập đoàn Masan',
    sector: 'Hàng tiêu dùng',
    referencePrice: 65000,
  },
  {
    ticker: 'GVR',
    name: 'Tập đoàn Công nghiệp Cao su VN',
    sector: 'Nông nghiệp',
    referencePrice: 15000,
  },
  {
    ticker: 'ACB',
    name: 'Ngân hàng TMCP Á Châu',
    sector: 'Ngân hàng',
    referencePrice: 22000,
  },
  {
    ticker: 'MBB',
    name: 'Ngân hàng TMCP Quân đội',
    sector: 'Ngân hàng',
    referencePrice: 18000,
  },
  {
    ticker: 'TCB',
    name: 'Ngân hàng TMCP Kỹ Thương Việt Nam',
    sector: 'Ngân hàng',
    referencePrice: 24000,
  },
  {
    ticker: 'VPB',
    name: 'Ngân hàng TMCP Việt Nam Thịnh Vượng',
    sector: 'Ngân hàng',
    referencePrice: 17000,
  },
  {
    ticker: 'PLX',
    name: 'Tập đoàn Xăng dầu Việt Nam',
    sector: 'Năng lượng',
    referencePrice: 42000,
  },
  {
    ticker: 'GAS',
    name: 'Tổng Công ty Khí Việt Nam',
    sector: 'Năng lượng',
    referencePrice: 85000,
  },
  {
    ticker: 'POW',
    name: 'Tổng Công ty Điện lực Dầu khí VN',
    sector: 'Điện',
    referencePrice: 12000,
  },
  {
    ticker: 'PNJ',
    name: 'Công ty CP Vàng bạc Đá quý Phú Nhuận',
    sector: 'Bán lẻ',
    referencePrice: 82000,
  },
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
      const ceiling = Math.round((s.referencePrice * 1.07) / 100) * 100;
      const floor = Math.round((s.referencePrice * 0.93) / 100) * 100;
      await repo.save(
        repo.create({
          ...s,
          exchange: Exchange.HOSE,
          ceilingPrice: ceiling,
          floorPrice: floor,
          lotSize: 100,
        }),
      );
      console.log(`Seeded: ${s.ticker}`);
    } else {
      console.log(`Skipped (exists): ${s.ticker}`);
    }
  }
  await dataSource.destroy();
  console.log('Seed complete.');
}

seed().catch(console.error);
