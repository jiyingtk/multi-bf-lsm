#ifndef STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#define STORAGE_LEVELDB_INCLUDE_STATISTICS_H_
#include<memory>
#include<vector>
#include<string>
#include<assert.h>
namespace leveldb{
  enum Tickers: uint32_t{
	MEM_READ_TIME = 0,
	IMMEM_READ_TIME,
	READ_0_TIME,
	READ_1_TIME,
	READ_2_TIME,
	READ_3_TIME,
	READ_4_TIME,
	READ_5_TIME,
	READ_6_TIME,
	READ_7_TIME,
	ACCESS_L0_TIME,
	ACCESS_L1_TIME,
	ACCESS_L2_TIME,
	ACCESS_L3_TIME,
	ACCESS_L4_TIME,
	ACCESS_L5_TIME,
	ACCESS_L6_TIME,
	TICKER_ENUM_MAX
 };  

 const std::vector<std::pair<Tickers,std::string>> TickersNameMap={
     {MEM_READ_TIME,"leveldb.mem.read.time"},
     {IMMEM_READ_TIME,"leveldb.immem.read.time"},
     {READ_0_TIME,"leveldb.0.time"},
     {READ_1_TIME,"leveldb.1.time"},
     {READ_2_TIME,"leveldb.2.time"},
     {READ_3_TIME,"leveldb.3.time"},
     {READ_4_TIME,"leveldb.4.time"},
     {READ_5_TIME,"leveldb.5.time"},
     {READ_6_TIME,"leveldb.6.time"},
     {READ_7_TIME,"leveldb.7.time"},
      {ACCESS_L0_TIME,"leveldb.access.l0.time"},
     {ACCESS_L1_TIME,"leveldb.access.l1.time"},
     {ACCESS_L2_TIME,"leveldb.access.l2.time"},
     {ACCESS_L3_TIME,"leveldb.access.l3.time"},
     {ACCESS_L4_TIME,"leveldb.access.l4.time"},
     {ACCESS_L5_TIME,"leveldb.access.l5.time"},
     {ACCESS_L6_TIME,"leveldb.access.l6.time"}
};

struct HistogramData{
	double min;
	double max = 0.0;
	double average;
};

class Statistics {
public:
	Statistics();
	virtual ~Statistics() {}
	virtual uint64_t getTickerCount(uint32_t tickerType)  const;
	virtual void recordTick(uint32_t tickerType,uint64_t count = 0) ;
	virtual void measureTime(uint32_t tickerType,uint64_t value);
	virtual void setTickerCount(uint32_t tickerType,uint64_t count = 0);
	virtual std::string ToString(uint32_t begin_type = READ_0_TIME,uint32_t end_type= READ_7_TIME);
private:
	uint64_t tickers_[TICKER_ENUM_MAX];
	HistogramData histograms_[TICKER_ENUM_MAX];
};

std::shared_ptr<Statistics> CreateDBStatistics();
void MeasureTime(Statistics * statistics,uint32_t histogram_type,uint64_t value);
};



#endif
