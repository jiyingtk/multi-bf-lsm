#include"leveldb/statistics.h"
#include <limits>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

namespace leveldb{

#define STAT_COUNTER_CAP ((UINT64_C(100000)))
#define STAT_COUNTER_NUSEC ((UINT64_C(1)))

class latency_statistics {
	uint32_t* counters;

public:
	latency_statistics();
	~latency_statistics();
	void latency_record(const uint64_t usec);
	void latency_show(const char * const tag, FILE * const out);
};

latency_statistics::latency_statistics() {
  const uint64_t size = sizeof(uint32_t) * STAT_COUNTER_CAP;
  counters = (uint32_t*) malloc(size);
  assert(counters);
  bzero(counters, size);
}

latency_statistics::~latency_statistics() {
	free(counters);
}

void latency_statistics::latency_record(const uint64_t usec) {
  const uint64_t id = usec/STAT_COUNTER_NUSEC;
  if (id < STAT_COUNTER_CAP) {
    __sync_add_and_fetch(&(counters[id]), 1);
  }
}

void latency_statistics::latency_show(const char * const tag, FILE * const out) {
  uint64_t sum = 0;
  double t_sum = 0;
  uint64_t max = 0;
  // sum & max
  for (uint64_t i = 0; i < STAT_COUNTER_CAP; i++) {
    if (counters[i] > 0) {
      sum += counters[i];
      t_sum += i * counters[i];
      max = i;
    }
  }
  if (sum == 0) return;
  fprintf(out, "====Latency Stat:%s, avg: %lfus\n", tag, t_sum/sum);
  fprintf(out, "[x<L<x+%" PRIu64 "] %10s %10s\n", STAT_COUNTER_NUSEC, "[COUNT]", "[%]");

  // 1/1024
  const uint64_t c1 = sum >> 10;
  const double sumd = (double)sum;
  const double d1 = sumd * 0.01;

  const uint64_t p95 = (uint64_t)(sumd * 0.95);
  const uint64_t p99 = (uint64_t)(sumd * 0.99);
  const uint64_t p999 = (uint64_t)(sumd * 0.999);
  uint64_t i95=0, i99=0, i999=0;

  uint64_t count = 0;
  for (uint64_t i = 0; i < STAT_COUNTER_CAP; i++) {
    if (counters[i] > 0) {
      count += counters[i];
    }

    if (count && (count >= p95) && (i95 == 0)) i95 = i;
    if (count && (count >= p99) && (i99 == 0)) i99 = i;
    if (count && (count >= p999) && (i999 == 0)) i999 = i;

    if (counters[i] > c1) {
      const double p = ((double)counters[i]) / d1;
      fprintf(out, "%10" PRIu64 " %10u %10.3lf\n", i * STAT_COUNTER_NUSEC, counters[i], p);
    }
  }
  fprintf(out, "%6s  %8" PRIu64 " us\n%6s  %8" PRIu64 " us\n%6s  %8" PRIu64 " us\n%6s  %8" PRIu64 " us\n",
    "MAX", max * STAT_COUNTER_NUSEC, "95%", i95 * STAT_COUNTER_NUSEC,
    "99%", i99 * STAT_COUNTER_NUSEC, "99.9%", i999 * STAT_COUNTER_NUSEC);
}


void Statistics::init(){
    int i;
    for(i = 0 ; i < TICKER_ENUM_MAX ; i++){
	histograms_[i].min = std::numeric_limits<double>::max();
	histograms_[i].max = 0;
	histograms_[i].average = 0;
	tickers_[i] = 0;
    }
}
std::shared_ptr<Statistics> Statistics::statis_ = nullptr;
Statistics::Statistics(){
  init();
}

void Statistics::reset(){
  init();
}

inline uint64_t Statistics::getTickerCount(uint32_t tickerType) const
{
    assert(tickerType < TICKER_ENUM_MAX);
    return tickers_[tickerType];
}

inline void Statistics::recordTick(uint32_t tickerType, uint64_t count)
{
    assert(tickerType < TICKER_ENUM_MAX);
    tickers_[tickerType] += count;
}

inline void Statistics::measureTime(uint32_t tickerType, uint64_t value)
{
    assert(tickerType < TICKER_ENUM_MAX);
    recordTick(tickerType,1);
    if(histograms_[tickerType].max < value){
	histograms_[tickerType].max = value;
    }else if(histograms_[tickerType].min > value){
	histograms_[tickerType].min = value;
    }
    histograms_[tickerType].average += value;
}

inline void Statistics::setTickerCount(uint32_t tickerType, uint64_t count)
{
    assert(tickerType < TICKER_ENUM_MAX);
    tickers_[tickerType] = count;
}

inline uint64_t Statistics::GetTickerHistogram(uint32_t tickerType) const
{
     assert(tickerType < TICKER_ENUM_MAX);
     return histograms_[tickerType].average;
}

std::string Statistics::ToString(uint32_t begin_type, uint32_t end_type)
{
	int i;
	char buf[200];
	std::string res;
	res.reserve(20000);
	uint64_t totalReadCount = 0;
	uint64_t totalReadTime = 0;
	for(i = begin_type ; i <= end_type ; i++){
	    if(histograms_[i].average == 0){
		continue;
	    }
       	    histograms_[i].min = (histograms_[i].min == std::numeric_limits<double>::max() ? -1 : histograms_[i].min);
	    totalReadCount += tickers_[i]*(i - begin_type);
	    totalReadTime += histograms_[i].average;
	    histograms_[i].average = histograms_[i].average/tickers_[i];
	    snprintf(buf,sizeof(buf),"%s min:%.3lf ave:%.3lf max:%.3lf count:%lu\n",TickersNameMap[i].second.c_str(),histograms_[i].min,histograms_[i].average,histograms_[i].max,tickers_[i]);
	    res.append(buf);
	}
	snprintf(buf,sizeof(buf),"totalAccessCount %lu totalAccessTime %lu \n",totalReadCount,totalReadTime);
	res.append(buf);
	res.shrink_to_fit();
	return res;
}


std::shared_ptr<Statistics> CreateDBStatistics(){
	auto sp = std::make_shared<Statistics>();
	Statistics::SetStatisitics(sp);
	return sp;
}

void MeasureTime(Statistics * statistics,uint32_t histogram_type,uint64_t value){
    if(statistics){
	statistics->measureTime(histogram_type,value);
    }
}

inline void RecordTick(Statistics *statistics,uint32_t ticker_type,uint64_t count=1){
    if(statistics){
	statistics->recordTick(ticker_type,count);
    }
}

inline void setTickerCount(Statistics *statistics,uint32_t ticker_type,uint64_t count){
    if(statistics){
	statistics->setTickerCount(ticker_type,count);
    }
}

};
