#include"leveldb/statistics.h"
#include <limits>
#include <stdio.h>
namespace leveldb{
void Statistics::init(){
    int i;
    for(i = 0 ; i < TICKER_ENUM_MAX ; i++){
	histograms_[i].min = std::numeric_limits<double>::max();
	histograms_[i].max = 0;
	histograms_[i].average = 0;
	tickers_[i] = 0;
    }
}

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

std::string Statistics::ToString(uint32_t begin_type, uint32_t end_type)
{
	int i;
	char buf[200];
	std::string res;
	res.reserve(20000);
	for(i = begin_type ; i <= end_type ; i++){
       	    histograms_[i].min = (histograms_[i].min == std::numeric_limits<double>::max() ? -1 : histograms_[i].min);
	    histograms_[i].average = histograms_[i].average/tickers_[i];
	    snprintf(buf,sizeof(buf),"%s min:%.3lf ave:%.3lf max:%.3lf count:%lu\n",TickersNameMap[i].second.c_str(),histograms_[i].min,histograms_[i].average,histograms_[i].max,tickers_[i]);
	    res.append(buf);
	}
	res.shrink_to_fit();
	reset();
	return res;
}


std::shared_ptr<Statistics> CreateDBStatistics(){
	return std::make_shared<Statistics>();
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