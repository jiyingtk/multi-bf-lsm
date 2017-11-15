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
	CREATE_FILTER_TIME,
	WRITE_FILTER_TIME,
	ADD_FILTER_TIME_0,
	ADD_FILTER_TIME_1,
	ADD_FILTER_TIME_2,
	ADD_FILTER_TIME_3,
	ADD_FILTER_TIME_4,
	ADD_FILTER_TIME_5,
	ADD_FILTER_TIME_6,
	REMOVE_FILTER_TIME,
	REMOVE_EXPIRED_FILTER_TIME_0,
	REMOVE_EXPIRED_FILTER_TIME_1,
	REMOVE_EXPIRED_FILTER_TIME_2,
	REMOVE_EXPIRED_FILTER_TIME_3,
	REMOVE_EXPIRED_FILTER_TIME_4,
	REMOVE_EXPIRED_FILTER_TIME_5,
	REMOVE_EXPIRED_FILTER_TIME_6,
	REMOVE_HEAD_FILTER_TIME_0,
	REMOVE_HEAD_FILTER_TIME_1,
	REMOVE_HEAD_FILTER_TIME_2,
	REMOVE_HEAD_FILTER_TIME_3,
	REMOVE_HEAD_FILTER_TIME_4,
	REMOVE_HEAD_FILTER_TIME_5,
	REMOVE_HEAD_FILTER_TIME_6,
	FILTER_LOCK_TIME,
	FILTER_WAIT_TIME,
	SYNC_TIME,
	CHILD_CREATE_FILTER_TIME,
	CHILD_FILTER_OTHER_TIME,
	SET_FRE_COUNT_IN_COMPACTION_TIME,
	SLOW_SHRINKING,
	QUICK_SHRINKING,
	FORCE_SHRINKING,
	FINDTABLE,
	INTERNALGET,
        RELEASE,
	BLOCK_READ_TIME,
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
     {ACCESS_L6_TIME,"leveldb.access.l6.time"},
     {CREATE_FILTER_TIME,"leveldb.create.filter.time"},
     {WRITE_FILTER_TIME,"leveldb.write.filter.time"},
     {ADD_FILTER_TIME_0,"leveldb.add.filter.time"},
     {ADD_FILTER_TIME_1,"leveldb.add.filter.time"},
     {ADD_FILTER_TIME_2,"leveldb.add.filter.time"},
     {ADD_FILTER_TIME_3,"leveldb.add.filter.time"},
     {ADD_FILTER_TIME_4,"leveldb.add.filter.time"},
     {ADD_FILTER_TIME_5,"leveldb.add.filter.time"},
     {ADD_FILTER_TIME_6,"leveldb.add.filter.time"},
     {REMOVE_FILTER_TIME,"leveldb.remove(adj).filter.time"},
     {REMOVE_EXPIRED_FILTER_TIME_0,"leveldb.remove.expired.filter.time0"},
     {REMOVE_EXPIRED_FILTER_TIME_1,"leveldb.remove.expired.filter.time1"},
     {REMOVE_EXPIRED_FILTER_TIME_2,"leveldb.remove.expired.filter.time2"},
     {REMOVE_EXPIRED_FILTER_TIME_3,"leveldb.remove.expired.filter.time3"},
     {REMOVE_EXPIRED_FILTER_TIME_4,"leveldb.remove.expired.filter.time4"},
     {REMOVE_EXPIRED_FILTER_TIME_5,"leveldb.remove.expired.filter.time5"},
     {REMOVE_EXPIRED_FILTER_TIME_6,"leveldb.remove.expired.filter.time6"},
     {REMOVE_HEAD_FILTER_TIME_0,"leveldb.remove.head.filter.time0"},
     {REMOVE_HEAD_FILTER_TIME_1,"leveldb.remove.head.filter.time1"},
     {REMOVE_HEAD_FILTER_TIME_2,"leveldb.remove.head.filter.time2"},
     {REMOVE_HEAD_FILTER_TIME_3,"leveldb.remove.head.filter.time3"},
     {REMOVE_HEAD_FILTER_TIME_4,"leveldb.remove.head.filter.time4"},
     {REMOVE_HEAD_FILTER_TIME_5,"leveldb.remove.head.filter.time5"},
     {REMOVE_HEAD_FILTER_TIME_6,"leveldb.remove.head.filter.time6"},
     {FILTER_LOCK_TIME,"leveldb.filter.lock.time"},
     {FILTER_WAIT_TIME,"leveldb.filter.wait.time"},
     {SYNC_TIME,"leveldb.sync.time"},
     {CHILD_CREATE_FILTER_TIME,"leveldb.child.create.filter.time"},
     {CHILD_FILTER_OTHER_TIME,"leveldb.child.filter.other.time"},
     {SET_FRE_COUNT_IN_COMPACTION_TIME,"leveldb.set.fre.count.in.compaction.time"},
     {SLOW_SHRINKING,"leveldb.slow.shrinking.time"},
     {QUICK_SHRINKING,"leveldb.quick.shrinking.time"},
     {FORCE_SHRINKING,"leveldb.force.shrinking.time"},
     {FINDTABLE,"leveldb.findtable.time"},
     {INTERNALGET,"leveldb.internalget.time"},
     {RELEASE,"leveldb.release.time"},
      {BLOCK_READ_TIME,"leveldb.block.read.time"}
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
	virtual void reset();
	virtual void init();
	virtual uint64_t GetTickerHistogram(uint32_t tickerType) const;
	static std::shared_ptr<Statistics>  GetStatistics(){
	    return statis_;
	}
	static void SetStatisitics(std::shared_ptr<Statistics> sp){
	  statis_ = sp;
	}
private:
	uint64_t tickers_[TICKER_ENUM_MAX];
	HistogramData histograms_[TICKER_ENUM_MAX];
	static std::shared_ptr<Statistics> statis_;
};

std::shared_ptr<Statistics> CreateDBStatistics();
void MeasureTime(Statistics * statistics,uint32_t histogram_type,uint64_t value);
};



#endif
