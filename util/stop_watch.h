#include"leveldb/env.h"
#include"leveldb/statistics.h"
namespace leveldb {
class StopWatch{
public:
	StopWatch(Env* const env,Statistics *statistics,const uint32_t hist_type):env_(env),start_time_(env->NowMicros()),
			statistics_(statistics),hist_type_(hist_type){}
	~StopWatch(){
	    MeasureTime(statistics_,hist_type_,env_->NowMicros() - start_time_);
	}
	void setHistType(uint32_t hist_type){
	    hist_type_ = hist_type;
	}
private:
	Env *env_;
	const uint64_t start_time_;
	Statistics *statistics_;
	uint32_t hist_type_;
};
};