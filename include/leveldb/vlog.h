//
// Created by wujy on 8/21/18.
//

#ifndef MULTI_BF_LSM_VLOG_H
#define MULTI_BF_LSM_VLOG_H

#include "db.h"
#include <string>
#include <iostream>

using std::string;

namespace leveldb {

    class SepDB {
    public:
        SepDB(Options options, const std::string &dbname, const std::string &vlogname, Status &s) {
            s = DB::Open(options, dbname, &indexDB);
            if (!s.ok()) {
                std::cout << s.ToString() << std::endl;
                return;
            }
            vlog = fopen(vlogname.c_str(), "a+");
        }

        ~SepDB(){
            fclose(vlog);
            delete indexDB;
        }

        static Status Open(Options options,const std::string & dbname, const std::string &vlogname, SepDB **db) {
            Status s;
            *db = new SepDB(options, dbname, vlogname, s);
            return s;
        }

        /*
         * indexDB: <key,offset+value size>
         * vlog: <key size, value size, key, value>
         * use '$' to seperate offset and value size, key size and value size, value size and key
        */
        Status Put(const WriteOptions writeOptions,const string& key, const string& val){
            long keySize = key.size();
            long valueSize = val.size();
            string keySizeStr = std::to_string(keySize);
            string valueSizeStr = std::to_string(valueSize);

            string vlogStr = keySizeStr+"$"+valueSizeStr+"$"+key+val; // | key size | value size | key | value |
            fwrite(vlogStr.c_str(),vlogStr.size(),1,vlog);

            long vlogOffset = ftell(vlog)-val.size();
            string vlogOffsetStr = std::to_string(vlogOffset);
            string indexStr = vlogOffsetStr+"$"+valueSizeStr;
            Status s = indexDB->Put(writeOptions, key, indexStr);
            return s;
        }

        /*
         * Get value offset and value size from indexDB
         * Get value from vlog
        */
        Status Get(const ReadOptions readOptions,const string& key, string* val){
            string valueInfo;
            Status s = indexDB->Get(readOptions,key,&valueInfo);
            if(!s.ok()) return s;
            size_t sepPos = valueInfo.find('$');
            string offsetStr = valueInfo.substr(0,sepPos);
            string valueSizeStr = valueInfo.substr(sepPos+1,valueInfo.size()-sepPos+1);
            long offset = std::stol(offsetStr);
//            std::cout<<offset<<std::endl;
            long valueSize = std::stol(valueSizeStr);
//            std::cout<<valueSize<<std::endl;
            char value[valueSize];
            fseek(vlog,offset,SEEK_SET);
//            std::cout<<"pos:"<<ftell(vlog)<<std::endl;
            int read = fread(value,valueSize,1,vlog);
//            std::cout<<read<<"      "<<strlen(value)<<std::endl;
//            std::cout<<"\n/n##########/n\n"<<value<<std::endl;
            val->assign(value,valueSize);
            return s;
        }

        Status Delete(const WriteOptions writeOptions,const string& key){
            Status s = indexDB->Delete(writeOptions,key);
            return s;
        }

        bool GetProperty(const string& property, std::string* value) {
            return indexDB->GetProperty(property,value);
        }

        void DoSomeThing(void* arg){
            indexDB->DoSomeThing(arg);
        }


    private:
        DB *indexDB;
        FILE *vlog;
        size_t tail;
    };
}

#endif //MULTI_BF_LSM_VLOG_H