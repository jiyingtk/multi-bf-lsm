// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "util/coding.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

#include <sys/time.h>
#include <time.h>
#include <unistd.h>

namespace leveldb
{

static const int kVerbose = 1;

static Slice Key(int i, char *buffer)
{
  EncodeFixed32(buffer, i);
  return Slice(buffer, sizeof(uint32_t));
}
int bits_per_key_per_filter[] = {24, 0};
const int filter_len = sizeof(bits_per_key_per_filter) / sizeof(int) - 1;
class BloomTest
{
private:
  const FilterPolicy *policy_;
  std::list<std::string> *filters_;
  std::vector<std::string> keys_;

public:
  BloomTest() : policy_(NewBloomFilterPolicy(bits_per_key_per_filter, 8)), filters_(NULL) { Reset(); }

  ~BloomTest()
  {
    delete policy_;
  }

  void Reset()
  {
    keys_.clear();
    filters_ = new std::list<std::string>();
    for (int i = 0; i < filter_len; i++)
    {
      filters_->push_back(std::string(""));
    }
  }

  void Add(const Slice &s)
  {
    keys_.push_back(s.ToString());
  }

  void Build()
  {
    std::vector<Slice> key_slices;
    for (size_t i = 0; i < keys_.size(); i++)
    {
      key_slices.push_back(Slice(keys_[i]));
    }

    policy_->CreateFilter(&key_slices[0], static_cast<int>(key_slices.size()),
                          *filters_);
    keys_.clear();
    if (kVerbose >= 2)
      DumpFilter();
  }

  size_t FilterSize() const
  {
    size_t sum = 0;
    for (auto iter = filters_->begin(); iter != filters_->end(); iter++)
    {
      sum += iter->size();
    }
    return sum;
  }

  void DumpFilter()
  {
    fprintf(stderr, "F(");
    for (auto iter = filters_->begin(); iter != filters_->end(); iter++)
    {
      for (size_t i = 0; i + 1 < iter->size(); i++)
      {
        const unsigned int c = static_cast<unsigned int>((*iter)[i]);
        for (int j = 0; j < 8; j++)
        {
          fprintf(stderr, "%c", (c & (1 << j)) ? '1' : '.');
        }
      }
    }
    fprintf(stderr, ")\n");
  }

  bool Matches(const Slice &s)
  {
    if (!keys_.empty())
    {
      Build();
    }
    std::list<Slice> tmp_filters;
    for (auto iter = filters_->begin(); iter != filters_->end(); iter++)
    {
      tmp_filters.push_back(*iter);
    }
    return policy_->KeyMayMatchFilters(s, tmp_filters);
  }

  double FalsePositiveRate()
  {
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++)
    {
      if (Matches(Key(i + 1000000000, buffer)))
      {
        result++;
      }
    }
    return result / 10000.0;
  }

  const char * Name() {
    return policy_->Name();
  }
};

TEST(BloomTest, EmptyFilter)
{
  ASSERT_TRUE(!Matches("hello"));
  ASSERT_TRUE(!Matches("world"));
}

TEST(BloomTest, Small)
{
  Reset();
  Add("hello");
  Add("world");
  ASSERT_TRUE(Matches("hello"));
  ASSERT_TRUE(Matches("world"));
  ASSERT_TRUE(!Matches("x"));
  ASSERT_TRUE(!Matches("foo"));
}

static int NextLength(int length)
{
  if (length < 10)
  {
    length += 1;
  }
  else if (length < 100)
  {
    length += 10;
  }
  else if (length < 1000)
  {
    length += 100;
  }
  else
  {
    length += 1000;
  }
  return length;
}

TEST(BloomTest, VaryingLengths)
{
  char buffer[sizeof(int)];

  // Count number of filters that significantly exceed the false positive rate
  int mediocre_filters = 0;
  int good_filters = 0;

  for (int length = 1000; length <= 24000; length = NextLength(length))
  {
    Reset();
    for (int i = 0; i < length; i++)
    {
      Add(Key(i, buffer));
    }
    Build();

    // ASSERT_LE(FilterSize(), static_cast<size_t>((length * 10 / 8) + 40))
    //     << length;

    // All added keys must match
    for (int i = 0; i < length; i++)
    {
      ASSERT_TRUE(Matches(Key(i, buffer)))
          << "Length " << length << "; key " << i;
    }

    // Check false positive rate
    double rate = FalsePositiveRate();
    if (kVerbose >= 1)
    {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
              rate * 100.0, length, static_cast<int>(FilterSize()));
    }
    //ASSERT_LE(rate, 0.02);   // Must not be over 2%
    if (rate > 0.0125)
      mediocre_filters++; // Allowed, but not too often
    else
      good_filters++;
  }
  if (kVerbose >= 1)
  {
    fprintf(stderr, "Filters: %d good, %d mediocre\n",
            good_filters, mediocre_filters);
  }
  ASSERT_LE(mediocre_filters, good_filters / 5);
}

float getInterval(struct timeval m_begin, struct timeval m_end)
{
  if (m_end.tv_usec < m_begin.tv_usec)
  {
    m_end.tv_usec += 1000;
    m_end.tv_sec = m_end.tv_sec - 1;
  }

  return (m_end.tv_sec - m_begin.tv_sec) + (m_end.tv_usec - m_begin.tv_usec) / 1000000.0;
}

TEST(BloomTest, Performance)
{
  char buffer[sizeof(int)];

  long length = 100000000;

  struct timeval tv1, tv2;
  float used_time;

#ifdef ChildPolicy
  fprintf(stderr, "Test Bloom filter: %s\n", STR(ChildPolicy));
#else
  fprintf(stderr, "Test Bloom filter: %s\n", Name());
#endif

  Reset();
  for (long i = 0; i < length; i++)
  {
    Add(Key(i, buffer));
  }

  gettimeofday(&tv1, NULL);
  Build();
  gettimeofday(&tv2, NULL);
  used_time = getInterval(tv1, tv2);
  fprintf(stderr, "Building bf with %ld keys takes %fs\n", length, used_time);

  gettimeofday(&tv1, NULL);
  for (long i = 0; i < length; i++)
  {
    Matches(Key(i, buffer));
  }
  gettimeofday(&tv2, NULL);
  used_time = getInterval(tv1, tv2);
  fprintf(stderr, "Querying bf with %ld keys takes %fs\n", length, used_time);
}

// Different bits-per-byte

} // namespace leveldb

int main(int argc, char **argv)
{
  return leveldb::test::RunAllTests();
}
