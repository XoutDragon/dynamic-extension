/*
 * include/framework/structure/RangeScanIterator.h
 *
 * Distributed under the Modified BSD License.
 *
 * An iterator over every record within a key range, merged in sorted order
 * across the buffer and every shard of a DynamicExtension.
 * 
 * 
 */

#pragma once

#include <algorithm>
#include <cassert>
#include <functional>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "framework/interface/Record.h"

namespace de
{

  template <typename Extension, typename ShardType>
  class RangeScanIterator
  {
    typedef typename ShardType::RECORD RecordType;
    typedef typename ShardType::Iterator ShardIterator;
    typedef typename Extension::_Epoch _Epoch;

    struct FreeEpoch
    {
      Extension* extension;
      void operator()(_Epoch *epoch) const
      {
        extension->end_job(epoch);
      }
    };

    struct HeapEntry
    {
      Wrapped<RecordType> rec;
      size_t source;
 
      bool operator>(const HeapEntry &other) const
      {
        return other.rec.rec < rec.rec;
      }
    };


  public:
    RangeScanIterator(Extension *extension, const RecordType &start,
                      const RecordType &finish)
        : m_epoch(extension->get_active_epoch(), FreeEpoch{extension}), m_finish(finish),
        m_buffer_idx(0), m_pending(0)
    {
      {
        auto view = m_epoch->get_buffer();
        for (size_t i = 0; i < view.get_record_count(); i++)
        {
          auto rec = view.get(i);
          if (rec->rec.key >= start.key && rec->rec.key <= finish.key)
            m_buffer_recs.push_back(*rec);
        }
      }

      std::sort(m_buffer_recs.begin(), m_buffer_recs.end(),
                [](const Wrapped<RecordType> &a, const Wrapped<RecordType> &b)
                { return a.rec < b.rec; });

      for (auto &level : m_epoch->get_structure()->get_levels())
      {
        if (!level)
          continue;

        for (size_t i = 0; i < level->get_shard_count(); i++)
        {
          auto shard = level->get_shard(i);
          if (!shard)
            continue;

          m_shard_iters.push_back(
              std::make_unique<ShardIterator>(shard->get_iterator(start.key)));
        }
      }

      for (size_t source = 0; source <= m_shard_iters.size(); source++) {
        refill(source);
      }

      advance();
    }

    bool has_next() { return m_pending > 0; }

    RecordType next()
    {
      assert(m_pending > 0);

      RecordType rec = m_current;
      if (--m_pending == 0)
        advance();

      return rec;
    }

  private:
    void refill(size_t source)
    {
      if (source == 0)
      {
        if (m_buffer_idx < m_buffer_recs.size())
          m_heap.push({m_buffer_recs[m_buffer_idx++], 0});
        return;
      }

      auto rec = m_shard_iters[source - 1]->get_current();
      if (rec && rec->rec.key <= m_finish.key)
      {
        m_heap.push({*rec, source});
        m_shard_iters[source - 1]->get_next();
      }
    }

    void advance()
    {
      while (!m_heap.empty())
      {
        RecordType rec = m_heap.top().rec.rec;
        ssize_t live = 0;

        while (!m_heap.empty() && m_heap.top().rec.rec == rec)
        {
          HeapEntry entry = m_heap.top();
          m_heap.pop();

          if (entry.rec.is_tombstone())
            live--;
          else if (!entry.rec.is_deleted())
            live++;

          refill(entry.source);
        }

        if (live > 0)
        {
          m_current = rec;
          m_pending = live;
          return;
        }
      }

      m_pending = 0;
    }

    std::unique_ptr<_Epoch, FreeEpoch> m_epoch;
    RecordType m_finish;

    std::vector<Wrapped<RecordType>> m_buffer_recs;
    size_t m_buffer_idx;
    std::vector<std::unique_ptr<ShardIterator>> m_shard_iters;

    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> m_heap;
    RecordType m_current;
    size_t m_pending;
  };

} // namespace de
